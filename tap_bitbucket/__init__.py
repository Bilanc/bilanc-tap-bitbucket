import os
import json
import collections
import time
import requests
import backoff
import singer
import jwt
import math
import argparse
from datetime import datetime, timedelta
import pytz
from singer import bookmarks, metrics, metadata
from simplejson import JSONDecodeError
import base64

session = requests.Session()
logger = singer.get_logger()

BASE_URL: str = "https://api.bitbucket.org/2.0"

# set default timeout of 300 seconds
REQUEST_TIMEOUT = 300

REQUIRED_CONFIG_KEYS = ["start_date"]

KEY_PROPERTIES = {
    "commits": ["hash"],
    "pull_requests": ["id"],
    "pull_request_comments": ["id"],
    "pull_request_files": ["pr_id"],
    "pull_request_stats": ["pr_id"],
    "pull_request_commits": ["id"],
    "pull_request_details": ["id"],
    "deployments": ["key"],
    "organization_members": ["uuid"],
}

VISITED_ORGS_IDS = set()


class GithubException(Exception):
    pass


class BadCredentialsException(GithubException):
    pass


class AuthException(GithubException):
    pass


class NotFoundException(GithubException):
    pass


class BadRequestException(GithubException):
    pass


class InternalServerError(GithubException):
    pass


class UnprocessableError(GithubException):
    pass


class NotModifiedError(GithubException):
    pass


class MovedPermanentlyError(GithubException):
    pass


class ConflictError(GithubException):
    pass


class APIRateLimitExceededError(GithubException):
    pass


class RetriableServerError(GithubException):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    301: {
        "raise_exception": MovedPermanentlyError,
        "message": "The resource you are looking for is moved to another URL.",
    },
    304: {
        "raise_exception": NotModifiedError,
        "message": "The requested resource has not been modified since the last time you accessed it.",
    },
    400: {
        "raise_exception": BadRequestException,
        "message": "The request is missing or has a bad parameter.",
    },
    401: {
        "raise_exception": BadCredentialsException,
        "message": "Invalid authorization credentials.",
    },
    403: {
        "raise_exception": AuthException,
        "message": "User doesn't have permission to access the resource.",
    },
    404: {
        "raise_exception": NotFoundException,
        "message": "The resource you have specified cannot be found. Alternatively the access_token is not valid for the resource",
    },
    409: {
        "raise_exception": ConflictError,
        "message": "The request could not be completed due to a conflict with the current state of the server.",
    },
    422: {
        "raise_exception": UnprocessableError,
        "message": "The request was not able to process right now.",
    },
    500: {
        "raise_exception": InternalServerError,
        "message": "An error has occurred at Github's end.",
    },
    502: {"raise_exception": RetriableServerError, "message": "Server Error"},
}


def translate_state(state, catalog, repositories):
    """
    This tap used to only support a single repository, in which case the
    state took the shape of:
    {
      "bookmarks": {
        "commits": {
          "since": "2018-11-14T13:21:20.700360Z"
        }
      }
    }
    The tap now supports multiple repos, so this function should be called
    at the beginning of each run to ensure the state is translate to the
    new format:
    {
      "bookmarks": {
        "singer-io/tap-adwords": {
          "commits": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        }
        "singer-io/tap-salesforce": {
          "commits": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        }
      }
    }
    """
    nested_dict = lambda: collections.defaultdict(nested_dict)
    new_state = nested_dict()

    for stream in catalog["streams"]:
        stream_name = stream["tap_stream_id"]
        for repo in repositories:
            if bookmarks.get_bookmark(state, repo, stream_name):
                return state
            if bookmarks.get_bookmark(state, stream_name, "since"):
                new_state["bookmarks"][repo][stream_name]["since"] = (
                    bookmarks.get_bookmark(state, stream_name, "since")
                )

    return new_state


def get_bookmark(state, repo, stream_name, bookmark_key, start_date):
    repo_stream_dict = bookmarks.get_bookmark(state, repo, stream_name)
    if repo_stream_dict:
        return repo_stream_dict.get(bookmark_key)
    if start_date:
        return start_date
    return None


def raise_for_error(resp, source):
    error_code = resp.status_code
    try:
        response_json = resp.json()
    except JSONDecodeError:
        response_json = {}

    if (
        error_code == 409
        and response_json
        and response_json.get("message") == "Git Repository is empty."
    ):
        logger.info(response_json.get("message"))
        return None

    if error_code == 404:
        details = ERROR_CODE_EXCEPTION_MAPPING.get(error_code).get("message")
        if source == "teams":
            details += " or it is a personal account repository"
        message = "HTTP-error-code: 404, Error: {}. Please refer '{}' for more details.".format(
            details, response_json.get("documentation_url")
        )
        logger.info(message)
        # don't raise a NotFoundException
        return None

    if (
        error_code == 403
        and response_json
        and "message" in response_json
        and "API rate limit exceeded" in response_json["message"]
    ):
        raise APIRateLimitExceededError(f"[Error Code] {error_code}: {response_json}")

    if 500 <= error_code < 600:
        raise RetriableServerError(resp.text)

    message = "HTTP-error-code: {}, Error: {}".format(
        error_code,
        (
            ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get(
                "message", "Unknown Error"
            )
            if response_json == {}
            else response_json
        ),
    )

    exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get(
        "raise_exception", GithubException
    )
    raise exc(message) from None


def calculate_seconds(epoch):
    current = time.time()
    return math.ceil(epoch - current)


def get_reset_time_and_remaining_calls(
    response, message="Reset time will be reached in {} seconds. Remaining {} calls"
):
    try:
        reset_time = int(response.headers["X-RateLimit-Reset"])
        remaining = int(response.headers["X-RateLimit-Remaining"])
        seconds_to_sleep = calculate_seconds(reset_time)
        logger.info(message.format(seconds_to_sleep, remaining))
    except:
        logger.exception(response.headers)
        return None, None
    return reset_time, remaining


def rate_throttling(response):
    reset_time, remaining = get_reset_time_and_remaining_calls(response)
    if remaining == 0:
        seconds_to_sleep = calculate_seconds(reset_time)
        logger.info(
            "API rate limit was consumed. Tap will retry the data collection after %s seconds.",
            seconds_to_sleep,
        )
        time.sleep(seconds_to_sleep)


access_token_expires_at = None
refresh_token_expires_at = None
config_path = None


def refresh_token_if_expired():
    stale_access_token = (
        access_token_expires_at
        and access_token_expires_at < datetime.now(pytz.UTC).isoformat()
    )
    stale_refresh_token = (
        refresh_token_expires_at
        and refresh_token_expires_at < datetime.now(pytz.UTC).isoformat()
    )

    if stale_access_token or stale_refresh_token:
        # Pull config
        with open(config_path, "r") as f:
            config = json.load(f)

        refresh_oauth_token(config)


# pylint: disable=dangerous-default-value
# during 'Timeout' error there is also possibility of 'ConnectionError',
# hence added backoff for 'ConnectionError' too.
@backoff.on_exception(
    backoff.expo,
    (
        requests.Timeout,
        requests.ConnectionError,
        ConnectionRefusedError,
        ConnectionResetError,
        APIRateLimitExceededError,
        RetriableServerError,
        InternalServerError,
    ),
    max_tries=10,
    factor=2,
)
def authed_get(source, url, headers={}):
    refresh_token_if_expired()
    with metrics.http_request_timer(source) as timer:
        session.headers.update(headers)
        logger.info("Making request to %s", url)
        resp = session.request(method="get", url=url, timeout=get_request_timeout())
        logger.info("Request received status code %s", resp.status_code)
        if resp.status_code != 200:
            _ = get_reset_time_and_remaining_calls(
                resp,
                message=f"[Request Status {resp.status_code}] Reset time was going to be reached in"
                + " {} seconds.  Remaining {} calls",
            )
            # wait for limit to reset
            if resp.status_code == 403:
                rate_throttling(resp)
            # retry
            raise_for_error(resp, source)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        if resp.status_code in [404, 409]:
            # return an empty response body since we're not raising a NotFoundException
            resp._content = b"{}"  # pylint: disable=protected-access
        return resp


def authed_get_all_pages(source, url, headers={}):
    while True:
        r = authed_get(source, url, headers)
        yield r
        if "next" in r.links:
            url = r.links["next"]["url"]
        else:
            break


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def generate_pr_commit_schema(commit_schema):
    pr_commit_schema = commit_schema.copy()
    pr_commit_schema["properties"]["pr_number"] = {"type": ["null", "integer"]}
    pr_commit_schema["properties"]["pr_id"] = {"type": ["null", "string"]}
    pr_commit_schema["properties"]["id"] = {"type": ["null", "string"]}

    return pr_commit_schema


def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path, encoding="utf-8") as file:
            schemas[file_raw] = json.load(file)

    return schemas


class DependencyException(Exception):
    pass


def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = (
        "Unable to extract '{0}' data, "
        "to receive '{0}' data, you also need to select '{1}'."
    )

    for main_stream, sub_streams in SUB_STREAMS.items():
        if main_stream not in selected_stream_ids:
            for sub_stream in sub_streams:
                if sub_stream in selected_stream_ids:
                    errs.append(msg_tmpl.format(sub_stream, main_stream))

    if errs:
        raise DependencyException(" ".join(errs))


def write_metadata(mdata, values, breadcrumb):
    mdata.append({"metadata": values, "breadcrumb": breadcrumb})


def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    # mdata = metadata.write(mdata, (), 'forced-replication-method', KEY_PROPERTIES[schema_name])
    mdata = metadata.write(
        mdata, (), "table-key-properties", KEY_PROPERTIES[schema_name]
    )

    for field_name in schema["properties"].keys():
        if field_name in KEY_PROPERTIES[schema_name]:
            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", "automatic"
            )
        else:
            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", "available"
            )

    return mdata


def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # get metadata for each field
        mdata = populate_metadata(schema_name, schema)

        # create and add catalog entry
        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "metadata": metadata.to_list(mdata),
            "key_properties": KEY_PROPERTIES[schema_name],
        }
        streams.append(catalog_entry)

    return {"streams": streams}


def get_all_repos(workspaces: list) -> list:
    """
    Retrieves all repositories for the provided organizations and
        verifies basic access for them.

    """
    repos = []

    for worskspace_path in workspaces:
        workspace = worskspace_path.split("/")[0]
        for response in authed_get_all_pages(
            "get_all_repos",
            f"{BASE_URL}/repositories/{workspace}",
        ):
            org_repos = response.json()

            for repo in org_repos["values"]:
                repo_full_name = repo.get("full_name")

                logger.info("Verifying access of repository: %s", repo_full_name)
                verify_repo_access(
                    f"{BASE_URL}/repositories/{repo_full_name}/commits",
                    repo,
                )

                repos.append(repo_full_name)

    return repos


def extract_repos_from_config(config: dict) -> list:
    """
    Extracts all repositories from the config and calls get_all_repos()
        for organizations using the wildcard 'org/*' format.
    """
    repo_paths = []

    if not config.get("repository") and not config.get("repositories"):
        raise ValueError("Config does not contain 'repository' or 'repositories' keys.")

    # Check for the old format
    if "repository" in config and isinstance(config["repository"], str):
        repo_paths = list(filter(None, config["repository"].split(" ")))

    # Check for the new format
    elif "repositories" in config and isinstance(config["repositories"], list):
        for repo_obj in config["repositories"]:
            if not isinstance(repo_obj, dict) or "repository" not in repo_obj:
                raise ValueError(
                    "Invalid repository object in new config. Each item must be a dict with a 'repository' key."
                )
            repo_paths.append(repo_obj["repository"])

    orgs_with_all_repos = list(filter(lambda x: x.split("/")[1] == "*", repo_paths))

    if orgs_with_all_repos:
        # remove any wildcard "org/*" occurrences from `repo_paths`
        repo_paths = list(set(repo_paths).difference(set(orgs_with_all_repos)))

        # get all repositores for an org in the config
        all_repos = get_all_repos(orgs_with_all_repos)

        # update repo_paths
        repo_paths.extend(all_repos)

    return repo_paths


def verify_repo_access(url_for_repo, repo):
    try:
        authed_get("verifying repository access", url_for_repo)
    except NotFoundException:
        # throwing user-friendly error message as it checks token access
        message = "HTTP-error-code: 404, Error: Please check the repository name '{}' or you do not have sufficient permissions to access this repository.".format(
            repo
        )
        raise NotFoundException(message) from None


def verify_access_for_repo(config):

    access_token = config["access_token"]
    if config["is_jwt_token"]:
        session.headers.update(
            {"authorization": "token " + access_token, "per_page": "1", "page": "1"}
        )
    else:
        session.headers.update(
            {"Authorization": "Bearer " + access_token, "per_page": "1", "page": "1"}
        )

    repositories = extract_repos_from_config(config)

    for repo in repositories:
        logger.info("Verifying access of repository: %s", repo)

        url_for_repo = f"{BASE_URL}/repositories/{repo}/pullrequests"

        # Verifying for Repo access
        verify_repo_access(url_for_repo, repo)


def do_discover(config):
    verify_access_for_repo(config)
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))


def get_all_pull_requests(schemas, repo_path, state, mdata, start_date):
    """
    https://developer.github.com/v3/pulls/#list-pull-requests
    """

    bookmark_value = get_bookmark(
        state, repo_path, "pull_requests", "since", start_date
    )
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter(
        "pull_request_commits"
    ) as pull_request_commits_counter, metrics.record_counter(
        "pull_requests"
    ) as counter, metrics.record_counter(
        "pull_request_files"
    ) as pull_request_files_counter, metrics.record_counter(
        "pull_request_comments"
    ) as pull_request_comments_counter, metrics.record_counter(
        "pull_request_stats"
    ) as pull_request_stats_counter, metrics.record_counter(
        "pull_request_details"
    ) as pull_request_details_counter:
        for pr_state in ["OPEN", "MERGED", "DECLINED", "SUPERSEDED"]:
            for response in authed_get_all_pages(
                "pull_requests",
                f"{BASE_URL}/repositories/{repo_path}/pullrequests?state={pr_state}",
            ):
                pull_requests = response.json()["values"]
                extraction_time = singer.utils.now()
                for pr in pull_requests:

                    # skip records that haven't been updated since the last run
                    # the GitHub API doesn't currently allow a ?since param for pulls
                    # once we find the first piece of old data we can return, thanks to
                    # the sorting
                    if (
                        bookmark_time
                        and singer.utils.strptime_to_utc(pr.get("updated_on"))
                        < bookmark_time
                    ):
                        return state

                    pr_number = pr.get("id")
                    pr["pr_number"] = pr_number
                    pr["id"] = "{}-{}".format(repo_path, pr_number)
                    pr_id = pr["id"]
                    pr["_sdc_repository"] = repo_path

                    # transform and write pull_request record
                    try:
                        with singer.Transformer() as transformer:
                            rec = transformer.transform(
                                pr,
                                schemas["pull_requests"],
                                metadata=metadata.to_map(mdata["pull_requests"]),
                            )
                    except:
                        logger.exception(f"Failed to transform record [{pr}]")
                        raise

                    singer.write_record(
                        "pull_requests", rec, time_extracted=extraction_time
                    )
                    singer.write_bookmark(
                        state,
                        repo_path,
                        "pull_requests",
                        {"since": singer.utils.strftime(extraction_time)},
                    )
                    counter.increment()

                    # sync reviews if that schema is present (only there if selected)
                    if schemas.get("pull_request_comments"):
                        for review_rec in get_comments_for_pr(
                            pr_id,
                            pr_number,
                            schemas["pull_request_comments"],
                            repo_path,
                            state,
                            mdata["pull_request_comments"],
                        ):
                            singer.write_record(
                                "pull_request_comments",
                                review_rec,
                                time_extracted=extraction_time,
                            )
                            singer.write_bookmark(
                                state,
                                repo_path,
                                "pull_request_comments",
                                {"since": singer.utils.strftime(extraction_time)},
                            )

                            pull_request_comments_counter.increment()

                    if schemas.get("pull_request_files"):
                        for pr_file in get_pull_request_files(
                            pr_id,
                            pr_number,
                            schemas["pull_request_files"],
                            repo_path,
                            state,
                            mdata["pull_request_files"],
                        ):
                            singer.write_record(
                                "pull_request_files",
                                pr_file,
                                time_extracted=extraction_time,
                            )
                            singer.write_bookmark(
                                state,
                                repo_path,
                                "pull_request_files",
                                {"since": singer.utils.strftime(extraction_time)},
                            )
                            pull_request_files_counter.increment()

                    if schemas.get("pull_request_commits"):
                        for pull_request_stats in get_commits_for_pr(
                            pr_id,
                            pr_number,
                            schemas["pull_request_commits"],
                            repo_path,
                            state,
                            mdata["pull_request_commits"],
                        ):
                            singer.write_record(
                                "pull_request_commits",
                                pull_request_stats,
                                time_extracted=extraction_time,
                            )
                            singer.write_bookmark(
                                state,
                                repo_path,
                                "pull_request_commits",
                                {"since": singer.utils.strftime(extraction_time)},
                            )
                            pull_request_commits_counter.increment()

                    if schemas.get("pull_request_stats"):
                        for pull_request_stats in get_pull_request_stats(
                            pr_id,
                            pr_number,
                            schemas["pull_request_stats"],
                            repo_path,
                            state,
                            mdata["pull_request_stats"],
                        ):
                            singer.write_record(
                                "pull_request_stats",
                                pull_request_stats,
                                time_extracted=extraction_time,
                            )
                            singer.write_bookmark(
                                state,
                                repo_path,
                                "pull_request_stats",
                                {"since": singer.utils.strftime(extraction_time)},
                            )
                            pull_request_stats_counter.increment()

                    if schemas.get("pull_request_details"):
                        for pull_request_stats in get_pull_request_details(
                            pr_id,
                            pr_number,
                            schemas["pull_request_details"],
                            repo_path,
                            state,
                            mdata["pull_request_details"],
                        ):
                            singer.write_record(
                                "pull_request_details",
                                pull_request_stats,
                                time_extracted=extraction_time,
                            )
                            singer.write_bookmark(
                                state,
                                repo_path,
                                "pull_request_details",
                                {"since": singer.utils.strftime(extraction_time)},
                            )
                            pull_request_details_counter.increment()

    return state


def get_commits_for_pr(pr_id, pr_number, schema, repo_path, state, mdata):
    for response in authed_get_all_pages(
        "pr_commits",
        f"{BASE_URL}/repositories/{repo_path}/pullrequests/{pr_number}/commits",
    ):
        commit_data = response.json()
        for commit in commit_data["values"]:
            commit["_sdc_repository"] = repo_path
            commit["pr_number"] = pr_number
            commit["pr_id"] = pr_id
            commit["id"] = "{}-{}".format(pr_id, commit["hash"])
            with singer.Transformer() as transformer:
                rec = transformer.transform(
                    commit, schema, metadata=metadata.to_map(mdata)
                )
            yield rec

        return state


def get_comments_for_pr(pr_id, pr_number, schema, repo_path, state, mdata):
    for response in authed_get_all_pages(
        "pull_request_comments",
        f"{BASE_URL}/repositories/{repo_path}/pullrequests/{pr_number}/comments",
    ):
        comments = response.json()
        for comment in comments["values"]:
            comment["_sdc_repository"] = repo_path
            comment["pr_id"] = pr_id
            comment["pr_number"] = pr_number
            comment["number"] = comment["id"]
            comment["id"] = "{}-{}".format(pr_id, comment["id"])
            with singer.Transformer() as transformer:
                rec = transformer.transform(
                    comment, schema, metadata=metadata.to_map(mdata)
                )
            yield rec
        return state


def get_pull_request_stats(pr_id, pr_number, schema, repo_path, state, mdata):
    for response in authed_get_all_pages(
        "pull_request_stats",
        f"{BASE_URL}/repositories/{repo_path}/pullrequests/{pr_number}/diffstat",
    ):
        stats = response.json()
        for stat in stats["values"]:
            stat["pr_id"] = pr_id
            stat["pr_number"] = pr_number
            stat["_sdc_repository"] = repo_path
            with singer.Transformer() as transformer:
                rec = transformer.transform(
                    stat, schema, metadata=metadata.to_map(mdata)
                )
            yield rec
        return state


def get_pull_request_files(pr_id, pr_number, schema, repo_path, state, mdata):
    logger.info("Starting to process files for PR %s", pr_id)
    for response in authed_get_all_pages(
        "pull_request_files",
        f"{BASE_URL}/repositories/{repo_path}/pullrequests/{pr_number}/patch",
    ):
        file = {}
        file["file"] = str(response.content).replace('"', "'")
        if not response.content:
            return state
        file["_sdc_repository"] = repo_path
        file["pr_id"] = pr_id
        file["pr_number"] = pr_number
        with singer.Transformer() as transformer:
            rec = transformer.transform(file, schema, metadata=metadata.to_map(mdata))
        yield rec

    return state


def get_pull_request_details(pr_id, pr_number, schema, repo_path, state, mdata):
    for response in authed_get_all_pages(
        "pull_request_details",
        f"{BASE_URL}/repositories/{repo_path}/pullrequests/{pr_number}",
    ):
        details = response.json()
        details["id"] = pr_id
        details["pr_number"] = pr_number
        details["_sdc_repository"] = repo_path
        with singer.Transformer() as transformer:
            rec = transformer.transform(
                details, schema, metadata=metadata.to_map(mdata)
            )
            yield rec
        return state


def get_all_commits(schema, repo_path, state, mdata, start_date):
    """
    https://developer.github.com/v3/repos/commits/#list-commits-on-a-repository
    """
    bookmark = get_bookmark(state, repo_path, "commits", "since", start_date)
    if bookmark:
        query_string = "?since={}".format(bookmark)
    else:
        query_string = ""

    with metrics.record_counter("commits") as counter:
        for response in authed_get_all_pages(
            "commits", f"{BASE_URL}/repositories/{repo_path}/commits{query_string}"
        ):
            commits = response.json()
            extraction_time = singer.utils.now()
            for commit in commits["values"]:
                commit["_sdc_repository"] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(
                        commit, schema, metadata=metadata.to_map(mdata)
                    )
                singer.write_record("commits", rec, time_extracted=extraction_time)
                singer.write_bookmark(
                    state,
                    repo_path,
                    "commits",
                    {"since": singer.utils.strftime(extraction_time)},
                )
                counter.increment()

    return state


def get_all_deployments(schema, repo_path, state, mdata, start_date):
    """
    https://docs.github.com/en/rest/deployments/deployments?apiVersion=2022-11-28#list-deployments
    """
    bookmark_value = get_bookmark(state, repo_path, "deployments", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter("deployments") as counter:
        for response in authed_get_all_pages(
            "deployments", f"{BASE_URL}/repositories/{repo_path}/deployments"
        ):
            deployments = response.json()
            extraction_time = singer.utils.now()
            for deployment in deployments["values"]:
                if (
                    bookmark_time
                    and singer.utils.strptime_to_utc(deployment.get("created_on"))
                    < bookmark_time
                ):
                    return state
                deployment["_sdc_repository"] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(
                        deployment, schema, metadata=metadata.to_map(mdata)
                    )
                singer.write_record("deployments", rec, time_extracted=extraction_time)
                singer.write_bookmark(
                    state,
                    repo_path,
                    "deployments",
                    {"since": singer.utils.strftime(extraction_time)},
                )
                counter.increment()

    return state


def get_all_organization_members(schemas, workspace, state, mdata, _start_date):
    with metrics.record_counter("organization_members") as counter:
        extraction_time = singer.utils.now()
        for response in authed_get_all_pages(
            "organization_members", f"{BASE_URL}/workspaces/{workspace}/members"
        ):
            organization_members = response.json()
            for member in organization_members["values"]:
                # transform and write release record
                member["uuid"] = member["user"]["uuid"]
                with singer.Transformer() as transformer:
                    team_members_rec = transformer.transform(
                        member, schemas, metadata=metadata.to_map(mdata)
                    )
                    singer.write_record(
                        "organization_members",
                        team_members_rec,
                        time_extracted=extraction_time,
                    )
                counter.increment()

    return state


def get_selected_streams(catalog):
    """
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    """
    selected_streams = []
    for stream in catalog["streams"]:
        stream_metadata = stream["metadata"]
        if stream["schema"].get("selected", False):
            selected_streams.append(stream["tap_stream_id"])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry["breadcrumb"] and entry["metadata"].get("selected", None):
                    selected_streams.append(stream["tap_stream_id"])

    return selected_streams


def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog["streams"]:
        if stream["tap_stream_id"] == stream_id:
            return stream
    return None


# return the 'timeout'
def get_request_timeout():
    args = singer.utils.parse_args([])
    # get the value of request timeout from config
    config_request_timeout = args.config.get("request_timeout")

    # only return the timeout value if it is passed in the config and the value is not 0, "0" or ""
    if config_request_timeout and float(config_request_timeout):
        # return the timeout from config
        return float(config_request_timeout)

    # return default timeout
    return REQUEST_TIMEOUT


def get_jwt_token(config):
    # Get app token
    private_key = config["private_key"]
    app_id = config["app_id"]

    now = int(datetime.now().timestamp())

    payload = {"iat": now - 60, "exp": now + (10 * 60), "iss": app_id}

    jwt_token = jwt.encode(payload, private_key, algorithm="RS256")

    # Get client token
    inst_id = config["installation_id"]
    url = f"https://api.github.com/app/installations/{inst_id}/access_tokens"

    header = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {jwt_token}",
    }

    response = requests.post(url, headers=header)
    token = response.json()["token"]

    return token


def refresh_oauth_token(config):
    """
    Refreshes the OAuth access token using the refresh token
    and updates the config file with the new tokens.

    Args:
        config (dict): The current configuration with refresh_token

    Returns:
        dict: Updated config with new access_token and refresh_token
    """
    global access_token_expires_at
    global refresh_token_expires_at
    if "refresh_token" not in config:
        logger.error("No refresh token found in config, cannot refresh access token")
        raise BadCredentialsException("No refresh token found in config")

    logger.info("Refreshing OAuth access token")

    token_url = "https://github.com/login/oauth/access_token"

    data = {
        "client_id": config["client_id"],
        "client_secret": config["client_secret"],
        "refresh_token": config["refresh_token"],
        "grant_type": "refresh_token",
    }

    headers = {"Accept": "application/json"}

    try:
        response = requests.post(token_url, data=data, headers=headers)
        response.raise_for_status()
        token_data = response.json()

        config["access_token"] = token_data["access_token"]
        now = datetime.now(pytz.UTC)
        if "expires_in" in token_data:
            config["access_token_expires_at"] = (
                now + timedelta(seconds=token_data["expires_in"] - 60)
            ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            access_token_expires_at = config["access_token_expires_at"]
        if "refresh_token_expires_in" in token_data:
            config["refresh_token_expires_at"] = (
                now + timedelta(seconds=token_data["refresh_token_expires_in"] - 60)
            ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            refresh_token_expires_at = config["refresh_token_expires_at"]
        if "refresh_token" in token_data:
            config["refresh_token"] = token_data["refresh_token"]
        save_config(config)

        session.headers["Authorization"] = "Bearer " + config["access_token"]

        logger.info("Successfully refreshed OAuth access token")
        return config

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to refresh OAuth token: {str(e)}")
        raise BadCredentialsException(f"Failed to refresh OAuth token: {str(e)}")


def save_config(config):
    """
    Saves the updated config back to the config file.

    Args:
        config (dict): The updated configuration
    """
    try:
        with open(config_path, "w") as f:
            json.dump(config, f, indent=4)
        logger.info(f"Updated config saved to {config_path}")
    except Exception as e:
        logger.warning(f"Failed to save updated config: {str(e)}")


SYNC_FUNCTIONS = {
    "pull_requests": get_all_pull_requests,
    "organization_members": get_all_organization_members,
    "deployments": get_all_deployments,
}

SUB_STREAMS = {
    "pull_requests": [
        "pull_request_comments",
        "pull_request_stats",
        "pull_request_details",
        "pull_request_files",
        "pull_request_commits",
        "pull_request_details",
    ],
}


def do_sync(config, state, catalog):
    access_token = config["access_token"]
    if config["is_jwt_token"]:
        session.headers.update({"authorization": "token " + access_token})
    else:
        session.headers.update({"Authorization": "Bearer " + access_token})

    start_date = config["start_date"] if "start_date" in config else None
    # get selected streams, make sure stream dependencies are met
    selected_stream_ids = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)

    repositories = extract_repos_from_config(config)

    workspace: str = config.get("workspace")

    state = translate_state(state, catalog, repositories)
    singer.write_state(state)

    # pylint: disable=too-many-nested-blocks
    for repo in repositories:
        logger.info("Starting sync of repository: %s", repo)
        for stream in catalog["streams"]:
            stream_id = stream["tap_stream_id"]
            stream_schema = stream["schema"]
            mdata = stream["metadata"]

            # if it is a "sub_stream", it will be sync'd by its parent
            if not SYNC_FUNCTIONS.get(stream_id):
                continue

            # if stream is selected, write schema and sync
            if stream_id in selected_stream_ids:
                singer.write_schema(stream_id, stream_schema, stream["key_properties"])

                # get sync function and any sub streams
                sync_func = SYNC_FUNCTIONS[stream_id]
                sub_stream_ids = SUB_STREAMS.get(stream_id, None)

                # sync stream
                if not sub_stream_ids:
                    if stream_id == "organization_members":
                        state = sync_func(
                            stream_schema, workspace, state, mdata, start_date
                        )
                    else:
                        state = sync_func(stream_schema, repo, state, mdata, start_date)

                # handle streams with sub streams
                else:
                    stream_schemas = {stream_id: stream_schema}
                    stream_mdata = {stream_id: mdata}

                    # get and write selected sub stream schemas
                    for sub_stream_id in sub_stream_ids:
                        if sub_stream_id in selected_stream_ids:
                            sub_stream = get_stream_from_catalog(sub_stream_id, catalog)
                            stream_schemas[sub_stream_id] = sub_stream["schema"]
                            stream_mdata[sub_stream_id] = sub_stream["metadata"]
                            singer.write_schema(
                                sub_stream_id,
                                sub_stream["schema"],
                                sub_stream["key_properties"],
                            )
                    # sync stream and it's sub streams
                    state = sync_func(
                        stream_schemas, repo, state, stream_mdata, start_date
                    )

                singer.write_state(state)


@singer.utils.handle_top_exception(logger)
def main():
    global access_token_expires_at
    global refresh_token_expires_at
    global config_path

    # Store config path for later use
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="config.json")
    path_args, unknown = parser.parse_known_args()
    config_path = path_args.config

    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    args.config["is_jwt_token"] = False

    access_token_expires_at = args.config.get("access_token_expires_at")
    refresh_token_expires_at = args.config.get("refresh_token_expires_at")

    if not args.config.get("access_token"):
        if (
            "app_id" in args.config
            and "private_key" in args.config
            and "installation_id" in args.config
        ):
            args.config["access_token"] = get_jwt_token(args.config)
            args.config["is_jwt_token"] = True
        elif (
            "refresh_token" in args.config
            and "client_id" in args.config
            and "client_secret" in args.config
        ):
            args.config = refresh_oauth_token(args.config)
        else:
            raise BadCredentialsException(
                "No valid authentication method provided. Either provide an access_token, or app credentials, or OAuth credentials with refresh token."
            )

    if args.discover:
        do_discover(args.config)
    else:
        catalog = args.properties if args.properties else get_catalog()

        do_sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()

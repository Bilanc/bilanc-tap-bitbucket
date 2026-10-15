import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import backoff
import requests
import singer

logger = singer.get_logger()

NANGO_API_URL = "https://api.nango.dev"
NANGO_PROVIDER_CONFIG_KEY = "bitbucket"
NANGO_REQUEST_TIMEOUT = 30
NANGO_MAX_TRIES = 5
NANGO_MAX_RETRY_AFTER = 60
NANGO_FORCE_REFRESH_RETRY_DELAY = 5
NANGO_EXPIRES_AT_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
# The tap refreshes this long before `expires_at`; a token taken from the
# no-force fallback must outlive it or the tap would refresh again right away.
NANGO_EXPIRY_MARGIN = timedelta(minutes=20)


class NangoServerError(Exception):
    """Nango answered 429 or 5xx; retried by `fetch_nango_connection`."""


class NangoRefreshError(Exception):
    """Nango could not serve credentials for the connection."""


def nango_error_details(response: requests.Response) -> str:
    """Nango's `error.code: error.message`; never the connection payload."""
    try:
        error = response.json().get("error")
    except (ValueError, AttributeError):
        error = None
    if isinstance(error, dict):
        error = ": ".join(str(v) for v in (error.get("code"), error.get("message")) if v)
    return str(error or response.reason or f"HTTP {response.status_code}")


def _retry_after(response: requests.Response) -> int:
    try:
        return min(int(response.headers.get("Retry-After", 0)), NANGO_MAX_RETRY_AFTER)
    except ValueError:
        return 0


@backoff.on_exception(
    backoff.expo,
    (requests.RequestException, NangoServerError),
    max_tries=NANGO_MAX_TRIES,
)
def fetch_nango_connection(
    config: Dict[str, Any], provider_config_key: str, force_refresh: bool
) -> requests.Response:
    """GET the Nango connection, retrying timeouts, connection errors, 429 and 5xx."""
    url = (
        f"{NANGO_API_URL}/connection/{config['nango_connection_id']}"
        f"?provider_config_key={provider_config_key}"
    )
    if force_refresh:
        url += "&force_refresh=true"

    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {config['nango_secret_key']}"},
        timeout=NANGO_REQUEST_TIMEOUT,
    )

    if response.status_code == 429:
        time.sleep(_retry_after(response))
    if response.status_code == 429 or response.status_code >= 500:
        raise NangoServerError(
            f"Nango returned {response.status_code}: {nango_error_details(response)}"
        )
    return response


def _outlives_margin(expires_at: Optional[str]) -> bool:
    try:
        expiry = datetime.strptime(expires_at, NANGO_EXPIRES_AT_FORMAT)
    except (TypeError, ValueError):
        return False
    return expiry - NANGO_EXPIRY_MARGIN > datetime.utcnow()


def refresh_nango_token(
    config: Dict[str, Any],
    provider_config_key: str = NANGO_PROVIDER_CONFIG_KEY,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """Force-refresh the Nango connection and return (config, expires_at).

    A 400 on the forced refresh (Bitbucket rejected that one refresh) is
    retried once without `force_refresh`; the token Nango still holds is used
    if it outlives `NANGO_EXPIRY_MARGIN`. Any other non-2xx raises
    `NangoRefreshError` with Nango's error code/message.
    """
    for key in ("nango_secret_key", "nango_connection_id"):
        if not config.get(key):
            raise ValueError(f"{key} not configured")

    response = fetch_nango_connection(config, provider_config_key, force_refresh=True)

    forced_refresh_error = None
    if response.status_code == 400:
        forced_refresh_error = nango_error_details(response)
        logger.warning(
            "Nango force_refresh failed (%s); retrying without force_refresh",
            forced_refresh_error,
        )
        time.sleep(NANGO_FORCE_REFRESH_RETRY_DELAY)
        response = fetch_nango_connection(config, provider_config_key, force_refresh=False)

    if not response.ok:
        raise NangoRefreshError(
            f"Nango returned {response.status_code} for connection "
            f"{config['nango_connection_id']}: {nango_error_details(response)}"
        )

    credentials = response.json().get("credentials", {})
    expires_at = credentials.get("expires_at")

    if forced_refresh_error and not _outlives_margin(expires_at):
        raise NangoRefreshError(
            f"Nango returned 400 on force_refresh for connection "
            f"{config['nango_connection_id']}: {forced_refresh_error}; "
            f"the token Nango still holds expires at {expires_at}, too soon to use"
        )

    config["access_token"] = credentials.get("access_token")
    return config, expires_at

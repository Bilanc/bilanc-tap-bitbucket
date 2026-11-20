import unittest
from unittest import mock

import requests

import tap_bitbucket


def not_found_response():
    result = requests.Response()
    result.status_code = 404
    result._content = b'{"type": "error", "error": {"message": "Repository not found"}}'
    return result


@mock.patch("tap_bitbucket.refresh_token_if_expired", return_value=None)
@mock.patch("tap_bitbucket.get_request_timeout", return_value=300)
@mock.patch("singer.write_record")
@mock.patch.object(tap_bitbucket.session, "request")
class TestNotFoundRepoIsSkipped(unittest.TestCase):
    repo = "ws/deleted-repo"
    start_date = "2020-01-01T00:00:00Z"

    def test_pull_requests(self, request, write_record, _timeout, _refresh):
        request.return_value = not_found_response()

        state = tap_bitbucket.get_all_pull_requests(
            {}, self.repo, {}, {}, self.start_date
        )

        self.assertEqual(state, {})
        write_record.assert_not_called()

    def test_deployments(self, request, write_record, _timeout, _refresh):
        request.return_value = not_found_response()

        state = tap_bitbucket.get_all_deployments(
            {}, self.repo, {}, [], self.start_date
        )

        self.assertEqual(state, {})
        write_record.assert_not_called()

    def test_deployment_environments(self, request, write_record, _timeout, _refresh):
        request.return_value = not_found_response()

        state = tap_bitbucket.get_all_deployment_environments(
            {}, self.repo, {}, [], self.start_date
        )

        self.assertEqual(state, {})
        write_record.assert_not_called()

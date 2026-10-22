import unittest
from unittest import mock

import requests
import urllib3

import tap_bitbucket


def ok_response():
    result = requests.Response()
    result.status_code = 200
    result._content = b'{"values": []}'
    return result


@mock.patch("tap_bitbucket.get_request_timeout", return_value=300)
@mock.patch("time.sleep", return_value=None)
@mock.patch.object(tap_bitbucket.session, "request")
class TestAuthedGetRetry(unittest.TestCase):
    url = "https://api.bitbucket.org/2.0/repositories/ws/repo/pullrequests"

    def test_retries_chunked_encoding_error(self, request, _sleep, _timeout):
        request.side_effect = [
            requests.exceptions.ChunkedEncodingError(
                "Connection broken: InvalidChunkLength(got length b'', 0 bytes read)"
            ),
            ok_response(),
        ]

        resp = tap_bitbucket.authed_get("pull_requests", self.url)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(request.call_count, 2)

    def test_retries_urllib3_protocol_error(self, request, _sleep, _timeout):
        request.side_effect = [
            urllib3.exceptions.ProtocolError("Connection broken"),
            ok_response(),
        ]

        resp = tap_bitbucket.authed_get("pull_requests", self.url)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(request.call_count, 2)

    def test_gives_up_after_max_tries(self, request, _sleep, _timeout):
        request.side_effect = requests.exceptions.ChunkedEncodingError("Connection broken")

        with self.assertRaises(requests.exceptions.ChunkedEncodingError):
            tap_bitbucket.authed_get("pull_requests", self.url)

        self.assertEqual(request.call_count, 10)

    def test_does_not_retry_other_request_exceptions(self, request, _sleep, _timeout):
        request.side_effect = requests.exceptions.InvalidURL("bad url")

        with self.assertRaises(requests.exceptions.InvalidURL):
            tap_bitbucket.authed_get("pull_requests", self.url)

        self.assertEqual(request.call_count, 1)

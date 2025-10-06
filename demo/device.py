"""
Code for obtaining a token via OAuth2 Device Flow
"""

import logging
import time

import requests
import urllib3

_log = logging.getLogger(__name__)


class DeviceClientError(Exception):
    """Errors while trying to the device flow."""


class DeviceClientUnexpectedOutput(DeviceClientError):
    """Server responded with something unexpected."""


class DeviceClientTimedOut(DeviceClientError):
    """The device flow session expired."""


class DeviceClientRequestNotInProgress(DeviceClientError):
    """No device flow session is in progress."""


class DeviceClientAccessDenied(DeviceClientError):
    """The user denied the token request."""


class DeviceClient:
    GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"
    REQUEST_ENDPOINT = "/auth/device_authorization"

    def __init__(self, webapp_server: str, client_id: str):
        self.request_url = f"{webapp_server}{self.REQUEST_ENDPOINT}"
        self.client_id = client_id
        self.device_code = ""
        self.expires_at = 0.0
        self.interval = 0
        self.user_code = ""
        self.verification_uri = ""
        self.verification_uri_complete = ""
        self.request_in_progress = False

    def make_request(self) -> "DeviceClient":
        try:
            response = requests.post(
                url=self.request_url,
                data={"client_id": self.client_id},
            )
        except (OSError, urllib3.exceptions.HTTPError) as err:
            raise DeviceClientError(
                "Initial request failed to connect to server: %s" % err
            ) from err
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            msg = "Initial request resulted in %s" % err
            try:
                rj = response.json()
                msg += "; message from server: %s" % rj["error"]
            except (TypeError, KeyError, ValueError):
                pass
            raise DeviceClientError(msg)
        try:
            rj = response.json()
        except requests.exceptions.JSONDecodeError as err:
            raise DeviceClientUnexpectedOutput("Invalid JSON: %s" % err)
        try:
            self.device_code = rj["device_code"]
            expires_in = rj["expires_in"]
            self.expires_at = time.time() + float(expires_in)
            self.interval = int(rj.get("interval", 5))
            self.user_code = rj["user_code"]
            self.verification_uri = rj["verification_uri"]
            self.verification_uri_complete = rj.get(
                "verification_uri_complete", self.verification_uri
            )
        except KeyError as err:
            raise DeviceClientUnexpectedOutput("Server response missing %s" % err)
        except ValueError as err:
            raise DeviceClientUnexpectedOutput(
                "Server responded with unexpected output %s" % err
            ) from err
        self.request_in_progress = True
        return self

    def poll_for_token(self) -> bytes | None:
        if not self.request_in_progress:
            raise DeviceClientRequestNotInProgress()
        try:
            response = requests.post(
                url=self.request_url,
                data={
                    "client_id": self.client_id,
                    "grant_type": self.GRANT_TYPE,
                    "device_code": self.device_code,
                },
            )
            response_json = response.json()
        except requests.exceptions.JSONDecodeError as err:
            raise DeviceClientUnexpectedOutput("Invalid JSON: %s" % err)
        except (OSError, urllib3.exceptions.HTTPError) as err:
            raise DeviceClientError("Lost connection to server: %s" % err) from err

        if response.status_code == 400:
            try:
                error: str = response_json["error"]
            except KeyError:
                raise DeviceClientUnexpectedOutput("Unknown failure from server")
            if error == "authorization_pending":
                return None
            if error == "slow_down":
                self.interval += 5
                _log.debug("Received slow_down; interval set to %d", self.interval)
                return None
            if error == "access_denied":
                raise DeviceClientAccessDenied()
            if error == "expired_token":
                raise DeviceClientTimedOut("Server responds device code expired")

            raise DeviceClientUnexpectedOutput(
                "Server responds with unexpected failure %s" % error
            )

        elif response.status_code == 200:
            try:
                access_token = response_json["access_token"]
                token_type = response_json["token_type"]
                # expires_in = response_json.get("expires_in", None)
            except KeyError as err:
                raise DeviceClientUnexpectedOutput("Response missing %s" % err)
            if token_type.lower() != "placement":
                raise DeviceClientUnexpectedOutput(
                    "Unexpected token type %s" % token_type
                )
            try:
                access_token_b = access_token.encode()
            except (TypeError, AttributeError, UnicodeEncodeError) as err:
                raise DeviceClientUnexpectedOutput(
                    "Failed to encode access token: %r" % err
                )
            return access_token_b

    def poll_for_token_loop(self) -> bytes:
        if not self.request_in_progress:
            raise DeviceClientRequestNotInProgress()

        while time.time() < self.expires_at:
            access_token_b = self.poll_for_token()
            if access_token_b is None:
                time.sleep(self.interval)
            else:
                return access_token_b

        raise DeviceClientTimedOut("Device code expired")

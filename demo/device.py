"""
device.py
-----------

This module provides the DeviceClient class for handling OAuth2 Device Flow authentication.
It allows a client to obtain an access token by guiding the user through the device authorization process.

Features:
- Initiates device authorization requests
- Polls for token completion
- Handles device code expiration, polling intervals, and error conditions

Usage:
    client = DeviceClient(webapp_server, client_id)
    client.make_request()
    # Display client.user_code and client.verification_uri to the user
    token = client.poll_for_token_loop()
    # Use the token as needed
"""

import logging
import time
import typing as t

import requests
import urllib3

_log = logging.getLogger(__name__)


class DeviceClientError(Exception):
    """Errors while trying to the device flow."""


class DeviceClientInitialRequestError(DeviceClientError):
    """Some failure to make the initial request to the remote server."""


class DeviceClientUnexpectedOutput(DeviceClientError):
    """Server responded with something unexpected."""


class DeviceClientTimedOut(DeviceClientError):
    """The device flow session expired."""


class DeviceClientRequestNotInProgress(DeviceClientError):
    """No device flow session is in progress."""


class DeviceClientAccessDenied(DeviceClientError):
    """The user denied the token request."""


class DeviceClient:
    """
    Client for obtaining tokens via OAuth2 Device Flow.

    This class handles the device flow authorization process, including:
    - Making the initial device authorization request
    - Polling for token completion
    - Managing device code expiration and polling intervals
    """

    GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"
    REQUEST_ENDPOINT = "/auth/device_authorization"

    def __init__(self, webapp_server: str, client_id: str):
        self.request_url = f"{webapp_server}{self.REQUEST_ENDPOINT}"
        self.client_id = client_id
        self._reset_attrs()

    def _reset_attrs(self):
        self.device_code = ""
        self.expires_at = 0.0
        self.interval = 0
        self.user_code = ""
        self.verification_uri = ""
        self.verification_uri_complete = ""
        self.request_in_progress = False
        self.access_token = b""

    def make_request(self) -> "DeviceClient":
        """
        Starts the session for the device flow by making the initial request
        to the webapp server for the token.

        Returns self for convenience.

        Raises:
            DeviceClientInitialRequestError:
                If we couldn't connect to the server or got an immediate
                error response.
            DeviceClientUnexpectedOutput:
                If the message from the server is malformed somehow.
        """
        self._reset_attrs()
        try:
            response = requests.post(
                url=self.request_url,
                data={"client_id": self.client_id},
            )
        except (OSError, urllib3.exceptions.HTTPError) as err:
            raise DeviceClientInitialRequestError(
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

    def poll_for_token(self) -> t.Optional[bytes]:
        """
        Poll the server performing the device flow for the placement token.
        Returns the token; also sets self.access_token to the token.

        Returns:
            The placement token encoded as bytes if successful, None if
            authorization is still pending.

        Raises:
            DeviceClientRequestNotInProgress: If no device flow session is in progress.
            DeviceClientUnexpectedOutput: If the server response is invalid or unexpected.
            DeviceClientAccessDenied: If the user denied the token request.
            DeviceClientTimedOut: If the device code has expired.
            DeviceClientError: If connection to server is lost.
        """
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
            self.access_token = access_token_b
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

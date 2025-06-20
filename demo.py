import datetime
import html
import logging
import os
import pathlib
import sys
import time
import typing as t

import dateutil
import htcondor2
import requests
import urllib3
import ipywidgets as widgets
from IPython.display import display


DEVICE_CLIENT_ID = os.environ.get("DEVICE_CLIENT_ID") or "placement_demo_notebook"
WEBAPP_SERVER = os.environ.get("PLACEMENT_WEBAPP_LINK") or "http://localhost:5000"
TOKEN_FILENAME = "Placement.token"

_log = logging.getLogger(__name__)


def write_token(token_filename: str, token_contents: bytes):
    """
    Write the given bytes to a token file in the condor tokens dir.

    token_filename: The name of the file (without directory) to create,
        under the tokens directory.  (Should end in '.token')

    token_contents: The bytes to write into the token file.
    """
    condor_tokens_dir = pathlib.Path.home() / ".condor/tokens.d"
    condor_tokens_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    token_dest = condor_tokens_dir / token_filename
    with open(token_dest, mode="wb") as fh:
        token_dest.chmod(0o600)
        fh.write(token_contents)


def token_stat(token_filename: str):
    """
    Calls stat() on the token file and returns the results.  If there is
    an error (e.g., the file does not exist), returns None.
    """
    condor_tokens_dir = pathlib.Path.home() / ".condor/tokens.d"
    condor_tokens_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    token_dest = condor_tokens_dir / token_filename
    try:
        return token_dest.stat()
    except OSError:
        return None


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

    _log = _log.getChild("DeviceClient")

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

    def poll_for_token(self) -> t.Optional[bytes]:
        if not self.request_in_progress:
            raise DeviceClientRequestNotInProgress()
        response = requests.post(
            url=self.request_url,
            data={
                "client_id": self.client_id,
                "grant_type": self.GRANT_TYPE,
                "device_code": self.device_code,
            },
        )
        try:
            response_json = response.json()
        except requests.exceptions.JSONDecodeError as err:
            raise DeviceClientUnexpectedOutput("Invalid JSON: %s" % err)

        if response.status_code == 400:
            try:
                error: str = response_json["error"]
            except KeyError:
                raise DeviceClientUnexpectedOutput("Unknown error from server")
            if error == "authorization_pending":
                return None
            if error == "slow_down":
                self.interval += 1
                self._log.debug("Received slow_down; interval set to %d", self.interval)
                return None
            if error == "access_denied":
                raise DeviceClientAccessDenied()
            if error == "expired_token":
                raise DeviceClientTimedOut("Server responds device code expired")

            raise DeviceClientUnexpectedOutput(
                "Server responds with unexpected error %s" % error
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
                    "Error encoding access token: %r" % err
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


class DeviceWidgets:
    def __init__(self):
        maybe_tz = os.environ.get("TIMEZONE")
        if maybe_tz:
            self.tz = dateutil.tz.gettz(maybe_tz)  # type: ignore
        else:
            self.tz = dateutil.tz.gettz()  # type: ignore

        # The description on the Button widget doesn't fit the default
        # layout so set up one of our own.  See
        # https://ipywidgets.readthedocs.io/en/latest/examples/Widget%20Layout.html#examples
        items_layout = widgets.Layout(width="auto")
        box_layout = widgets.Layout(
            display="flex", flex_flow="column", align_items="stretch", width="50%"
        )
        # A button for starting the token request
        self.start_token_request_button = widgets.Button(
            description="Request Token", button_style="primary", layout=items_layout
        )
        self.start_token_request_button.on_click(
            lambda button: self.on_request_token_click(button)
        )
        # A label that will contain the link to the token request page and the code to type in.
        self.user_instructions_html = widgets.HTML(
            "Click the button to start the token request"
        )
        # A label that will contain the message status
        self.status_html: widgets.HTML = widgets.HTML()
        self.box = widgets.Box(
            [
                self.user_instructions_html,
                self.start_token_request_button,
                self.status_html,
            ],
            layout=box_layout,
        )

        self.client = DeviceClient(WEBAPP_SERVER, DEVICE_CLIENT_ID)

    def on_request_token_click(self, button: widgets.Button):
        try:
            self.client.make_request()
        except DeviceClientError as err:
            self.status_html.value = (
                "Initial request failed. The error message was:<br>%s"
                % html.escape(str(err))
            )
            self.user_instructions_html.value = (
                "The token request failed; please try again."
            )
            return
        button.description = "Token request in progress, please wait"
        button.disabled = True
        try:
            link = html.escape(self.client.verification_uri)
            link_complete = html.escape(self.client.verification_uri_complete)
            code = html.escape(self.client.user_code)
            self.user_instructions_html.value = (
                f'Please go to the following link: <u><a href="{link_complete}" target="_blank">{link}</a></u>, '
                f"and type in this code: <strong><kbd>{code}</kbd></strong>"
            )
            access_token_b = None
            try:
                while time.time() < self.client.expires_at:
                    expires_in = self.client.expires_at - time.time()
                    expires_minutes = int(expires_in) // 60
                    expires_seconds = int(expires_in) % 60
                    self.status_html.value = (
                        "Request in progress, will expire in %d:%02d"
                        % (
                            expires_minutes,
                            expires_seconds,
                        )
                    )
                    try:
                        access_token_b = self.client.poll_for_token()
                    except DeviceClientError as err:
                        self.status_html.value = "Request failed:<br>%s" % html.escape(
                            str(err)
                        )
                        break
                    if access_token_b is not None:
                        break
                    time.sleep(self.client.interval)
            except KeyboardInterrupt:
                self.status_html.value = "Request cancelled"
                raise

            if access_token_b:
                self.status_html.value = "Request successful"
                write_token(TOKEN_FILENAME, access_token_b)
                self.status_html.value = "Request successful, token installed"
                display(self.status_html)  # Force update?
        finally:
            button.description = "Click to Request Token"
            button.disabled = False
            display(button)  # Force update?

    def display_widgets(self):
        display(self.start_token_request_button)
        display(self.user_instructions_html)
        display(self.status_html)


class TokenFileUploadWidgets:
    def __init__(self):
        maybe_tz = os.environ.get("TIMEZONE")
        if maybe_tz:
            self.tz = dateutil.tz.gettz(maybe_tz)  # type: ignore
        else:
            self.tz = dateutil.tz.gettz()  # type: ignore

        # The description on the FileUpload widget doesn't fit the default
        # layout so set up one of our own.  See
        # https://ipywidgets.readthedocs.io/en/latest/examples/Widget%20Layout.html#examples
        items_layout = widgets.Layout(width="auto")
        box_layout = widgets.Layout(
            display="flex", flex_flow="column", align_items="stretch", width="50%"
        )

        # Create the 'upload' button for uploading the token;
        # call on_token_upload when "value" changes, i.e., a file is uploaded.
        self.token_widget = widgets.FileUpload(
            accept=".tkn,.token",
            description="Click to select a token to upload",
            layout=items_layout,
            multiple=False,
            button_style="primary",
        )
        self.token_widget.observe(self.on_token_upload, names="value")

        self.token_label_widget = widgets.Label("", layout=items_layout)
        self.token_box = widgets.Box(
            [self.token_label_widget, self.token_widget], layout=box_layout
        )

    def set_token_label_text(self):
        """
        Sets the label next to the 'token upload' button to either the
        time of when the token was uploaded (as obtained from the token file
        timestamp) or text asking the user to upload a token.
        """
        # TODO This should just return the text instead of setting the widget.
        ts = token_stat(TOKEN_FILENAME)
        if not ts:
            self.token_label_widget.value = "Please upload a token"
        else:
            dt = datetime.datetime.fromtimestamp(ts.st_ctime, tz=self.tz)
            self.token_label_widget.value = f"Token uploaded at {dt:%H:%M}"

    def on_token_upload(self, change: dict):
        """
        Event handler for uploading a token
        """
        _ = change
        value = self.token_widget.value
        if value:  # we have data
            # Write out the uploaded data to a file
            write_token(TOKEN_FILENAME, value[0].content.tobytes())
            # Clear the value (so saving the widget state won't embed the token)
            # in the notebook.  (Can't use the 'value' local var here, we need
            # to overwrite what's in the widget).
            self.token_widget.value = ()

            # Set the label to show the last successful upload time.
            self.set_token_label_text()
            # now = datetime.datetime.now(tz=self.tz)
            # self.token_label_widget.value = f"Upload successful at {now:%H:%M}"

            self.token_widget.button_style = "success"
            self.token_widget.description = "Token Uploaded"

    def display_widgets(self):
        self.set_token_label_text()
        display(self.token_box)


def setup():
    """
    Set up the widgets in the demo notebook.
    """
    wid = DeviceWidgets()
    wid.display_widgets()


def print_placement_status(placement: htcondor2.SubmitResult, schedd: htcondor2.Schedd):
    """
    Print the status of jobs in the cluster from a placement (SubmitResult).
    """
    query = schedd.query(f"ClusterId == {placement.cluster()}", ["JobStatus"])
    for code, name in [
        (1, "idle"),
        (2, "running"),
        (3, "removed"),
        (4, "completed"),
        (5, "held"),
    ]:
        num_in_status = len([j for j in query if j["JobStatus"] == code])
        if num_in_status > 1:
            print(f"{num_in_status} jobs are {name}.")
        elif num_in_status == 1:
            print(f"1 job is {name}.")

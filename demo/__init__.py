import base64
import datetime
import enum
import html
import json
import logging
import math
import os
import pathlib
import sys
import time
import typing as t

import classad2
import dateutil
import htcondor2
import ipywidgets as widgets
import requests
import urllib3
from IPython.display import display

DEVICE_CLIENT_ID = os.environ.get("DEVICE_CLIENT_ID") or "placement_demo_notebook"
WEBAPP_SERVER = os.environ.get("PLACEMENT_WEBAPP_LINK") or "http://localhost:5000"
TOKEN_FILENAME = "Placement.token"

_log = logging.getLogger(__name__)


MaybeError = t.Union[None, Exception]


class TokenState(enum.Enum):
    MISSING = "MISSING"
    UNREADABLE = "UNREADABLE"
    EXPIRED = "EXPIRED"
    OK = "OK"


#
#
# Utils for installing the token once obtained
#
#


def write_token(token_filename: str, token_contents: bytes):
    """
    Write the given bytes to a token file in the condor tokens dir.

    token_filename: The name of the file (without directory) to create,
        under the tokens directory.  (Should end in '.token')

    token_contents: The bytes to write into the token file.
    """
    condor_tokens_dir = pathlib.Path.home() / ".condor/tokens.d"
    condor_tokens_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    condor_tokens_dir.chmod(0o700)  # mkdir doesn't set the mode if it already exists
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
    token_dest = condor_tokens_dir / token_filename
    try:
        return token_dest.stat()
    except OSError:
        return None


def have_valid_token(
    token_filename: t.Union[str, os.PathLike] = TOKEN_FILENAME,
) -> TokenState:
    """
    Return whether the token is expired, missing, unreadable, or OK
    """
    token_path = pathlib.Path(token_filename)
    try:
        contents = token_path.read_bytes()
    except FileNotFoundError as err:
        _log.debug("%s not found", token_path)
        return TokenState.MISSING
    except OSError as err:
        _log.debug("OSError(%s) reading token %s", err, token_path)
        return TokenState.UNREADABLE
    try:
        body = contents.split(b'.')[1]
        body_json = json.loads(base64.urlsafe_b64decode(body + b'=='))
        expiration = float(body_json["exp"])
    except (IndexError, ValueError) as err:
        _log.debug("Error %s decoding token %s", err, token_path, exc_info=True)
        return TokenState.UNREADABLE
    if expiration < time.time():
        return TokenState.EXPIRED
    return TokenState.OK


#
#
# Code for obtaining a token via OAuth2 Device Flow
#
#


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
                self._log.debug("Received slow_down; interval set to %d", self.interval)
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


#
#
# Widgets for interacting with the Device Flow client
#
#


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
        self.user_instructions_html = widgets.HTML("")
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
        button.description = (
            "Token requested; please follow the link to complete the procedure."
        )
        button.disabled = True
        try:
            link = html.escape(self.client.verification_uri)
            link_complete = html.escape(self.client.verification_uri_complete)
            code = html.escape(self.client.user_code)
            self.user_instructions_html.value = (
                f'<p class="fs-5">Please go to the following link: <u><a href="{link_complete}" target="_blank">{link}</a></u>, '
                f"and type in this code: <strong><kbd>{code}</kbd></strong></p>"
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
                self.user_instructions_html.value = (
                    "You can continue with the rest of the notebook."
                )
                display(self.status_html)  # Force update?
                display(self.user_instructions_html)
        finally:
            button.description = "Request Another Token"
            button.disabled = False
            display(button)  # Force update?

    def display_widgets(self):
        display(self.start_token_request_button)
        display(self.user_instructions_html)
        display(self.status_html)


#
#
# Widgets for uploading a token file
#
#


class TokenFileUploadWidgets:
    def __init__(self):
        self.tz = get_timezone()

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


#
#
# Other functions and classes, mostly for interactive use
#
#


def get_timezone():
    maybe_tz = os.environ.get("TIMEZONE")
    if maybe_tz:
        return dateutil.tz.gettz(maybe_tz)  # type: ignore
    else:
        return dateutil.tz.gettz()  # type: ignore


def setup():
    """
    Set up the widgets in the demo notebook.
    """
    wid = DeviceWidgets()
    wid.display_widgets()


class Placement:
    _log = _log.getChild("Placement")

    MIN_DELAY_BETWEEN_UPDATES = 10.0  # seconds
    # ^^ maybe I should base this on DCDC?
    MAX_STATUS_WAIT = 60.0  # seconds
    HOLD_REASON_CODE_SPOOLING_INPUT = 16
    IN_PROGRESS_STATUSES = [
        "idle",
        "running",
        "transferring_input",
        "transferring_output",
    ]

    def __init__(self, submit_result: htcondor2.SubmitResult, ap: "AP"):
        self.cluster = submit_result.cluster()
        self.num_procs = submit_result.num_procs()
        self.constraint = f"ClusterId == {self.cluster}"
        self.ap = ap
        self.status_last_update = 0.0
        self.status_next_update = 0.0
        self._status = dict(
            idle=0,
            running=0,
            removed=0,
            completed=0,
            held=0,
            transferring_output=0,
            suspended=0,
            transferring_input=0,  # this one is not a real code
        )
        self.tz = get_timezone()
        self._update_status()

    @property
    def status(self) -> dict:
        self._update_status()
        return self._status.copy()

    def _update_status(self, force=False) -> bool:
        """
        Update self.status() with the latest status of the jobs in this
        placement.  Since it's a remote placement, we query the schedd instead
        of using the JobEventLog.  Keep track of the last update time in
        self.status_last_update; don't update more frequently than
        MIN_DELAY_BETWEEN_UPDATES, unless `force` is true.

        Return True if we were able to update, False otherwise.
        """
        now = time.time()
        if not force and now < self.status_next_update:
            self._log.debug("Not updating status yet -- too soon")
            return False
        try:
            query = self.ap.query(self.constraint, ["JobStatus", "HoldReasonCode"])
        except htcondor2.HTCondorException:
            self._log.warning("Unable to update status", exc_info=True)
            return False

        for code, name in enumerate(self._status.keys(), start=1):
            self._status[name] = 0
            for job in query:
                hold_reason_code = job.get("HoldReasonCode")
                if name == "transferring_input":
                    if (
                        job["JobStatus"] == 5
                        and hold_reason_code == self.HOLD_REASON_CODE_SPOOLING_INPUT
                    ):
                        self._status[name] += 1
                elif job["JobStatus"] == code:
                    if not (
                        name == "held"
                        and hold_reason_code == self.HOLD_REASON_CODE_SPOOLING_INPUT
                    ):
                        self._status[name] += 1
        self.status_last_update = time.time()
        self.status_next_update = time.time() + self.MIN_DELAY_BETWEEN_UPDATES
        return True

    def show_status(self):
        """
        Print the status of jobs in the cluster from this placement.
        """
        self._update_status()
        if not self.status_last_update:
            print("Status unknown")
            return
        update_datetime = datetime.datetime.fromtimestamp(
            self.status_last_update, tz=self.tz
        )
        update_time_str = update_datetime.strftime("%T")
        print(f"As of {update_time_str}:")
        for status_name, num_in_status in self._status.items():
            space_name = status_name.replace("_", " ")
            if num_in_status > 1:
                print(f"{num_in_status} jobs are {space_name}.")
            elif num_in_status == 1:
                print(f"1 job is {space_name}.")

    def show_job_ids(self):
        """
        Print the range of job IDs in this cluster.
        """
        if self.num_procs == 1:
            print("Placement has job ID {0}".format(self.cluster))
        else:
            print(
                "Placement has job IDs {0}.0 - {0}.{1}".format(
                    self.cluster, self.num_procs - 1
                )
            )

    def monitor_jobs(self, minutes: t.Union[int, float] = math.inf):
        """
        Loop and wait until all jobs in this placement are no longer in
        progress.  In progress means 'idle', 'running', 'transferring input',
        or 'transferring output', but not 'held', 'completed', 'removed', or
        'suspended'.

        Exit early if we haven't gotten a status update
        in MAX_STATUS_WAIT.
        """
        minutes = float(minutes)
        if minutes < 0.0:
            raise ValueError(f"'minutes' cannot be negative: {minutes}")
        if math.isinf(minutes):
            print("Monitoring jobs")
        elif minutes == 0.0:
            print(f"Checking job status")
            self._update_status(force=True)
            return self.show_status()
        else:
            print(f"Monitoring jobs for up to {minutes:.2g} minutes")
        end_time = time.time() + minutes * 60.0

        # Monitor jobs until the following:
        #   - no update has been received in MAX_STATUS_WAIT seconds
        #   - no jobs in progress (IN_PROGRESS_STATUS)
        #   - the next update would be after end_time
        while True:
            update_success = self._update_status()
            now = time.time()
            time_since_last_update = self.status_last_update - now
            if time_since_last_update > self.MAX_STATUS_WAIT:
                print(f"No update received in {int(time_since_last_update)} seconds.")
                print(f"Stopped monitoring early; please investigate.")
                return
            if update_success:
                print("---------")
                self.show_status()
                if not self.jobs_in_progress:
                    print("No jobs in progress; done monitoring.")
                    return
            if self.status_next_update > end_time:
                print("End time reached; stopped monitoring.")
                return
            time.sleep(self.MIN_DELAY_BETWEEN_UPDATES)

    @property
    def jobs_in_progress(self) -> int:
        """
        The number of jobs still 'in progress'.
        Does _not_ pull the new status from the schedd.
        """
        return sum(
            self._status[status_name] for status_name in self.IN_PROGRESS_STATUSES
        )

    def retrieve_results(self) -> bool:
        """
        Retrieve results for completed jobs.
        Return True if we didn't get any exceptions from HTCondor.

        Hard to get other information (like how many jobs' results were retrieved)
        because HTCondor does not give us that.  We could try to figure that out
        by querying the schedd, getting the list of expected files, and checking them
        out ourselves but that would be inaccurate since the status of the queue
        would be different between retrieval and query...
        """
        try:
            self.ap.schedd.retrieve(job_spec=self.constraint)
        except htcondor2.HTCondorException as err:
            print(
                f"Retreiving results failed with error message {err}", file=sys.stderr
            )
            return False
        print("Retrieving results successful")
        return True


class AP:
    """
    AP is a class for interacting with a remote Access Point, specifically its SchedD.
    """

    def __init__(self, collector_host=None, schedd_host=None):
        self.collector_host = collector_host
        if collector_host:
            collector = htcondor2.Collector(collector_host)
        else:
            collector = htcondor2.Collector()
        self.schedd_host = schedd_host or htcondor2.param["SCHEDD_HOST"]
        self.schedd_ad = collector.locate(htcondor2.DaemonType.Schedd, self.schedd_host)
        self.schedd = htcondor2.Schedd(self.schedd_ad)

    def __getstate__(self):
        """
        Return the object's state, removing unpicklable entries -- in this case,
        the handle to the schedd.  We save the schedd ad instead.
        """
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state["schedd"]
        return state

    def __setstate__(self, state):
        """
        Restore the object's state from a pickle; recreate the handle to the
        schedd using the schedd ad we saved.
        """
        # Restore instance attributes
        self.__dict__.update(state)
        self.schedd = htcondor2.Schedd(self.schedd_ad)

    def place(self, submit_object: htcondor2.Submit) -> Placement:
        try:
            submit_result = self.schedd.submit(submit_object, spool=True)
        except htcondor2.HTCondorException as err:
            if "errmsg=AUTHENTICATE" in str(err):
                raise RuntimeError(
                    "Authentication to the AP failed. You might need to get another token."
                )
            else:
                raise
        self.schedd.spool(submit_result)
        placement = Placement(submit_result, ap=self)
        return placement

    def query(
        self,
        constraint: t.Union[str, classad2.ExprTree] = "True",
        attributes: t.Optional[list[str]] = None,
    ):
        return self.schedd.query(constraint=constraint, projection=attributes or [])

    def get_job_count(
        self, constraint: t.Union[str, classad2.ExprTree] = "True"
    ) -> int:
        return len(
            self.query(constraint=constraint, attributes=["ClusterId", "ProcId"])
        )

    def show_job_count(self, constraint: t.Union[str, classad2.ExprTree] = "True"):
        count = self.get_job_count(constraint)
        if constraint == "True":
            print("There are %d jobs currently placed at the AP." % count)
        else:
            print(
                "There are %d jobs matching the constraint %s currently placed at the AP"
                % (count, constraint)
            )

    def describe_token(self):
        project = None
        user = None
        have_read = False
        have_write = False
        ad = {}
        text = []

        state = have_valid_token()

        match state:
            case TokenState.MISSING:
                print("The token file is missing")
                return False, False, None
            case TokenState.UNREADABLE:
                print("The token file cannot be read or is not a recognizable token")
                return False, False, None
            case TokenState.EXPIRED:
                print("The token is expired.")
                return False, False, None
            case TokenState.OK:
                pass

        try:
            ad = htcondor2.ping(self.schedd_ad, "READ")
            have_read = True
            # ^^ maybe also check ad['AuthorizationSucceeded'] ?
            text.append(
                "You can list jobs and view the details of jobs with your current token."
            )
        except htcondor2.HTCondorException as err:
            if "Failed to start command" in str(err):
                have_read = False
                text.append(
                    "You CANNOT list jobs or view the details of jobs with your current token."
                )
            else:
                raise
        try:
            ad = htcondor2.ping(self.schedd_ad, "WRITE")
            have_write = True
            # ^^ maybe also check ad['AuthorizationSucceeded'] ?
            user_ad = (self.schedd.queryUserAds(constraint=f'User=="{user}"') or [{}])[0]  # fmt: skip
            # ^^ TODO We can't do this query if we don't have READ.
            if user_ad.get("Enabled", True):
                text.append(
                    "You can place, remove, edit, hold, release, and otherwise manipulate jobs with your current token."
                )
            else:
                text.append(
                    "You can remove, edit, hold, release, and otherwise manipulate existing jobs with your current token."
                )
                text.append(
                    "However, you CANNOT place new jobs, and your existing jobs will not start."
                )
        except htcondor2.HTCondorException as err:
            if "Failed to start command" in str(err):
                have_read = False
                text.append(
                    "You CANNOT place, remove, edit, hold, release, or otherwise manipulate jobs with your current token."
                )
            else:
                raise
        project = ad.get("AuthTokenProject")
        user = ad.get("MyRemoteUserName")
        if user:
            text.append(f"Your AP User ID is '{user}'.")
        else:
            text.append(
                "ERROR: Your AP User ID is unknown."
            )  # XXX how can this happen?
        if project:
            text.append(f"Your currently selected project is '{project}'.")
        else:
            text.append("WARNING: Your currently selected project is unknown.")

        print("\n".join(text))


class PickleableSubmit(htcondor2.Submit):
    # A pickleable htcondor2.Submit
    def __getstate__(self):
        state = dict(
            input=str(self),
            submitMethod=self.getSubmitMethod(),
        )
        return state

    def __setstate__(self, state):
        super().__init__(state["input"])
        self.setSubmitMethod(state["submitMethod"], allow_reserved_values=True)


def load_job_description(submit_file: t.Union[str, os.PathLike]):
    """
    Reads the job description from the given submit file path.
    """
    file_path = pathlib.Path(submit_file)
    return PickleableSubmit(file_path.read_text())

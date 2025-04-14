import datetime
import os
import pathlib
import shutil
import sys
import time

import dateutil
import htcondor2
import ipywidgets as widgets
from IPython.display import Javascript, display


TOKEN_FILENAME = "Placement.token"
COOKIE_TOKEN_FILENAME = "ap-placement-cookie.tkn"


def ensure_tokens_dir() -> pathlib.Path:
    """
    Ensure that the directory condor looks for tokens in exists and has the
    right permissions.

    Returns:
        the pathlib.Path of the tokens directory
    """
    condor_tokens_dir = pathlib.Path.home() / ".condor/tokens.d"
    # need two steps here: mkdir(mode=0o700, ...) does nothing if the dir already exists
    condor_tokens_dir.mkdir(parents=True, exist_ok=True)
    condor_tokens_dir.chmod(0o700)
    return condor_tokens_dir


def write_token(token_filename: str, token_contents: bytes):
    """
    Write the given bytes to a token file in the condor tokens dir.

    token_filename: The name of the file (without directory) to create,
        under the tokens directory.  (Should end in '.tkn')

    token_contents: The bytes to write into the token file.
    """
    token_dest = ensure_tokens_dir() / token_filename
    with open(token_dest, mode="wb") as fh:
        token_dest.chmod(0o600)
        fh.write(token_contents)


JS_FUNCTION_GETCOOKIE = """
function getCookie(name) {
    let nameEquals = name + '=';
    if (document.cookie && document.cookie !== '') {
        const cookies = document.cookie.split(';');
        for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].trim();
            if (cookie.substring(0, nameEquals.length) === nameEquals) {
                return decodeURIComponent(cookie.substring(nameEquals.length));
            }
        }
    }
    return null;
}
"""
JS_PUT_TOKEN = """
var placement_token = getCookie("placement_token");
var the_request = {
  method: 'PUT',
  body: JSON.stringify({
    content: btoa(placement_token),
    format: 'base64',
    type: 'file'
  }),
  headers: {
    'Content-Type': 'application/json',
    'X-XSRFToken': getCookie("_xsrf")
  }
};
fetch(`/api/contents/%(dest_path)s`, the_request).then((response) => {
    if (!response.ok) {
        window.alert("Uploading token cookie failed");
    }
});
"""


def install_token_file(source_path, token_filename):
    # type: (str|os.PathLike, str) -> None
    """
    Moves an existing token file into the condor tokens directory and gives it the correct permissions.

    Arguments:
        source_path: The location of the existing token file
        token_filename: The basename of the destination token file
    """
    token_dest = ensure_tokens_dir() / token_filename
    shutil.move(source_path, token_dest)
    token_dest.chmod(0o700)
    os.chown(token_dest, os.getuid(), os.getgid())


def extract_cookie_and_put(token_path: str) -> None:
    """
    Add Javascript to the page to extract the placement_token cookie
    out of the browser and upload it to the Jupyter server at the given path.

    Arguments:
        token_path: The path the token will be uploaded to, which must be
            relative to the home directory and not contain any hidden
            components (dirs starting with ".").
    """
    # These must be in one call for them to be in each others' scope.
    # (Alternatively, I could add properties to a global such as `document`
    #  but that seems like it pollutes the namespace.)
    display(
        Javascript(JS_FUNCTION_GETCOOKIE + (JS_PUT_TOKEN % {"dest_path": token_path}))
    )


def upload_cookie_token(max_wait_secs=10.0) -> bool:
    """
    Put the token from the cookie named `placement_token` into the condor
    tokens directory.

    Arguments:
        max_wait_secs: The max number of seconds to wait for the token to
            appear before giving up and returning failure.

    Returns
        True on success, False on failure
    """
    upload_filename = f"tmp-upload-{time.time()}.tkn"
    extract_cookie_and_put(upload_filename)
    cookie_token_path = pathlib.Path.home() / upload_filename
    elapsed_secs = 0.0
    while not cookie_token_path.exists():
        time.sleep(0.1)
        elapsed_secs += 0.1
        if elapsed_secs > max_wait_secs:
            print("Token upload failed", file=sys.stderr)
            return False
    install_token_file(cookie_token_path, COOKIE_TOKEN_FILENAME)
    return True


def token_stat(token_filename: str):
    """
    Calls stat() on the token file and returns the results.  If there is
    an error (e.g., the file does not exist), returns None.
    """
    condor_tokens_dir = pathlib.Path.home() / ".condor/tokens.d"
    condor_tokens_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    token_dest = ensure_tokens_dir() / token_filename
    try:
        return token_dest.stat()
    except OSError:
        return None


class Widgets:
    def __init__(self):
        maybe_tz = os.environ.get("TIMEZONE")
        if maybe_tz:
            self.tz = dateutil.tz.gettz(maybe_tz)
        else:
            self.tz = dateutil.tz.gettz()

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
    wid = Widgets()
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

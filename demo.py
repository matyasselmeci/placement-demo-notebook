import datetime
import os
import pathlib

import dateutil
import htcondor2
import ipywidgets as widgets
from IPython.display import display


def write_token(token_filename: str, token_contents: bytes):
    """
    Write the given bytes to a token file in the condor tokens dir.

    token_filename: The name of the file (without directory) to create,
        under the tokens directory.  (Should end in '.tkn')

    token_contents: The bytes to write into the token file.
    """
    condor_tokens_dir = pathlib.Path.home() / ".condor/tokens.d"
    condor_tokens_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    token_dest = condor_tokens_dir / token_filename
    with open(token_dest, mode="wb") as fh:
        token_dest.chmod(0o600)
        fh.write(token_contents)


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
            accept=".tkn",
            description="Click to select a token to upload",
            layout=items_layout,
        )
        self.token_widget.observe(self.on_token_upload, names="value")
        self.token_label_widget = widgets.Label(
            "Please upload a token", layout=items_layout
        )
        self.token_box = widgets.Box(
            [self.token_label_widget, self.token_widget], layout=box_layout
        )

    def on_token_upload(self, change: dict):
        """
        Event handler for uploading a token
        """
        _ = change
        value = self.token_widget.value
        if value:  # we have data
            # Write out the uploaded data to a file
            write_token("ap-placement-upload.tkn", value[0].content.tobytes())
            # Clear the value (so saving the widget state won't embed the token)
            # in the notebook.  (Can't use the 'value' local var here, we need
            # to overwrite what's in the widget).
            self.token_widget.value = ()

            # Set the label to show the last successful upload time.
            now = datetime.datetime.now(tz=self.tz)
            self.token_label_widget.value = f"Upload successful at {now:%H:%M}"

    def display_widgets(self):
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
        if num_in_status:
            print(f"{num_in_status} job(s) are in {name} state.")

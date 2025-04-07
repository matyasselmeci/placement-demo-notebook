import datetime
import pathlib

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
        # Create the 'upload' button for uploading the token;
        # call on_token_upload when "value" changes, i.e., a file is uploaded.
        self.token_widget = widgets.FileUpload(
            accept=".tkn", description="Click to select a token to upload"
        )
        self.token_widget.observe(self.on_token_upload, names="value")
        self.token_label_widget = widgets.Label("Please upload a token")

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
            now = datetime.datetime.now()
            self.token_label_widget.value = f"Upload successful at {now:%H:%M}"

    def display_widgets(self):
        display(self.token_widget)
        display(self.token_label_widget)


def setup():
    """
    Set up the widgets in the demo notebook.
    """
    wid = Widgets()
    wid.display_widgets()

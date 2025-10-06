"""
Widgets for getting tokens in various ways
"""

import datetime
import html
import time

import ipywidgets as widgets
from IPython.display import display

from demo.common import (
    DEVICE_CLIENT_ID,
    TOKEN_FILENAME,
    WEBAPP_SERVER,
    get_timezone,
    token_stat,
    write_token,
)
from demo.device import DeviceClient, DeviceClientError


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


class DeviceWidgets:
    def __init__(self):
        self.tz = get_timezone()

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


def setup():
    """
    Set up the widgets in the demo notebook.
    """
    wid = DeviceWidgets()
    wid.display_widgets()

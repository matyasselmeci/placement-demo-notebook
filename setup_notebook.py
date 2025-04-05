import ipywidgets as widgets
from IPython.display import display

def on_upload(change):
    print("uploaded")
    if token_widget.value:
        with open("ap-placement-upload.tkn", mode="wb") as fh:
            fh.write(token_widget.value[0].content.tobytes())
        token_widget.value = ()
        token_label_widget.value = "Upload successful"

token_widget = widgets.FileUpload(accept='.tkn')
token_widget.observe(on_upload, names='value')
token_label_widget = widgets.Label("Please upload a token")

def display_widgets():
    display(token_widget)
    display(token_label_widget)


import base64
import enum
import json
import logging
import os
import pathlib
import time

import dateutil

_log = logging.getLogger(__name__)


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


def get_token_state(
    token_filename: str,
) -> TokenState:
    """
    Return whether the token is expired, missing, unreadable, or OK
    """
    token_path = pathlib.Path.home() / ".condor/tokens.d" / token_filename
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


def get_timezone():
    maybe_tz = os.environ.get("TIMEZONE", os.environ.get("TZ"))
    if maybe_tz:
        return dateutil.tz.gettz(maybe_tz)  # type: ignore
    else:
        return dateutil.tz.gettz()  # type: ignore


TOKEN_FILENAME = "Placement.token"
DEVICE_CLIENT_ID = os.environ.get("DEVICE_CLIENT_ID") or "placement_demo_notebook"
WEBAPP_SERVER = os.environ.get("PLACEMENT_WEBAPP_LINK") or "http://localhost:5000"

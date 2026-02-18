"""
Interactively get a token using the device flow.
"""

import datetime
# import time
import typing as t

from demo import common  # , device
from demo.device import DeviceClient, DeviceClientError


def init_default_deviceclient() -> DeviceClient:
    """
    Create a DeviceClient with default settings.
    """
    # TODO This should take config from elsewhere (Condor config?)
    return DeviceClient(
        webapp_server=common.WEBAPP_SERVER,
        client_id=common.DEVICE_CLIENT_ID,
    )


def request_token_and_return(dc: t.Optional[DeviceClient] = None) -> t.Optional[bytes]:
    """
    Requests a token interactively and returns it as bytes.

    Arguments:
        dc:
            A DeviceClient used for making and tracking state
            of the request.

    Returns:
        The token as bytes or None.
    """
    tz = common.get_timezone()
    if not dc:
        dc = init_default_deviceclient()

    try:
        dc.make_request()
    except DeviceClientError as err:
        print(f"Request failed: {err}")
        return None

    expires_at_dt = datetime.datetime.fromtimestamp(dc.expires_at, tz)

    print(
        f"Token requested; please go to\n\n\t{dc.verification_uri_complete}\n\n"
        f'and use the code "{dc.user_code}".\n'
        f"The code will expire at {expires_at_dt.strftime(r"%Y-%m-%d %H:%M:%S")}."
    )
    try:
        access_token_b = dc.poll_for_token_loop()
    except DeviceClientError as err:
        print(f"Request failed: {err}")
        return None

    print("Request successful!")
    return access_token_b


def request_token(
    dc: t.Optional[DeviceClient] = None, token_filename: str = common.TOKEN_FILENAME
) -> bool:
    """
    Request a token and install it into the user tokens directory.

    Args:
        dc:
            The DeviceClient used for the token request and state.
            A default one will be created if not specified.
        token_filename:
            The basename of the token file to write.  May not contain directory
            components.

    Returns:
        bool: True on success, False on failure.
    """
    token_contents = request_token_and_return(dc)
    if not token_contents:
        return False
    try:
        token_path = common.write_token(
            token_filename=token_filename, token_contents=token_contents
        )
        print(f"Token has been installed at {token_path}")
        return True
    except OSError as err:
        print(f"Token failed to install: {err}")
        return False

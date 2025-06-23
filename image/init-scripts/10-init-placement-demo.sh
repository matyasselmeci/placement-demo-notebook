# TODO This script should be part of the Kubernetes application, not here.
#
# This script is meant to be mounted into /image-init.d, to be sourced
# at container startup.
#

# The Kubernetes startup scripts cannot test for the name jovyan because they
# rely on sssd to create that user.
if [ "$(id -u)" = 1000 ]
then
    (
        cd "$HOME"
        git clone -b "${NOTEBOOK_BRANCH:-main}" https://github.com/matyasselmeci/placement-demo-notebook placement_demo
        cd placement_demo &&
        cp \
            demo.py \
            ..
    )
    export PLACEMENT_WEBAPP_LINK
    export DEVICE_CLIENT_ID
fi


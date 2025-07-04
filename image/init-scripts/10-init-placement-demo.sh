# Changes to this script should be copied to the Kubernetes application
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
            test_job.submit \
            ..
    )
    export PLACEMENT_WEBAPP_LINK
    export DEVICE_CLIENT_ID
fi


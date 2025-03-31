# TODO This script should be part of the Kubernetes application, not here.
#
# This script is meant to be mounted into /image-init.d, to be sourced
# at container startup.
#

if [ "$(id -un)" = "jovyan" ]
then
    (
        cd "$HOME"
        git clone https://github.com/matyasselmeci/placement-demo-notebook
        cd placement-demo-notebook &&
        cp \
            demo.ipynb \
            install-token \
            test.sub \
            ..
    )
fi


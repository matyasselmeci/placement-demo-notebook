Overview
========

This notebook is used to run commands against an AP containing a PlacementD
for authorization.  This requires a special build of HTCondor.

Building
========

In the subdirectory named `condor`, place `.deb` files from an Ubuntu or
Debian build of HTCondor, then build from the Dockerfile.
Example build:

```
docker build -t notebook:placementd .
```

Running
=======

Mount the files in `init-scripts/` into `/image-init.d/` in the container.
You must specify the schedd host in `_condor_SCHEDD_HOST` and the pool in
`_condor_CONDOR_HOST` to avoid starting up the notebook's personal Condor.

Example invocation:

```
TAG=notebook:placementd
docker run -i -t --rm -v \
    $PWD/init-scripts/20-clone-notebook.sh:/image-init.d/20-clone-notebook.sh \
    --name pdnotebook --hostname pdnotebook \
    -e _condor_CONDOR_HOST=cm.example.net:9618 \
    -e _condor_SCHEDD_HOST=ap.example.net \
    -p 8888:8888 \
    "$TAG"
```

The notebook will be accessible on port 8888.


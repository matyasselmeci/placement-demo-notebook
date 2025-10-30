FROM hub.opensciencegrid.org/osg-jupyterhub/htc-minimal-notebook:4.1.0

# This image uses a special build of condor so we need to copy and install the packages for those.

ADD --chmod=0644 condor/condor_*.deb \
                 condor/htcondor_*.deb \
                 condor/minicondor_*.deb \
                 condor/minihtcondor_*.deb \
                 condor/pelican*.deb \
                 /condor-debs/

USER root

RUN true \
    # Remove packages that conflict with our build and install the new packages \
    && apt-get remove -y minihtcondor condor-upgrade-checks \
    && apt-get update \
    && apt-get install -y jq \
    && dpkg -i /condor-debs/*.deb \
    # sqlite3 is useful for debugging \
    && apt-get install -y sqlite3 \
    && apt-get clean \
    # pre-create a tokens directory that the notebook will be writing to \
    && install -o jovyan -m 0700 -d /home/jovyan/.condor/tokens.d \
    && chown -R jovyan /home/jovyan/.condor \
    && true

USER $NB_UID:$NB_GID

ADD requirements.txt /tmp/requirements.txt

RUN true \
    # install the extra packages we use in our instance \
    && python3 -m pip install -U --no-cache-dir -r /tmp/requirements.txt \
    # conda installed its own copy of the python bindings; we need to use the one installed from the .deb files above \
    && rm -rf /opt/conda/lib/python3.12/site-packages/htcondor2 \
    && ln -s /usr/lib/python3/dist-packages/htcondor2 /opt/conda/lib/python3.12/site-packages/htcondor2 \
    && true

ADD --chmod=0644 --chown=$NB_UID:$NB_GID \
    # jupyter_app_launcher is an extension we added to allow us to add buttons to the main page \
    # Install its configuration \
    jp_app_launcher_pddemo.yml \
    /home/jovyan/.local/share/jupyter/jupyter_app_launcher/jp_app_launcher_pddemo.yml

# Fix annoying startup warning. TODO: This should probably be put into the base image.
CMD ["start-notebook.py"]

# The placement tokens are IDTOKENS so make sure that is the auth method that's used. (Possibly FS is unnecessary?)
ENV _condor_SEC_CLIENT_AUTHENTICATION_METHODS=FS,IDTOKENS

#
# Default parameters
#
ENV NOTEBOOK_BRANCH=main
ENV DEVICE_CLIENT_ID=placement_demo_notebook
ENV PLACEMENT_WEBAPP_LINK=https://demo-ap.chtc.wisc.edu
ENV TIMEZONE=America/Chicago

FROM hub.opensciencegrid.org/osg-jupyterhub/htc-minimal-notebook:4.1.0

ADD --chmod=0644 condor/condor_*.deb \
                 condor/htcondor_*.deb \
                 condor/minicondor_*.deb \
                 condor/minihtcondor_*.deb \
                 /condor-debs

USER root

RUN true \
    && dpkg -i /condor-debs/*.deb \
    && apt-get update \
    && apt-get install -y sqlite3 \
    && apt-get clean \
    && install -o jovyan -m 0700 -d /home/jovyan/.condor/tokens.d \
    && chown -R jovyan /home/jovyan/.condor \
    && true

USER $NB_UID:$NB_GID

ADD requirements.txt /tmp/requirements.txt

RUN true \
    && python3 -m pip install -U --no-cache-dir -r /tmp/requirements.txt \
    && rm -rf /opt/conda/lib/python3.12/site-packages/htcondor2 \
    && ln -s /usr/lib/python3/dist-packages/htcondor2 /opt/conda/lib/python3.12/site-packages/htcondor2 \
    && true

ADD --chmod=0644 --chown=$NB_UID:$NB_GID \
    jp_app_launcher_pddemo.yml \
    /home/jovyan/.local/share/jupyter/jupyter_app_launcher/jp_app_launcher_pddemo.yml

CMD ["start-notebook.py"]

ENV _condor_SEC_CLIENT_AUTHENTICATION_METHODS=FS,IDTOKENS

#
# Default parameters
#
ENV NOTEBOOK_BRANCH=main
ENV DEVICE_CLIENT_ID=placement_demo_notebook
ENV PLACEMENT_WEBAPP_LINK=https://demo-ap.chtc.wisc.edu
ENV TIMEZONE=America/Chicago

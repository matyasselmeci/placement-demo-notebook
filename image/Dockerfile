FROM hub.opensciencegrid.org/osg-jupyterhub/htc-minimal-notebook:4.1.0

ADD --chmod=0644 condor/condor_*.deb \
                 condor/htcondor_*.deb \
                 condor/minicondor_*.deb \
                 condor/minihtcondor_*.deb \
                 /condor-debs

USER root

RUN true \
    && dpkg -i /condor-debs/*.deb \
    && install -o jovyan -m 0700 -d /home/jovyan/.condor/tokens.d \
    && chown -R jovyan /home/jovyan/.condor \
    && true

USER $NB_UID:$NB_GID

ENV _condor_SEC_CLIENT_AUTHENTICATION_METHODS=FS,IDTOKENS


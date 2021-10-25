# #FROM rocker/verse:latest
# FROM quay.io/cdis/jupyter-6.4.0-with-base-packages:latest
# USER root

# # suppress warnings due to non-interactive front end
# ENV DEBIAN_FRONTEND="noninteractive"

# # debian source list
# #RUN echo "deb http://deb.debian.org/debian/ testing main" >> /etc/apt/sources.list
# RUN apt-get update \
#     && apt-get install -y --no-install-recommends apt-utils \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/


# ##### install clang ############################################################
# # debian clang package: https://clang.debian.net/
# # the hard way: https://packages.debian.org/stretch/clang
# RUN apt-get update \
#     && apt-get install -y --no-install-recommends clang-7 \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/

# # create soft/symbolic link
# RUN ln -s /usr/bin/clang++-7 /usr/bin/clang++ \
#     && ln -s /usr/bin/clang-7 /usr/bin/clang


# ##### install PyMC3 ############################################################
# # download and install from source
# WORKDIR /tmp
# RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
#     && bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/miniconda3 \
#     && chmod 770 -R /opt/miniconda3 \
#     && chgrp -R staff /opt/miniconda3 \
#     && ln -s /opt/miniconda3/bin/conda /usr/local/bin/conda \
#     && ln -s /opt/miniconda3/bin/jupyter /usr/local/bin/jupyter
# ENV PATH=/opt/miniconda3/bin:${PATH}

# # install relevant packages
# RUN conda update --yes conda \
#     && conda install --quiet --yes --channel conda-forge ipython jupyterlab pymc3 seaborn \
#     && rm /opt/miniconda3/pkgs/cache/*

# # reset working directory
# WORKDIR /

# ##### cleanup ##################################################################
# # clean up the tmp folder
# RUN rm -rf /tmp/*

FROM jupyter/minimal-notebook

COPY environment.yml /tmp/environment.yml

RUN conda env update -f /tmp/environment.yml && \
    conda clean --all --yes && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

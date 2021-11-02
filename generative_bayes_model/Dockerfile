FROM quay.io/cdis/jupyter_minimal-notebook:2021-10-25

RUN conda update --yes conda && \
    conda install --quiet --yes ipython numpy pandas matplotlib scipy scikit-learn jupyterlab seaborn && \
    conda install --yes -c conda-forge arviz=0.11.0 pymc3 mkl-service && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

COPY [--chown=jovyan:users] . /gbm
RUN chmod -R 777 /gbm
WORKDIR /gbm
CMD [ "python3", "pymc3_generative_model.py" ]

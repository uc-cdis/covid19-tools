FROM quay.io/cdis/jupyter_minimal-notebook:2021-10-25

RUN conda update --yes conda && \
    conda install --quiet --yes ipython numpy pandas matplotlib scipy scikit-learn jupyterlab seaborn && \
    conda install --yes -c conda-forge arviz=0.11.0 pymc3 mkl-service && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER
RUN mkdir $HOME/gbm
COPY . $HOME/gbm/
WORKDIR $HOME/gbm
USER root
CMD [ "bash" , "gbm-run-with-slack.sh" ]

FROM quay.io/cdis/jupyter_minimal-notebook:2021-10-25

RUN conda update --yes conda && \
    conda install --quiet --yes ipython=7.29.0 numpy=1.21.4 pandas=1.3.4 matplotlib=3.4.3 scipy=1.7.2 scikit-learn=1.0.1 jupyterlab=3.2.3 seaborn=0.11.2 && \
    conda install --yes -c conda-forge arviz=0.5.1 pymc3=3.8 mkl-service=2.4.0 && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER
RUN pip3 install awscli==1.18.*
RUN mkdir $HOME/gbm
COPY . $HOME/gbm/
WORKDIR $HOME/gbm
USER root
CMD [ "bash" , "gbm-run-with-slack.sh" ]

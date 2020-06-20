FROM python:3.8-slim-buster

# install R dependencies
# see: https://github.com/ImperialCollegeLondon/covid19model/blob/v6.0/docker/Dockerfile

# TODO @mattgarvin1:
# very well may be that we don't need most of this
# but just want to get the thing running first
# and later can strip down and minimize the image

RUN apt-get update

RUN apt-get install -y \
	fonts-open-sans \
	fonts-arkpandora \
	fonts-adf-verana \
	gnupg2

RUN echo "deb http://cloud.r-project.org/bin/linux/debian buster-cran40/" | tee -a /etc/apt/sources.list && \
    apt-key adv --keyserver keys.gnupg.net --recv-key 'E19F5F87128899B192B1A2C2AD5F960A256A04AF'
RUN apt-get update && \
	apt-get install -y -t buster-cran40 r-base

RUN Rscript -e "install.packages('rstan', dependencies=TRUE)"
RUN Rscript -e "install.packages('lubridate', dependencies=TRUE)"
RUN Rscript -e "install.packages('dplyr', dependencies=TRUE)"
RUN Rscript -e "install.packages('matrixstats', dependencies=TRUE)"
RUN Rscript -e "install.packages('scales', dependencies=TRUE)"
RUN Rscript -e "install.packages('gridextra', dependencies=TRUE)"
RUN Rscript -e "install.packages('bayesplot', dependencies=TRUE)"
RUN Rscript -e "install.packages('tidyr', dependencies=TRUE)"
RUN Rscript -e "install.packages('gdata', dependencies=TRUE)"
RUN Rscript -e "install.packages('svglite', dependencies=TRUE)"
RUN Rscript -e "install.packages('jsonlite', dependencies=TRUE)"
RUN Rscript -e "install.packages('optparse', dependencies=TRUE)"
RUN Rscript -e "install.packages('nortest', dependencies=TRUE)"
RUN Rscript -e "install.packages('pbkrtest', dependencies=TRUE)"
RUN Rscript -e "install.packages('ggpubr', dependencies=TRUE)"
RUN Rscript -e "install.packages('cowplot', dependencies=TRUE)"
RUN Rscript -e "install.packages('EnvStats', dependencies=TRUE)"
RUN Rscript -e "install.packages('BH', dependencies=TRUE)"
RUN Rscript -e "install.packages('ggplot2', dependencies=TRUE)"
RUN Rscript -e "install.packages('isoband', dependencies=TRUE)"

RUN rm -rf /tmp/downloaded_packages/ /tmp/*.rds

RUN Rscript -e "update.packages(.libPaths()[1])"

# install Python dependencies
RUN pip3 install --upgrade pip==20.0.*
RUN pip3 install awscli==1.18.*
WORKDIR /nb-etl

COPY ./nb-etl-requirements.txt /nb-etl/
RUN pip3 install -r nb-etl-requirements.txt

# copy R bayes-by-county simulation files
COPY ./bayes-by-county/ /nb-etl/bayes-by-county/

# copy Python notebooks
COPY ./seir-forecast/seir-forecast.ipynb /nb-etl/
COPY ./generate_top10_plots.py /nb-etl/

COPY ./nb-etl-run.sh /nb-etl/
CMD [ "bash", "/nb-etl/nb-etl-run.sh" ]

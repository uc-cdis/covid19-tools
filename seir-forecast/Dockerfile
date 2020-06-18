FROM python:3.8-slim-buster

#############
## R stuff ##
## see: https://github.com/ImperialCollegeLondon/covid19model/blob/v6.0/docker/Dockerfile

## very well may be that we don't need most of this
## but just want to get the thing running first
## and later can strip down and minimize the image

RUN apt-get update 
#RUN apt-get install -y build-essential libxml2-dev libcurl4-openssl-dev libssl-dev
RUN r-base
RUN apt-get install -y \
	r-cran-rstan \
	r-cran-lubridate r-cran-dplyr r-cran-matrixstats r-cran-scales r-cran-gridextra \
	r-cran-bayesplot r-cran-ggplot2 r-cran-tidyr r-cran-gdata \
	r-cran-svglite r-cran-jsonlite r-cran-optparse \
	fonts-open-sans \
	fonts-arkpandora \
	fonts-adf-verana

RUN apt-get install -y r-cran-ggpubr
RUN apt-get install -y r-cran-nortest
RUN apt-get install -y r-cran-cowplot
RUN apt-get install -y r-cran-bh

RUN Rscript -e "update.packages(.libPaths()[1])"

RUN install2.r --error --deps FALSE \
	EnvStats BH ggplot2 isoband \
	&& rm -rf /tmp/downloaded_packages/ /tmp/*.rds

WORKDIR /covid19-notebooks/bayes-by-county/

# is this redundant?
COPY modelInput modelInput/
COPY stan stan/
COPY r r/
COPY py py/
COPY run.sh run.sh

# probably not necessary
WORKDIR /

########
########

RUN pip3 install --upgrade pip==20.0.*
RUN pip3 install awscli==1.18.*
WORKDIR /seir-forecast

COPY ./requirements.txt /seir-forecast/requirements.txt
RUN pip3 install -r requirements.txt

COPY ./seir-forecast.ipynb /seir-forecast/seir-forecast.ipynb
COPY ./run_seir_forecast.sh /seir-forecast/run_seir_forecast.sh
COPY ./generate_plots.py /seir-forecast/generate_plots.py

CMD [ "bash", "/seir-forecast/run_seir_forecast.sh" ]

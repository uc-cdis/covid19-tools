FROM python:3.8-slim-buster

# install R dependencies
# see: https://github.com/ImperialCollegeLondon/covid19model/blob/v6.0/docker/Dockerfile

# TODO @mattgarvin1:
# very well may be that we don't need most of this
# but just want to get the thing running first
# and later can strip down and minimize the image

RUN apt-get install -y 

RUN apt-get update && \
	apt-get install -y build-essential libxml2-dev libcurl4-openssl-dev libssl-dev \
		fonts-open-sans fonts-arkpandora fonts-adf-verana gnupg2

RUN apt-get update && \
	apt-get install -y r-base

# >>>>>>>>> trying this >>>>>>>>>>> #

RUN apt-get install -y \
	r-cran-rstan \
	r-cran-lubridate r-cran-dplyr r-cran-matrixstats r-cran-scales r-cran-gridextra \
	r-cran-bayesplot r-cran-ggplot2 r-cran-tidyr r-cran-gdata \
	r-cran-svglite r-cran-jsonlite r-cran-optparse \
	r-cran-nortest r-cran-bh \
	fonts-open-sans \
	fonts-arkpandora \
	fonts-adf-verana

RUN Rscript -e "install.packages('cowplot', dependencies=FALSE)"
RUN Rscript -e "install.packages('ggpubr', dependencies=FALSE)"

RUN Rscript -e "update.packages(.libPaths()[1])"

RUN Rscript -e "install.packages('ggplot2', dependencies=FALSE)"
RUN Rscript -e "install.packages('EnvStats', dependencies=FALSE)"
RUN Rscript -e "install.packages('BH', dependencies=FALSE)"
RUN Rscript -e "install.packages('isoband', dependencies=FALSE)"

RUN rm -rf /tmp/downloaded_packages/ /tmp/*.rds

# <<<<<<< trying this <<<<<<<< #

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

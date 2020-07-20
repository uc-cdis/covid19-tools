FROM debian:bullseye

# install R dependencies
RUN apt-get update && \
	apt-get install -y build-essential \
	wget \
	libxml2-dev \
	libcurl4-openssl-dev \
	libssl-dev \
	fonts-open-sans \
	fonts-arkpandora \
	fonts-adf-verana \
	gnupg2 \
	python3 \
	python3-pip \
	r-base \
	r-cran-rstan \
	r-cran-tidyverse \
	r-cran-matrixstats \
	r-cran-scales \
	r-cran-gdata \
	r-cran-gridextra \
	r-cran-bayesplot \
	r-cran-svglite \
	r-cran-optparse \
	r-cran-nortest \
	r-cran-pbkrtest \
	r-cran-rcppeigen \
	r-cran-bh \
	r-cran-ggpubr \
	r-cran-cowplot \
	r-cran-isoband

RUN apt-get update && \
	apt-get install -y libboost-all-dev

RUN Rscript -e "install.packages('EnvStats', dependencies=TRUE)"
RUN Rscript -e "install.packages('BH', dependencies=TRUE)"

# install Python dependencies
RUN pip3 install --upgrade pip==20.1.*
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

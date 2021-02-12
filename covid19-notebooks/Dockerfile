FROM debian:bullseye

# install R dependencies
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
	build-essential \
	wget \
	libboost-all-dev \
	libxml2-dev \
	libcurl4-openssl-dev \
	libssl-dev \
	fonts-open-sans \
	fonts-arkpandora \
	fonts-adf-verana \
	gnupg2 \
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

RUN Rscript -e "install.packages('BH', dependencies=TRUE)"
RUN Rscript -e "install.packages('EnvStats', dependencies=TRUE)"
RUN Rscript -e "install.packages('mlr3verse', dependencies=TRUE)"
RUN Rscript -e "install.packages('ranger', dependencies=TRUE)"
RUN Rscript -e "install.packages('RcppParallel', dependencies=TRUE)"
RUN Rscript -e "install.packages('rstan', dependencies=TRUE)"
RUN Rscript -e "install.packages('StanHeaders', dependencies=TRUE)"
RUN Rscript -e "install.packages('visdat', dependencies=TRUE)"
RUN Rscript -e "install.packages('zoo', dependencies=TRUE)"

WORKDIR /nb-etl

# install Python dependencies
RUN apt-get update && apt-get install -y \
	python3 \
	python3-pip \
	python3-pandas \
	python3-matplotlib \
	python3-numpy \
	python3-scipy

COPY ./nb-etl-requirements.txt /nb-etl/
RUN pip3 install -r nb-etl-requirements.txt
RUN pip3 install awscli==1.18.*

# copy R bayes-by-county simulation files
COPY ./bayes-by-county/ /nb-etl/bayes-by-county/

# copy Python notebooks
COPY ./seir-forecast/seir-forecast.ipynb /nb-etl/
COPY ./generate_top10_plots.py /nb-etl/

COPY ./nb-etl-run.sh /nb-etl/
CMD [ "bash", "/nb-etl/nb-etl-run.sh" ]

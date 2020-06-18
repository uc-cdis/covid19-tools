FROM python:3.8-slim-buster

############
# R stuff ##
# see: https://github.com/ImperialCollegeLondon/covid19model/blob/v6.0/docker/Dockerfile

# very well may be that we don't need most of this
# but just want to get the thing running first
# and later can strip down and minimize the image <- TODO @mattgarvin1

# RUN apt-get update
# RUN apt-get install -y r-base
# RUN apt-get install -y \
# 	r-cran-rstan \
# 	r-cran-lubridate r-cran-dplyr r-cran-matrixstats r-cran-scales r-cran-gridextra \
# 	r-cran-bayesplot r-cran-ggplot2 r-cran-tidyr r-cran-gdata \
# 	r-cran-svglite r-cran-jsonlite r-cran-optparse \
# 	fonts-open-sans \
# 	fonts-arkpandora \
# 	fonts-adf-verana

# RUN Rscript -e "install.packages('ggpubr', dependencies=TRUE)"

# RUN apt-get install -y r-cran-nortest

# RUN Rscript -e "install.packages('cowplot', dependencies=TRUE)"

# RUN apt-get install -y r-cran-bh

# RUN Rscript -e "install.packages(c('EnvStats', 'BH', 'ggplot2', 'isoband'), dependencies=FALSE)"
# RUN rm -rf /tmp/downloaded_packages/ /tmp/*.rds

# RUN Rscript -e "update.packages(.libPaths()[1])"

# COPY /covid19-notebooks/bayes-by-county/modelInput /covid19-notebooks/bayes-by-county/modelInput/
# COPY /covid19-notebooks/bayes-by-county/stan /covid19-notebooks/bayes-by-county/stan/
# COPY /covid19-notebooks/bayes-by-county/r /covid19-notebooks/bayes-by-county/r/
# COPY /covid19-notebooks/bayes-by-county/py /covid19-notebooks/bayes-by-county/py/
# COPY /covid19-notebooks/bayes-by-county/run.sh /covid19-notebooks/bayes-by-county/run.sh

########
########

RUN pip3 install --upgrade pip==20.0.*
RUN pip3 install awscli==1.18.*
WORKDIR /nb-etl

COPY ./nb-etl-requirements.txt /nb-etl/
RUN pip3 install -r nb-etl-requirements.txt

COPY ./seir-forecast/seir-forecast.ipynb /nb-etl/
COPY ./generate_top10_plots.py /nb-etl/
COPY ./nb-etl-run.sh /nb-etl/

CMD [ "bash", "/nb-etl/nb-etl-run.sh" ]

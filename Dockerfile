FROM python:3.8-slim-buster

RUN apt update

# Installing gcloud package (includes gsutil)
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

COPY covid19-etl-requirements.txt ./covid19-etl-requirements.txt
RUN pip3 install --upgrade pip==20.1.*
RUN pip3 install -r covid19-etl-requirements.txt

COPY ./covid19-etl /covid19-etl
WORKDIR /covid19-etl

# output logs while running job
ENV PYTHONUNBUFFERED=1

CMD [ "python3", "/covid19-etl/main.py" ]

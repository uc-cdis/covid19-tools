FROM python:3.8-slim-buster

RUN pip3 install --upgrade pip==20.0.*
RUN pip3 install awscli==1.18.*
WORKDIR /seir-forecast

COPY ./requirements.txt /seir-forecast/requirements.txt
RUN pip3 install -r requirements.txt

COPY ./seir-forecast.ipynb /seir-forecast/seir-forecast.ipynb
COPY ./run_seir_forecast.sh /seir-forecast/run_seir_forecast.sh

CMD [ "bash", "/seir-forecast/run_seir_forecast.sh" ]

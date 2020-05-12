FROM python:3.8-slim-buster

COPY covid19-etl-requirements.txt ./covid19-etl-requirements.txt
RUN pip3 install --upgrade pip==20.1.*
RUN pip3 install -r covid19-etl-requirements.txt

COPY ./covid19-etl /covid19-etl

CMD [ "python3", "/covid19-etl/main.py" ]

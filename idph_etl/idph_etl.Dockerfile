FROM python:3.8-slim-buster

RUN pip3 install --upgrade pip==20.0.*
RUN pip3 install requests==2.23.*

COPY ./idph_etl.py /idph_etl.py

ENTRYPOINT [ "python3", "/idph_etl.py" ]

FROM python:3.8-slim-buster

RUN pip3 install --upgrade pip==20.0.*
RUN pip3 install requests==2.23.*

COPY etl_tools/johns_hopkins_etl.py /johns_hopkins_etl.py

ENTRYPOINT [ "python3", "/johns_hopkins_etl.py" ]

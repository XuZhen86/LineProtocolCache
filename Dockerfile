FROM python:3.8.0

WORKDIR /app
ADD . /app
RUN pip3 install --use-pep517 .

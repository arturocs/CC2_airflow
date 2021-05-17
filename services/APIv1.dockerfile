FROM python:slim

WORKDIR /tmp/p2/services

COPY . /tmp/p2/services

RUN apt-get update && pip install --upgrade pip && pip install --requirement requirements.txt

EXPOSE 8000

CMD gunicorn --bind 0.0.0.0:8000 APIv1:app --workers=1 --capture-output

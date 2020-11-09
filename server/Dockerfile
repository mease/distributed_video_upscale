FROM python:3
WORKDIR ~
RUN apt-get update && apt-get install -y ffmpeg
RUN pip3 install --upgrade pika pymongo jsonpickle requests flask google-cloud-storage
COPY rest-server.py .
CMD ["python3", "rest-server.py"]
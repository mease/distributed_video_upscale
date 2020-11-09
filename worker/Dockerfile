FROM python:3
WORKDIR ~
RUN pip3 install --upgrade pika pymongo jsonpickle google-cloud-storage opencv-python numpy opencv-contrib-python
COPY worker.py .
COPY ESPCN_x4.pb .
CMD ["python3", "worker.py"]
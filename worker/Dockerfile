FROM python:3
WORKDIR ~
RUN apt-get update
RUN apt-get install ffmpeg libsm6 libxext6 -y
RUN pip3 install --upgrade pika pymongo jsonpickle google-cloud-storage opencv-python numpy opencv-contrib-python pillow
COPY worker.py .
COPY ESPCN_x4.pb .
CMD ["python3", "worker.py"]
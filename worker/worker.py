from google.cloud import storage
from pymongo import MongoClient
from PIL import Image
import jsonpickle
import platform
import io
import os
import shutil
import pika
import numpy as np
import time
import sys
import cv2
from cv2 import dnn_superres


hostname = platform.node()

# Log to RabbitMQ
def log(channel, level, message):
    routing_key = '{}.worker.{}'.format(hostname, level)
    print('{} {}'.format(routing_key, message))
    channel.basic_publish(exchange='logs',
                          routing_key=routing_key,
                          body=message)
    
    
def callback(ch, method, properties, body):
    work_data = jsonpickle.decode(body)
    id = work_data['id']
    split_filename = work_data['split_filename']
    log(channel, 'info', 'Received work request for chunk: {}'.format(split_filename))
    start_time = time.time()

    # Get status from DB
    data = db_data.find_one({'id': id})
    if data == None:
        # If id is not in database, ACK here because we don't want an endless worker loop
        ch.basic_ack(delivery_tag = method.delivery_tag)
        raise Exception('No data in database for ID {}'.format(id))

    # Create working directories
    work_path = 'upscale/{}'.format(id)
    os.makedirs(work_path, exist_ok=True)

    # Download chunk
    log(channel, 'debug', 'Downloading chunk: {}'.format(split_filename))
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(split_filename)
    work_file = '{}/{}'.format(work_path, split_filename)
    blob.download_to_filename(work_file)

    # Upscale the chunk
    output_file = '{}_upscaled.mp4'.format(split_filename.split('.')[0])
    output_file_path = '{}/{}'.format(work_path, output_file)
    scale_factor = 4
    model = dnn_superres.DnnSuperResImpl_create()
    model.readModel("ESPCN_x4.pb")
    model.setModel("espcn", scale_factor)
    log(channel, 'debug', 'Upscaling chunk: {}'.format(output_file))
    single_process(work_file, output_file_path, model, scale_factor)

    # Upload upscaled chunk
    log(channel, 'debug', 'Uploading chunk: {}'.format(output_file))
    blob = bucket.blob(output_file)
    blob.upload_from_filename(output_file_path)

    # Clean up working directories
    shutil.rmtree(work_path)

    # Update DB status
    completion_time = time.time()
    db_data.update_one({'id': id, 'splits.name': split_filename},
                       {'$set': {'splits.$.upscale_name': output_file,
                                 'splits.$.time_start': start_time,
                                 'splits.$.time_end': completion_time,
                                 'splits.$.status': 'COMPLETE'}})

    ch.basic_ack(delivery_tag = method.delivery_tag)


def process_video(file_name, output_file_name, model, scale_factor, demo=False):
    # Read video file
    cap = cv2.VideoCapture(file_name)

    # get height, width and frame count of the video
    width, height = (
            int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
            int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        )
    fps = cap.get(cv2.CAP_PROP_FPS)

    # Define the codec and create VideoWriter object
    fourcc = cv2.VideoWriter_fourcc('m', 'p', '4', 'v')
    out = cv2.VideoWriter()
    if demo:
        out.open(output_file_name, fourcc, fps, (width*scale_factor*2, height*scale_factor), True)
        text_bi = (width*scale_factor//2, height*scale_factor-20)
        text_dnn = (width*scale_factor//2 + width*4, height*scale_factor-20)
    else:
        out.open(output_file_name, fourcc, fps, (width*scale_factor, height*scale_factor), True)

    max_frames = 1
    frame_count = 0
    try:
        while cap.isOpened():
            frame_count += 1
            ret, frame = cap.read()
            if not ret:
                break
            im = frame
            upscale = model.upsample(im)
            if demo:
                bilinear = cv2.resize(im, (width*4, height*4))
                combined = cv2.hconcat([bilinear, upscale])
                combined = cv2.putText(combined, 'Bicubic', text_bi, cv2.FONT_HERSHEY_SIMPLEX,
                                       1, (255, 255, 255), 2, cv2.LINE_AA)
                combined = cv2.putText(combined, 'DNN', text_dnn, cv2.FONT_HERSHEY_SIMPLEX,
                                       1, (255, 255, 255), 2, cv2.LINE_AA)
                out.write(combined)
            else:
                out.write(upscale)
    except Exception as e:
        print("Exception!")
        # Release resources
        cap.release()
        out.release()
        raise e
        

    # Release resources
    cap.release()
    out.release()


def single_process(file_name, output_file_name, model, scale_factor, demo=False):
    start_time = time.time()
    process_video(file_name, output_file_name, model, scale_factor, demo=demo)
    end_time = time.time()
    total_processing_time = end_time - start_time
    log(channel, 'info', 'Processed {} in {}'.format(output_file_name, total_processing_time))


mongoHost = os.getenv('MONGO_HOST') or 'localhost'
rabbitMQHost = os.getenv("RABBITMQ_HOST") or "localhost"
bucket_name = os.getenv('STORAGE_BUCKET')
if bucket_name == None:
    raise Exception('Missing env var STORAGE_BUCKET')

print("Connecting to rabbitmq({}) and mongo({})".format(rabbitMQHost, mongoHost))

client = MongoClient(mongoHost)
db = client.upscale
db_data = db.data

params = pika.ConnectionParameters(rabbitMQHost,
                                   heartbeat=600,
                                   blocked_connection_timeout=600)
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='toWorker')

channel.basic_consume(queue='toWorker',
                      on_message_callback=callback)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
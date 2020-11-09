from flask import Flask, request, Response
from google.cloud import storage
from pymongo import MongoClient
import jsonpickle
import platform
import os
import shutil
import pika
import hashlib
import requests
import traceback
import time


hostname = platform.node()
chunk_size = int(os.getenv('CHUNK_SIZE')) or 30
mongoHost = os.getenv('MONGO_HOST') or 'localhost'
rabbitMQHost = os.getenv('RABBITMQ_HOST') or 'localhost'
flaskPort = int(os.getenv('FLASK_PORT') or 5000)
bucket_name = os.getenv('STORAGE_BUCKET')
if bucket_name == None:
    raise Exception('Missing env var STORAGE_BUCKET')

BLOB_TIMEOUT = 600

print("Connecting to rabbitmq({}) and mongo({})".format(rabbitMQHost, mongoHost))

# Initialize the Flask application
app = Flask(__name__)


# Log to RabbitMQ
def log(channel, level, message):
    routing_key = '{}.rest.{}'.format(hostname, level)
    print('{} {}'.format(routing_key, message))
    channel.basic_publish(exchange='logs',
                          routing_key=routing_key,
                          body=message)


@app.route('/video/upscale/', methods=['POST'])
def video_upscale():
    # MQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitMQHost))
    channel = connection.channel()
    
    # Get filename from request
    data = jsonpickle.decode(request.data)
    id = data['id']
    filename = '{}.mp4'.format(id)

    # Create working directories
    work_path = 'upscale/{}'.format(id)
    split_path = '{}/split'.format(work_path)
    os.makedirs(split_path, exist_ok=True)

    # Download the file
    log(channel, 'debug', 'Downloading {}'.format(filename))
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    work_file = '{}/{}'.format(work_path, filename)
    blob.download_to_filename(work_file, timeout=BLOB_TIMEOUT)

    # Split the file
    log(channel, 'debug', 'Splitting {}'.format(filename))
    split_cmd = 'ffmpeg -i {} -acodec copy -f segment -segment_time {} -vcodec copy -reset_timestamps 1 -map 0 -an {}/{}_%08d.mp4'
    split_cmd = split_cmd.format(work_file, chunk_size, split_path, id)
    os.system(split_cmd)
    splits = os.listdir(split_path)

    # Upload splits
    for split_filename in splits:
        log(channel, 'debug', 'Uploading split: {}'.format(split_filename))
        blob = bucket.blob(split_filename)
        blob.upload_from_filename('{}/{}'.format(split_path, split_filename), timeout=BLOB_TIMEOUT)

    # Clean up working directories
    shutil.rmtree(work_path)

    # Update database
    split_data = []
    for split_filename in splits:
        split_info = {'name': split_filename,
                      'status': 'UNPROCESSED',
                      'time_start': -1,
                      'time_end': -1,
                      'upscale_name': 'NONE'}
        split_data.append(split_info)

    data = {'id': id,
            'splits': split_data,
            'status': 'IN_PROGRESS',
            'time_start': time.time(),
            'time_end': -1,
            'upscale_name': 'NONE'}

    doc_id = db_data.insert_one(data).inserted_id
    log(channel, 'debug', 'Updated DB for {}. Doc ID: {}'.format(filename, doc_id))

    # Notify workers
    for split_filename in splits:
        to_worker = {'id': id,
                     'split_filename': split_filename}
        log(channel, 'debug', 'Submitting to worker: {}'.format(split_filename))
        channel.basic_publish(exchange='',
                      routing_key='toWorker',
                      body=jsonpickle.encode(to_worker))

    # Close MQ connection
    connection.close()

    # Return response
    data = db_data.find_one({'id': id})
    response = {'id': id,
                'status': data['status'],
                'upscale_name': data['upscale_name']}
    return Response(response=jsonpickle.encode(response),
                    status=200,
                    mimetype="application/json")


@app.route('/video/query', methods=['GET'])
def video_query():
    # MQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitMQHost))
    channel = connection.channel()
    
    # Get file id
    data = jsonpickle.decode(request.data)
    id = data['id']
    log(channel, 'debug', 'Status query for ID: {}'.format(id))

    # Get status from database
    data = db_data.find_one({'id': id})
    if data == None:
        log(channel, 'debug', 'No status found for ID: {}'.format(id))
        response = {'id': id,
                    'status': 'NONE',
                    'upscale_name': 'NONE'}
    
        # Close MQ connection
        connection.close()

        # Return response
        return Response(response=jsonpickle.encode(response),
                    status=200,
                    mimetype="application/json")

    # Check if status needs updating
    if data['status'] == 'IN_PROGRESS':
        completed = True
        for split in data['splits']:
            if split['status'] != 'COMPLETE':
                completed = False
                break
        if completed:
            # Combine upscaled chunks
            upscale_filename = combine_chunks(id, channel)

            # Get latest worker completion time
            latest_worker_end_time = -1
            for split in data['splits']:
                if split['time_end'] > latest_worker_end_time:
                    latest_worker_end_time = split['time_end']

            # Update DB status
            log(channel, 'debug', 'Updating DB for ID: {}'.format(id))
            db_data.update_one({'id': id},
                               {'$set': {'status': 'COMPLETE',
                                         'time_end': latest_worker_end_time,
                                         'upscale_name': upscale_filename}})

            # Clean up split file in storage
            log(channel, 'debug', 'Cleaning up storage bucket for ID: {}'.format(id))
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob('{}.mp4'.format(id))
            blob.delete()
            for split in data['splits']:
                blob = bucket.blob(split['name'])
                blob.delete()
                blob = bucket.blob(split['upscale_name'])
                blob.delete()

            # Reload from DB
            data = db_data.find_one({'id': id})

    response = {'id': id,
                'status': data['status'],
                'upscale_name': data['upscale_name']}
    
    # Close MQ connection
    connection.close()

    # Return response
    return Response(response=jsonpickle.encode(response),
                    status=200,
                    mimetype="application/json")


def combine_chunks(id, channel):
    # Create working directories
    combine_path = 'upscale/{}/combine'.format(id)
    os.makedirs(combine_path, exist_ok=True)

    # Download original file
    filename = '{}.mp4'.format(id)
    log(channel, 'debug', 'Downloading {}'.format(filename))
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    work_file = '{}/{}'.format(combine_path, filename)
    blob.download_to_filename(work_file, timeout=BLOB_TIMEOUT)

    # Extract audio
    log(channel, 'debug', 'Extracting audio from {}'.format(filename))
    audio_file = '{}/{}.aac'.format(combine_path, id)
    audio_ex = 'ffmpeg -i {} -vn -acodec copy {}'.format(work_file, audio_file)
    os.system(audio_ex)

    # Download all upscaled chunks
    split_names = []
    data = db_data.find_one({'id': id})
    for split in data['splits']:
        split_filename = split['upscale_name']
        split_names.append(split_filename)
        log(channel, 'debug', 'Downloading upscaled chunk {}'.format(split_filename))
        blob = bucket.blob(split_filename)
        split_path = '{}/{}'.format(combine_path, split_filename)
        blob.download_to_filename(split_path, timeout=BLOB_TIMEOUT)
    split_names.sort()

    # Combine chunks
    upscale_noaudio_filename = '{}/{}_upscaled_noaudio.mp4'.format(combine_path, id)
    log(channel, 'debug', 'Combining chunks for {}'.format(upscale_noaudio_filename))
    split_name_file = '{}/splits.txt'.format(combine_path)
    with open(split_name_file, 'w') as f:
        for name in split_names:
            f.write('file \'{}\'\n'.format(name))
    combo = 'ffmpeg -f concat -safe 0 -i {} -c copy {}'
    combo = combo.format(split_name_file, upscale_noaudio_filename)
    os.system(combo)

    # Apply audio
    upscale_filename = '{}_upscaled.mp4'.format(id)
    upscale_filepath = '{}/{}_upscaled.mp4'.format(combine_path, id)
    log(channel, 'debug', 'Applying audio to {}'.format(upscale_filename))
    audio_ap = 'ffmpeg -i {} -i {} -c copy -map 0:v:0 -map 1:a:0 {}'
    audio_ap = audio_ap.format(upscale_noaudio_filename, audio_file, upscale_filepath)
    os.system(audio_ap)

    # Upload to storage
    log(channel, 'debug', 'Uploading upscaled video: {}'.format(upscale_filename))
    blob = bucket.blob(upscale_filename)
    blob.upload_from_filename(upscale_filepath, timeout=BLOB_TIMEOUT)

    # Clean up working directory
    shutil.rmtree(combine_path)

    return upscale_filename


@app.route('/', methods=['GET'])
def hello():
    return '<h1> Video Upscaling </h1><p> Use a valid endpoint </p>'


client = MongoClient(mongoHost)
db = client.upscale
db_data = db.data


# Declare queues/topics
connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitMQHost))
channel = connection.channel()
channel.queue_declare(queue='toWorker')
channel.exchange_declare(exchange='logs', exchange_type='topic')
connection.close()

# Start the Flask application
app.run(host="0.0.0.0", port=flaskPort)

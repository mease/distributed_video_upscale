#!/usr/bin/env python3
# 
#
# Video upscaling application client
#
from google.cloud import storage
import requests
import json
import sys, os
import jsonpickle
import uuid

def do_upscale(addr, filename, bucket_name):
    # Verify MP4
    if filename.split('.')[-1] != 'mp4':
        raise Exception('File must be MP4: {}'.format(filename))

    # Upload file to storage bucket
    id = str(uuid.uuid4())
    bucket_filename = '{}.mp4'.format(id)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(bucket_filename)
    print('Uploading to storage bucket...')
    blob.upload_from_filename(filename)
    print('Upload complete.')

    # Send request
    print('Sending request...')
    headers = {'content-type': 'application/json'}
    upscale_url = addr + '/video/upscale'
    data = jsonpickle.encode({'id': id})
    response = requests.post(upscale_url, data=data, headers=headers)

    # Decode response
    print('Response is', response)
    print(json.loads(response.text))

def do_query(addr, id):
    # Send query request
    upscale_url = addr + '/video/query/' + id
    response = requests.get(upscale_url, timeout=600)

    # Decode response
    print('Response is', response)
    print(json.loads(response.text))

host = sys.argv[1]
cmd = sys.argv[2]

addr = 'http://{}'.format(host)

if cmd == 'upscale':
    filename = sys.argv[3]
    bucket_name = sys.argv[4]
    do_upscale(addr, filename, bucket_name)
elif cmd == 'query':
    id = sys.argv[3]
    do_query(addr, id)
else:
    print("Unknown option", cmd)
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import json
import logging
import os
import sys
import tempfile

from PIL import Image, ImageFilter

from google.cloud import pubsub_v1, storage, vision


project_id = os.environ['PROJECT_ID']

subscription_name = 'safeimage-workers'
bucket_name = '{}-photostore'.format(project_id)
content_types = {'jpg': 'image/jpeg', 'jpeg': 'image/jpeg',
                 'png': 'image/png', 'gif': 'image/gif'}

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    project_id, subscription_name)


def blur_image(filename):
  bucket = storage.Client().get_bucket(bucket_name)
  with tempfile.NamedTemporaryFile() as temp:
    blob = bucket.blob(filename)
    blob.download_to_filename(temp.name)
    im = Image.open(temp.name)
    im = im.filter(ImageFilter.GaussianBlur(16))
    extention = filename.split('.')[-1].lower()
    temp_filename = '{}.{}'.format(temp.name, extention)
    im.save(temp_filename)
    content_type = content_types[extention]
    blob = bucket.blob(filename)
    blob.upload_from_filename(temp_filename, content_type=content_type)
    blob.make_public()


def validate_image(filename):
  vision_client = vision.ImageAnnotatorClient()
  image = vision.Image()
  image.source.image_uri = 'gs://{}/{}'.format(bucket_name, filename)
  response = vision_client.safe_search_detection(image=image)
  safe = response.safe_search_annotation
  print('Detected levels: {}'.format((safe.adult, safe.violence)))
  if safe.adult >= 3 or safe.violence >= 2:
    blur_image(filename)


def callback(message):
  try:
    data = message.data.decode('utf-8')
    attributes = message.attributes
    message.ack()
    if attributes['eventType'] != 'OBJECT_FINALIZE':
      return
    object_metadata = json.loads(data)
    filename = object_metadata['name']
    print('Process file: {}'.format(filename))
    validate_image(filename)
  except Exception as e:
    print('Something wrong happened: {}'.format(e.args))


streaming_pull_future = subscriber.subscribe(
  subscription_path, callback=callback)
print('Waiting for messages on {}'.format(subscription_path))

with subscriber:
  try:
    streaming_pull_future.result()
  except TimeoutError:
    streaming_pull_future.cancel()
    streaming_pull_future.result()

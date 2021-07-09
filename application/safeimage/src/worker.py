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
  logger.info('Blurring an image: {}'.format(filename))
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
    logger.info('Blurred an image: {}'.format(filename))


def validate_image(filename):
  logger = logging.getLogger(__name__)
  vision_client = vision.ImageAnnotatorClient()
  image = vision.Image()
  image.source.image_uri = 'gs://{}/{}'.format(bucket_name, filename)
  logger.info('Detecting levels: {}'.format(filename))
  response = vision_client.safe_search_detection(image=image)
  safe = response.safe_search_annotation
  logger.info('Detected levels for {}: {}'.format(filename, (safe.adult, safe.violence)))
  if safe.adult >= 3 or safe.violence >= 2:
    blur_image(filename)


def callback(message):
  logger = logging.getLogger(__name__)
  try:
    data = message.data.decode('utf-8')
    attributes = message.attributes
    message.ack()
    if attributes['eventType'] != 'OBJECT_FINALIZE' or 'overwroteGeneration' in attributes:
      return
    object_metadata = json.loads(data)
    filename = object_metadata['name']
    logger.info('Processing a file: {}'.format(filename))
    validate_image(filename)
    logger.info('Processed a file: {}'.format(filename))
  except Exception as e:
    logger.error('Something wrong happened: {}'.format(e.args))


def setup_logger():
  logger = logging.getLogger(__name__)
  logger.propagate = False
  stdout_handler = logging.StreamHandler(sys.stdout)
  stdout_handler.setLevel(logging.DEBUG)
  stdout_handler.addFilter(lambda r: r.levelno < logging.WARNING)
  logger.addHandler(stdout_handler)

  stderr_handler = logging.StreamHandler(sys.stderr)
  stderr_handler.setLevel(logging.DEBUG)
  stderr_handler.addFilter(lambda r: r.levelno >= logging.WARNING)
  logger.addHandler(stderr_handler)
  logger.setLevel(logging.DEBUG)


setup_logger()

streaming_pull_future = subscriber.subscribe(
  subscription_path, callback=callback)
logger = logging.getLogger(__name__)
logger.info('Waiting for messages on {}'.format(subscription_path))

with subscriber:
  try:
    streaming_pull_future.result()
  except TimeoutError:
    streaming_pull_future.cancel()
    streaming_pull_future.result()

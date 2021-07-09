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


import os
import tempfile
import uuid

from auth_decorator import requires_auth
from flask import Flask, render_template, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_wtf.file import FileField
from sqlalchemy import desc
from werkzeug.datastructures import CombinedMultiDict
from werkzeug.utils import secure_filename
from wtforms import Form, ValidationError

from google.cloud import storage, pubsub_v1


project_id = os.environ['PROJECT_ID']
dbuser = os.environ['DB_USER']
dbpass = os.environ['DB_PASS']


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = \
    'mysql+pymysql://{}:{}@localhost:3306/photo_db'.format(dbuser, dbpass)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
bucket_name = '{}-photostore'.format(project_id)
bucket = storage.Client().get_bucket(bucket_name)
storage_path = 'https://storage.googleapis.com/{}'.format(bucket_name)


content_types = {'jpg': 'image/jpeg', 'jpeg': 'image/jpeg',
                 'png': 'image/png', 'gif': 'image/gif'}
extensions = sorted(content_types.keys())


@app.before_request
@requires_auth
def before_request():
  pass


class Photo(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  filename = db.Column(db.String(128))
  label = db.Column(db.String(64))
  has_thumbnail = db.Column(db.Boolean)

  def __init__(self, filename):
    self.filename = filename
    self.label = None
    self.has_thumbnail = False


db.create_all()


def publish_message(topic_name, data):
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(project_id, topic_name)
  publisher.publish(topic_path, data.encode('utf-8'))


def is_photo():
  def _is_photo(_, field):
    if not field.data:
      raise ValidationError('No file')
    if field.data and \
       field.data.filename.split('.')[-1].lower() not in extensions:
      raise ValidationError('Invalid file name')
  return _is_photo


class UploadForm(Form):
  input_photo = FileField('Photo file (jpg, jpeg, png, gif)',
                          validators=[is_photo()])


@app.route('/')
def index():
  return render_template('index.html')


def show_photos(form):
  last_photos = Photo.query.order_by(desc(Photo.id)).limit(10)
  last_photos = [photo for photo in last_photos]
  return render_template('photos.html', form=form, storage_path=storage_path,
                         photos=last_photos)


@app.route('/photos')
def photos():
  form = UploadForm(request.form)
  return show_photos(form)


@app.route('/post', methods=['POST'])
def post():
  form = UploadForm(CombinedMultiDict((request.files, request.form)))
  if request.method == 'POST' and form.validate():
    filename = '{}.{}'.format(
        str(uuid.uuid4()),
        secure_filename(form.input_photo.data.filename))
    content_type = content_types[filename.split('.')[-1].lower()]
    with tempfile.NamedTemporaryFile() as temp:
      form.input_photo.data.save(temp.name)
      blob = bucket.blob(filename)
      blob.upload_from_filename(temp.name, content_type=content_type)
      blob.make_public()
    db.session.add(Photo(filename))
    db.session.commit()
    publish_message('thumbnail-service', filename)
  return show_photos(form)


@app.route('/delete', methods=['POST'])
def delete():
  photo_id = list(request.form.keys())[0]
  photo = db.session.query(Photo).filter_by(id=photo_id).first()
  bucket.delete_blobs(
      [photo.filename, 'thumbnails/{}'.format(photo.filename)],
      on_error=lambda _: None)
  db.session.delete(photo)
  db.session.commit()
  return redirect(url_for('photos'))


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=8080, debug=False)

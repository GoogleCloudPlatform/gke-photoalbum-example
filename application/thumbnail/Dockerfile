FROM debian:9.5
LABEL maintainer "Etsuji Nakai"
ENV REFRESHED_AT 2018/10/13

RUN apt-get -qq update; \
    apt-get -qq -y upgrade; \
    apt-get -qq -y install python python-dev python-pip \
        python-flask python-flask-sqlalchemy python-pymysql
RUN pip install --upgrade setuptools pillow \
        google-cloud==0.34.0 google-auth-httplib2==0.0.3 \
        google-cloud-storage==1.13.0 google-cloud-pubsub==0.38.0 \
        google-cloud-vision==0.34.0

ADD src /opt/thumbnail/bin
RUN chmod u+x /opt/thumbnail/bin/worker.py
CMD ["/opt/thumbnail/bin/worker.py"]

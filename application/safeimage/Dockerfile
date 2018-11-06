FROM debian:9.5
LABEL maintainer "Etsuji Nakai"
ENV REFRESHED_AT 2018/10/13

RUN apt-get -qq update; \
    apt-get -qq -y upgrade; \
    apt-get -qq -y install python python-dev python-pip
RUN pip install --upgrade setuptools pillow \
        google-cloud==0.34.0 google-auth-httplib2==0.0.3 \
        google-cloud-storage==1.13.0 google-cloud-pubsub==0.38.0 \
        google-cloud-vision==0.34.0

ADD src /opt/safeimage/bin
RUN chmod u+x /opt/safeimage/bin/worker.py
CMD ["/opt/safeimage/bin/worker.py"]

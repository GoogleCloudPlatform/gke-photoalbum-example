FROM debian:9.5
LABEL maintainer "Etsuji Nakai"
ENV REFRESHED_AT 2018/10/13

RUN apt-get -qq update; \
    apt-get -qq -y upgrade; \
    apt-get -qq -y install python python-dev python-pip \
        python-flask python-requests python-wtforms python-arrow \
        python-flask-sqlalchemy python-pymysql python-flaskext.wtf
RUN pip install --upgrade pip
RUN pip install --upgrade setuptools \
        google-cloud==0.34.0 google-auth-httplib2==0.0.3 \
        google-cloud-storage==1.13.0 google-cloud-pubsub==0.38.0

ADD src /opt/photoalbum/bin
RUN chmod u+x /opt/photoalbum/bin/app.py
EXPOSE 8080
CMD ["/opt/photoalbum/bin/app.py"]

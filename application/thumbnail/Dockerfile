FROM marketplace.gcr.io/google/debian10
LABEL maintainer "Etsuji Nakai"
ENV REFRESHED_AT 2021/07/07

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y python3 python3-dev wget
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3 get-pip.py

RUN pip install --upgrade google-cloud-core==1.7.1 \
	google-cloud-pubsub==2.6.0 google-cloud-storage==1.40.0 \
	google-cloud-vision==2.3.2 Flask==2.0.1 Flask-SQLAlchemy==2.5.1 \
	Pillow==8.3.1 PyMySQL==1.0.2

ADD src /opt/thumbnail/bin
RUN chmod u+x /opt/thumbnail/bin/worker.py
CMD ["/opt/thumbnail/bin/worker.py"]

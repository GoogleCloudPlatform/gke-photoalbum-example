FROM marketplace.gcr.io/google/debian10
LABEL maintainer "Etsuji Nakai"
ENV REFRESHED_AT 2021/07/07

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y python3 python3-dev wget
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3 get-pip.py

RUN pip install --upgrade google-cloud-core==1.7.1 \
	google-cloud-pubsub==2.6.0 google-cloud-storage==1.40.0 \
	google-cloud-vision==2.3.2 Pillow==8.3.1

ADD src /opt/safeimage/bin
RUN chmod u+x /opt/safeimage/bin/worker.py
CMD ["/opt/safeimage/bin/worker.py"]

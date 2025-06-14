ARG BASE_IMAGE=ubuntu:20.04
FROM $BASE_IMAGE

LABEL maintainer="neotech"
ENV LANG C.UTF-8
ENV TZ Asia/Shanghai

RUN apt-get update && \
    apt-get install -y tcsh vim curl python3 python3-pip

ADD ./requirements.txt /opt/workspace/neotech/requirements.txt
WORKDIR /opt/workspace/neotech
RUN pip3 install torch==1.13.1 --index-url https://download.pytorch.org/whl/cpu
RUN pip3 install -r requirements.txt

ADD ./UniPMT.tar.gz //mnt/softwares/
ADD ./unipmt_server.tar.gz /opt/workspace/neotech/
WORKDIR /opt/workspace/neotech/unipmt

EXPOSE 60001

CMD ["sh","./run.sh"]

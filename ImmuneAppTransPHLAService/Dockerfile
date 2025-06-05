ARG BASE_IMAGE=ubuntu:22.04
FROM $BASE_IMAGE

LABEL maintainer="neotech"
ENV LANG=C.UTF-8
ENV TZ=Asia/Shanghai

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.7 python3.7-distutils curl tcsh vim && \
    curl -sS https://bootstrap.pypa.io/pip/3.7/get-pip.py | python3.7 && \
    apt-get clean

RUN ln -sf /usr/bin/python3.7 /usr/bin/python && \
    ln -sf /usr/local/bin/pip /usr/bin/pip

ADD ./immune_transphla_software_20250507.tar.gz /opt/
ADD ./LinearDesign.tar.gz /mnt/softwares/

COPY ./requirements.txt /opt/workspace/neotech/requirements.txt
WORKDIR /opt/workspace/neotech
RUN pip install -r requirements.txt

COPY ./immune_transphla_service /opt/workspace/neotech/immune_transphla_service
WORKDIR /opt/workspace/neotech/immune_transphla_service

EXPOSE 60002

CMD ["sh","./run.sh"]

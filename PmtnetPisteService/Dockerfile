ARG BASE_IMAGE=ubuntu:20.04
FROM $BASE_IMAGE

LABEL maintainer="neotech"
ENV LANG=C.UTF-8
ENV TZ=Asia/Shanghai

# 1. 安装 Python 3.7 和基础工具
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        software-properties-common \
        ca-certificates \
        && \
    add-apt-repository -y universe && \
    add-apt-repository -y multiverse && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        tcsh \
        vim \
        curl \
        python3.7 \
        python3.7-dev \
        python3.7-distutils \
    && \
    curl -sSL https://bootstrap.pypa.io/pip/3.7/get-pip.py -o get-pip.py && \
    python3.7 get-pip.py && \
    ln -sf /usr/bin/python3.7 /usr/bin/python && \
    rm get-pip.py && \
    rm -rf /var/lib/apt/lists/*

# 2. 设置工作目录并添加文件
ADD ./piste_pmtnet_softwares.tar.gz /opt/
ADD ./requirements.txt /opt/workspace/neotech/
WORKDIR /opt/workspace/neotech

# 3. 安装依赖（分步安装确保关键依赖）
RUN pip install --upgrade "pip<21.0" "setuptools<45.0.0" && \
    pip install numpy==1.19.2

RUN pip install torch==1.10.2 torchvision==0.11.3 torchaudio==0.10.2

RUN pip install "tensorflow>=1.5.0,<=1.15.0"

RUN pip install -r requirements.txt

# 4. 复制应用代码
ADD ./piste_pmtnet_server /opt/workspace/neotech/piste_pmtnet_server
WORKDIR /opt/workspace/neotech/piste_pmtnet_server

EXPOSE 60003

CMD ["sh", "./run.sh"]
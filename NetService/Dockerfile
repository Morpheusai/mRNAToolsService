ARG BASE_IMAGE=ubuntu:22.04
FROM $BASE_IMAGE

LABEL maintainer="neotech"
ENV LANG C.UTF-8
ENV TZ Asia/Shanghai

RUN apt-get update && \
    apt-get install -y tcsh vim curl python3 python3-pip sudo \
    gawk build-essential libgomp1 && \
    rm -rf /var/lib/apt/lists/*

# 安装ViennaRNA-2.7.0
RUN mkdir -p /opt/softwares/ViennaRNA && \
    cd /opt/softwares && \
    curl -L https://www.tbi.univie.ac.at/RNA/download/sourcecode/2_7_x/ViennaRNA-2.7.0.tar.gz -o ViennaRNA-2.7.0.tar.gz && \
    tar -zxvf ViennaRNA-2.7.0.tar.gz && \
    cd ViennaRNA-2.7.0 && \
    ./configure --prefix=/opt/softwares/ViennaRNA && \
    make && \
    make install && \
    rm -rf /opt/softwares/ViennaRNA-2.7.0.tar.gz /opt/softwares/ViennaRNA-2.7.0

# 设置ViennaRNA环境变量
ENV PATH="/opt/softwares/ViennaRNA/bin:${PATH}"
ENV MANPATH="/opt/softwares/ViennaRNA/share/man:${MANPATH}"
ENV LD_LIBRARY_PATH="/opt/softwares/ViennaRNA/lib:${LD_LIBRARY_PATH}"

ADD ./net_big_pri_softwares.tar.gz /opt/

# 安装MixMHCpred3.0
RUN cd /opt/softwares/MixMHCpred && \
    chmod +x MixMHCpred install_packages && \
    ./install_packages 

# 安装PRIME2.1
RUN cd /opt/softwares/PRIME/lib && \
    g++ -O3 PRIME.cc -o PRIME.x

# 设置全局路径
ENV PATH="/opt/softwares/MixMHCpred:/opt/softwares/PRIME:${PATH}"

ADD ./requirements.txt /opt/workspace/neotech/requirements.txt
WORKDIR /opt/workspace/neotech
RUN pip install -r requirements.txt

ADD ./pmhc /opt/workspace/neotech/pmhc
WORKDIR /opt/workspace/neotech/pmhc

EXPOSE 60001

CMD ["sh","./run.sh"]
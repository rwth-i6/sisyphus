FROM ubuntu:18.04

# Install dependencies
RUN apt-get update && apt-get -y install python3-pip
RUN apt-get update && apt-get -y install git
RUN apt-get update && apt-get -y install linux-tools-aws
RUN apt-get update && apt-get -y install pypy
RUN apt-get update && apt-get -y install apt-utils
RUN apt-get update && apt-get -y install nfs-common

RUN cd opt && git clone https://github.com/rwth-i6/sisyphus.git
RUN cp -a /opt/sisyphus/sisyphus /usr/local/lib/python3.6/dist-packages/
RUN cp -a /opt/sisyphus/sis /usr/local/bin/

RUN pip3 install ipython flask fusepy Sphinx
RUN pip3 install psutil

RUN mkdir /efs
COPY startup.sh /root/startup.sh
ENTRYPOINT ["bash", "/root/startup.sh"]

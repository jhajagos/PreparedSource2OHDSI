FROM ubuntu:noble

RUN apt update
RUN apt install default-jdk -y

# Setup Python Environment for running PySpark
RUN apt install wget -y
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN chmod u+x Miniconda3-latest-Linux-x86_64.sh

RUN ./Miniconda3-latest-Linux-x86_64.sh -b

RUN /root/miniconda3/bin/conda create --name PySpark python=3.9 pandas

RUN /root/miniconda3/bin/conda init && bash ~/.bashrc && . ~/.bashrc

ENV conda /root/miniconda3/bin/conda
ENV bashrc /root/.bashrc

RUN $conda init && . $bashrc && conda activate PySpark && pip install pyspark

RUN mkdir /root/synthea/
RUN cd /root/synthea/ && wget https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar

RUN mkdir /root/synthea/modules/
RUN mkdir /root/scripts/

RUN mkdir /root/github/

RUN apt install git -y

RUN cd ~/github/ && git clone  https://github.com/jhajagos/PreparedSource2OHDSI.git

RUN $conda init && . $bashrc && conda activate PySpark && pip install build
RUN $conda init && . $bashrc && conda activate PySpark && cd ~/github/PreparedSource2OHDSI/ && python -m build --wheel
RUN $conda init && . $bashrc && conda activate PySpark && cd ~/github/PreparedSource2OHDSI/dist/ \
    && pip install preparedsource2ohdsi-0.1.1-py3-none-any.whl # Change for new version

RUN mkdir /root/config/

COPY synthea_config.json /root/config/

RUN mkdir -p /data/ohdsi/vocabulary/
RUN mkdir -p /data/preparedsource/synthea/
RUN mkdir /data/ohdsi/output/

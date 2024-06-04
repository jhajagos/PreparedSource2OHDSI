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

# Get Synthea Commandline tool

RUN mkdir /root/synthea/
RUN cd /root/synthea/ && wget https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar

RUN mkdir /root/synthea/modules/


RUN mkdir /root/github/
RUN apt install git -y

RUN cd ~/github/ && git clone  https://github.com/jhajagos/PreparedSource2OHDSI.git

RUN $conda init && . $bashrc && conda activate PySpark && pip install build
RUN $conda init && . $bashrc && conda activate PySpark && cd ~/github/PreparedSource2OHDSI/ && python -m build --wheel
RUN $conda init && . $bashrc && conda activate PySpark && cd ~/github/PreparedSource2OHDSI/dist/ \
    && pip install preparedsource2ohdsi-0.1.1-py3-none-any.whl # Change for new version

RUN mkdir /root/config/

# Add scripts for running the pipeline

COPY synthea_config.json /root/config/

RUN mkdir -p /data/ohdsi/vocabulary/
RUN mkdir -p /data/preparedsource/synthea/
RUN mkdir /data/ohdsi/output/
RUN mkdir /root/scripts/

COPY generate_synthetic_covid19.sh /root/scripts/
RUN chmod u+x /root/scripts/generate_synthetic_covid19.sh

COPY map_synthea_csv_to_prepared_source.sh /root/scripts/
RUN chmod u+x /root/scripts/map_synthea_csv_to_prepared_source.sh

RUN echo '/root/miniconda3/bin/conda init'  >> /root/.bashrc

COPY prepared_source_to_ohdsi_config.json /root/config/

COPY build_vocabulary_files_for_mapping.sh /root/scripts/
RUN chmod u+x /root/scripts/build_vocabulary_files_for_mapping.sh

COPY map_prepared_source_to_ohdsi.sh /root/scripts/
RUN chmod u+x /root/scripts/map_prepared_source_to_ohdsi.sh

COPY basic_mapped_data_stats.py /root/scripts/

RUN $conda init && . $bashrc && conda activate PySpark && conda install ipython

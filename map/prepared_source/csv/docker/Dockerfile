FROM ubuntu:noble

RUN apt update
RUN apt install default-jdk -y

# Setup Python Environment for running PySpark
RUN apt install wget -y
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN chmod u+x Miniconda3-latest-Linux-x86_64.sh

RUN ./Miniconda3-latest-Linux-x86_64.sh -b

# Setup Python

RUN /root/miniconda3/bin/conda create --name PySpark python=3.9 pandas
RUN /root/miniconda3/bin/conda init && bash ~/.bashrc && . ~/.bashrc

ENV conda /root/miniconda3/bin/conda
ENV bashrc /root/.bashrc

RUN $conda init && . $bashrc && conda activate PySpark && pip install pyspark


RUN mkdir /root/github/
RUN apt install git -y

# Install code for the OHDSI Spark based mapper
RUN cd ~/github/ && git clone  https://github.com/jhajagos/PreparedSource2OHDSI.git
RUN $conda init && . $bashrc && conda activate PySpark && pip install build
RUN $conda init && . $bashrc && conda activate PySpark && cd ~/github/PreparedSource2OHDSI/ && python -m build --wheel
RUN $conda init && . $bashrc && conda activate PySpark && cd ~/github/PreparedSource2OHDSI/dist/ \
    && pip install preparedsource2ohdsi-0.1.1-py3-none-any.whl # Change for new version

RUN $conda init && . $bashrc && conda activate PySpark && conda install ipython

RUN mkdir /root/config/

# Add directories which can be used as mount points
RUN mkdir -p /data/ohdsi/vocabulary/
RUN mkdir -p /data/prepared_source/csv/
RUN mkdir /data/ohdsi/output/
RUN mkdir /root/scripts/


RUN cd /root/scripts/ && ln -s /root/github/PreparedSource2OHDSI/map/ohdsi/utilities/basic_mapped_data_stats.py ./
RUN cd /root/scripts/ && ln -s /root/github/PreparedSource2OHDSI/map/ohdsi/utilities/jdbc_sql_loader.py ./
RUN cd /root/scripts/ && ln -s /root/github/PreparedSource2OHDSI/map/ohdsi/utilities/db_configure.py ./

RUN echo '/root/miniconda3/bin/conda init'  >> /root/.bashrc

COPY prepared_source_to_ohdsi_config.json /root/config/

COPY build_vocabulary_files_for_mapping.sh /root/scripts/
RUN chmod u+x /root/scripts/build_vocabulary_files_for_mapping.sh

COPY map_prepared_source_to_ohdsi.sh /root/scripts/
RUN chmod u+x /root/scripts/map_prepared_source_to_ohdsi.sh

RUN mkdir /root/jdbc/ # Hold JDBC drivers for connecting to a database

# Add the ability to run arbitrary SQL RDBMS code from witin the container
RUN $conda init && . $bashrc && conda activate PySpark && pip install JayDeBeApi # Connect through JDBC to databases
RUN $conda init && . $bashrc && conda activate PySpark && pip install sqlparse

# In case you need to execute the CDM
RUN cd /root/github && git clone https://github.com/OHDSI/CommonDataModel.git

COPY prepared_source_csv_to_parquet.sh /root/scripts/
RUN chmod u+x /root/scripts/prepared_source_csv_to_parquet.sh

COPY fully_load_ohdsi_parquet_files_into_rdbms.sh /root/scripts/
RUN chmod u+x /root/scripts/fully_load_ohdsi_parquet_files_into_rdbms.sh

COPY fully_load_ohdsi_parquet_files_excluding_concepts_into_rdbms.sh /root/scripts/
RUN chmod u+x /root/scripts/fully_load_ohdsi_parquet_files_excluding_concepts_into_rdbms.sh

RUN mkdir /root/scripts/mssql/

COPY ./mssql/load_staged_tables_into_ohdsi_rdbms.sh /root/scripts/mssql/
RUN chmod u+x /root/scripts/mssql/load_staged_tables_into_ohdsi_rdbms.sh

COPY ./mssql/load_all_staged_tables_into_ohdsi_rdbms.sh /root/scripts/mssql/
RUN chmod u+x /root/scripts/mssql/load_all_staged_tables_into_ohdsi_rdbms.sh

RUN mkdir /root/scripts/psql/

COPY ./psql/load_staged_tables_into_ohdsi_rdbms.sh /root/scripts/psql/
RUN chmod u+x /root/scripts/psql/load_staged_tables_into_ohdsi_rdbms.sh

COPY ./psql/load_all_staged_tables_into_ohdsi_rdbms.sh /root/scripts/psql/
RUN chmod u+x /root/scripts/psql/load_all_staged_tables_into_ohdsi_rdbms.sh

COPY ./mssql/stage_tables_excluding_concepts_into_rdbms.sh /root/scripts/mssql/
RUN chmod u+x /root/scripts/mssql/stage_tables_excluding_concepts_into_rdbms.sh

COPY ./mssql/stage_tables_into_rdbms.sh /root/scripts/mssql/
RUN chmod u+x /root/scripts/mssql/stage_tables_into_rdbms.sh


COPY ./psql/stage_tables_excluding_concepts_into_rdbms.sh /root/scripts/psql/
RUN chmod u+x /root/scripts/mssql/stage_tables_excluding_concepts_into_rdbms.sh

COPY ./psql/stage_tables_into_rdbms.sh /root/scripts/psql/
RUN chmod u+x /root/scripts/psql/stage_tables_into_rdbms.sh

RUN ln -s /root/scripts/psql/*.sh /root/scripts/



COPY .setup_environment /root/scripts
RUN chmod u+x /root/scripts/.setup_environment

RUN mkdir /data/prepared_source/parquet
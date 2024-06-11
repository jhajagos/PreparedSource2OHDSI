## Building the Docker Image


```bash
docker build -t syntheaohdsi:latest ./ 
```

Once you follow the process of building the vocabularies:

The directory: `/home/user/data/vocabulary/20231114` should contain `bz2` compressed files for each 
vocabulary files.

```bash
cd /home/user/data/vocabulary/20231114
ls
```

```bash ""
docker run --rm -it -v /home/user/data/vocabulary/20231114:/data/ohdsi/vocabulary \
  -v /home/user/data/synthea/covid19:/data/ohdsi/output \
  -v /home/user/jdbc:/root/jdbc syntheaohdsi:latest /bin/bash
```

The following steps assume you are running within the container.


## Preprocessing the vocabulary

You will only need to run this once:

```bash
conda activate PySpark
cd /root/scripts
./build_vocabulary_files_for_mapping.sh
```

## Generating synthetic OHDSI data

You can run multiple files 

## Loading into a relational database

```
python ./db_configure.py -j 'jdbc:sqlserver://sql1:1433;encrypt=false;database=synthea_ohdsi' -u sa -p aZHNjMgL5N
```



```bash
docker build -t csv2ohdsi:latest ./
```

```bash
docker run -it \
  --name ps2ohdsi --hostname ps2ohdsi \
  -v /home/user/data/vocabulary/20231114:/data/ohdsi/vocabulary  \
  -v /home/user/data/synthea/covid19:/data/ohdsi/output \  
  -v /home/user/data/prepared_source/csv:/data/prepared_source/csv \
  -v /home/user/data/prepared_source/parquet:/data/prepared_source/parquet
  ps2csv:latest /bin/bash 
```


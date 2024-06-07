# Building a Docker Image


```bash
docker build -t syntheaohdsi:latest ./ 
```

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


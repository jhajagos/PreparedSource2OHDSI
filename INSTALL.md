# Detailed instructions for running

## Setting up SPARK environment

A Spark environment is required to run the transform 
scripts. Two examples are given using the widely used
Databricks Spark environment or running locally.

### Setting up Databricks development environment

To see the latest version of: https://pypi.org/project/databricks-connect/ 

You will need to configure your cluster to match the runtime supported by the
databricks-connect library:

https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html


```bash
pip install -U databricks-connect==12.2.14
```

#### Generate access token

See instructions for generating an access token:
https://docs.databricks.com/en/dev-tools/auth.html#generate-a-token

Save the token locally.

#### Databricks connect

```bash
databricks-connect configure
```

```bash
databricks-connect test
```

### Running locally on Linux using PySpark

For small datasets the mapping process can be run locally.

#### Installing miniconda

Instructions taken from: https://waylonwalker.com/install-miniconda/

```bash
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
~/miniconda3/bin/conda init bash
~/miniconda3/bin/conda init zsh
```

#### Setting up PySpark to run locally

Instructions adapted from: https://spark.apache.org/docs/latest/api/python/getting_started/install.html
and adapted to Ubuntu.

## Installing libraries

### Building a wheel to run scripts on a computer

If you have not built the wheel yet

```
pip install build
```

To build the `whl` file

```bash
 python -m build --wheel
```

To install the wheel file to run the scripts:

```bash
cd ./dist/
pip install preparedsource2ohdsi-0.1.0-py3-none-any.whl
```

## Running the tests

The tests use `unittest` and are addressed by module path from the repository root, with `src/`
and the repository root on `PYTHONPATH`. The only third-party module needed for these three
suites is `pypdf`, which `cda_to_prepared_source_fragments.py` imports at the top of the file.

```bash
cd PreparedSource2OHDSI
export PYTHONPATH=src:.

python -m unittest map.prepared_source.cda.test.test_cda_to_ps -v
python -m unittest map.prepared_source.cda.test.test_clean_combine -v
python -m unittest map.prepared_source.fhir.test.test_fhir_to_ps -v
```

`python -m unittest discover` does not find these tests. The directories between the repository
root and the test modules are not packages, so discovery stops at the top level and reports that
no tests ran.

`tests/test_mapping_utilities.py` is a Spark test. It needs `pyspark` and a JDK that the installed
Spark version supports, and it reads `gender_mappings.csv` relative to the working directory, so
run it from `tests/`:

```bash
cd tests
export PYTHONPATH=../src:..
python -m unittest test_mapping_utilities
```

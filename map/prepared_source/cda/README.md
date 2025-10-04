## C-CDA and Healthkit CDA to Prepared Source Format to the OHDSI Common Data Model (CDM)

Most mappings of data to the OHDSI CDM focus on mapping a single institutions 
EHR (Electronic Health Record) data. In reality a person will see multiple providers across 
different health systems and the person's full data will not be captured in a single EHR system.

Most patient portals now support export of medical records to the C-CDA format. 
C-CDA is a templated version of the CDA (Clinical Document Architecture)
This code is designed to take a collection of C-CDAs (Consolidated Clinical 
Document Architecture) and map them to the Prepared Source Format (PSF). The code
also supports the extraction of vitals from Apple Health Kit CDA export.

This is a demonstration project for understanding the complexity of combining multiple 
data sources into a standardized CDM for analytics. I will be using this code to combine 
multiple C-CDAs and CDA to build a personal health record including multiple providers 
and high frequency heart beat data recorded from the Apple Watch.

## Building you own personal health record

### Create a directory

Create a directory on your machine were you will store downloaded C-CDA XML
documents. For this example we will assume the directory is 
`/home/user/phr_data/fml_documents`. I am using my initials here to indicate this 
is my personal record.

### EHRs C-CDAs

Most EHR portals allow the export of your medical record in a C-CDA XML format. 
The ability to export your health record is a requirement for EHR vendors in the
United States. The exporting of your record is usually hidden deep in the web
portal of your EHR. 

For example in Epic's MyChart you will need to navigate to the menu, scroll down
to "Sharing", select "Sharing Hub", and on the next page select "Anyone Else" and then
select "Download or send a snapshot". Then select the date range of visits you want
to export and click continue and then click download all. When your documents are ready 
to download you will get an email and can download a zip file.

Once downloaded you should decompress the file, navigate first to the 
`./IHE_XDM/` folder to the subdirectory, a directory with your name, and here you
should find several xml files which follow the format `DSC*.xml`. Copy the XML files
to the directory ``/home/user/phr_data/jgh_documents/`.

### For Apple HealthKit

On your iPhone open the Health App and in the upper right corner 
of the screen click your initials and scroll to the bottom and select 
"Export All Health Data". This action will generate a zip file which takes 
several minutes to generate. You can select deliver by email and send the record 
to  a person email account. Download the zip file, decompress the zip file, and
navigate to the directory `./apple_health_export/`. Copy the file `export_cda.xml`
to the directory `/home/user/phr_data/jgh_documents/`.

### Mapping to Prepared Source

The directory should `/home/user/phr_data/jgh_documents/` 

Navigate in the checked out code to  `~/github/PreparedSource2OHDSI/map/prepared_source/cda/`
in your system command terminal.

```bash
python ./cda_to_prepared_source_fragments.py -d /home/user/phr_data/jgh_documents/
```

In the output directory the following directories will be generated

```bash
ls
files/
ps_frags/
ps/
```
In the directory `./ps_frags` each XML files is converted to Prepared Source CSV file. The next step is to
combine these files into a single file.

```bash
python ./cda_to_prepared_source_fragments.py -d /home/user/phr_data/jgh_documents/
```

### Mapping to OHDSI using the Docker container

```bash
cd ~/github/PreparedSource2OHDSI/map/prepared_source/csv/docker
docker build -t csv2ohdi:latest ./
```

```bash
docker run -it --name ps2ohdsi --hostname ps2ohdsi \
-v /home/user/ohdsi/vocabulary/20240708:/data/ohdsi/vocabulary \
-v /home/user/phr_data/jgh_documents/output/ohdsi:/data/ohdsi/output \
-v /home/user/phr_data/jgh_documents/output/ps:/data/prepared_source/csv \
-v /home/user/phr_data/jgh_documents/output/ps:/data/prepared_source \
csv2ohdsi:latest /bin/bash 
```

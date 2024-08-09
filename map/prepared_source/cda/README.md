## C-CDA and Healthkit CDA to Prepared Source Format to the OHDSI Common Data Model (CDM)

Most mappings of data to the OHDSI CDM focus on mapping a single institutions 
EHR (Electronic Health Record) data. In reality a person will see multiple providers across different health systems and the person's full data will not be captured in a single EHR system.

Most patient portals now support export of medical records to the C-CDA format. 
C-CDA is a templated version of the CDA (Clinical Document Architecture)
This code is designed to take a collection of C-CDAs (Consolidated Clinical 
Document Architecture) and map them to the Prepared Source Format (PSF). The code
also supports the extraction of vitals from Apple Health Kit CDA export.

The is a demonstration project for understanding the complexity of combining multiple 
data sources into a standardized CDM for analytics. I will be using this code to combine multiple C-CDAs and CDA to build a personal health record for analyits from multiple sources including multiple providers and high frequency heart beat data recorded from the Apple Watch.

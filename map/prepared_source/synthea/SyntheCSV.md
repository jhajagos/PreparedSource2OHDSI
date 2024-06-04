# Main Synthea CSV files

* `patients.csv`
  * Id
  * BIRTHDATE
  * DEATHDATE 
  * RACE
  * ETHNICITY
  * GENDER
  * ADDRESS
  * CITY
  * STATE
  * COUNTY
  * ZIP
  * LAT
  * LON

* `encounters.csv`
  * Id
  * START
  * STOP
  * PATIENT
  * PROVIDER
  * PAYER
  * ENCOUNTERCLASS
  * CODE
  * DESCRIPTION
  * REASONCODE
  * REASONDESCRIPTION

* `conditions.csv`
  * START
  * STOP
  * PATIENT
  * ENCOUNTER
  * CODE
  * DESCRIPTION
  
* `procedures.csv`
  * DATE
  * PATIENT
  * ENCOUNTER
  * CODE

* `medications.csv`
  * START
  * STOP
  * PATIENT
  * PAYER
  * ENCOUNTER
  * CODE
  * DESCRIPTION
  * DISPENSES

* `observations.csv`: Includes labs, vitals, survey answers
  * DATE
  * PATIENT
  * ENCOUNTER
  * CODE
  * DESCRIPTION
  * VALUE
  * UNITS
  * TYPE

* `immunizations.csv`
  * DATE
  * PATIENT
  * ENCOUNTER
  * CODE
  * DESCRIPTION
  
* `providers.csv`
* `payers.csv`
* `payer_transitions.csv`
* `devices.csv`
* `allergies.csv`
* `careplans.csv`
* `imaging_studies.csv`
* `organizations.csv`
* `supplies.csv`

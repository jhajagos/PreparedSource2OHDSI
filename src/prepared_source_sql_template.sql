/*
    Template to be modified for SQL ETL to map source data to the prepared source format
    
    Definitions:
    
    OID = Object Identifier https://www.hl7.org/oid/
    ETL = Extract Transform Load
    
    Key:
    
    s_ field prefix indicates source value representation
    m_ field prefix indicates value has been mapped / transformed
    k_ field prefix indicates a key value for linking to another table
    
    _code field suffix indicates value is coded
    _code_type field suffix is the human-readable description
    _code_type_oid field suffix is the OID value of the field
*/


select
    NULL as k_care_site --Foreign key to the care site
   ,NULL as s_care_site_name
   ,NULL as k_location
from SourceCareSiteObject;

select
    NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_encounter_id --Source identifier for encounter or visit
   ,NULL as s_start_condition_datetime
   ,NULL as s_end_condition_datetime
   ,NULL as s_condition_code
   ,NULL as s_condition_code_type
   ,NULL as s_condition_code_type_oid
   ,NULL as m_condition_type
   ,NULL as m_condition_type_code
   ,NULL as m_condition_type_code_type
   ,NULL as m_condition_type_code_type_oid
   ,NULL as s_condition_status
   ,NULL as s_condition_status_code
   ,NULL as s_condition_status_code_type
   ,NULL as s_condition_status_code_type_oid
   ,NULL as m_condition_status
   ,NULL as m_condition_status_code
   ,NULL as m_condition_status_code_type
   ,NULL as m_condition_status_code_type_oid
   ,NULL as s_present_on_admission_indicator
   ,NULL as s_sequence_id
   ,NULL as k_provider --Foreign key to the provider
   ,NULL as i_exclude --Value of 1 instructs the mapper to skip the row
   ,NULL as s_id --Row source identifier
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as m_source_system --Mapped source system the row was extracted from
from SourceConditionObject;

select
    NULL as s_id --Row source identifier
   ,NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_encounter_id --Source identifier for encounter or visit
   ,NULL as s_start_device_datetime
   ,NULL as s_end_device_datetime
   ,NULL as s_device
   ,NULL as s_device_code
   ,NULL as s_device_code_type
   ,NULL as s_device_code_type_oid
   ,NULL as m_device
   ,NULL as m_device_code
   ,NULL as m_device_code_type
   ,NULL as m_device_code_type_oid
   ,NULL as s_unique_device_identifier --A unique identifier for the device exposed to
   ,NULL as m_device_type
   ,NULL as m_device_type_code
   ,NULL as m_device_type_code_type
   ,NULL as m_device_type_code_type_oid
   ,NULL as k_provider --Foreign key to the provider
   ,NULL as i_exclude --Value of 1 instructs the mapper to skip the row
   ,NULL as s_source_system  --Source system row was extracted from
   ,NULL as m_source_system --Mapped source system the row was extracted from
from SourceDeviceObject;

select
    NULL as s_encounter_detail_id
   ,NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_encounter_id --Source identifier for encounter or visit
   ,NULL as s_start_datetime
   ,NULL as s_end_datetime
   ,NULL as k_care_site --Foreign key to the care site
   ,NULL as s_visit_detail_type
   ,NULL as s_visit_detail_type_code
   ,NULL as s_visit_detail_type_code_type
   ,NULL as s_visit_detail_type_code_type_oid
   ,NULL as m_visit_detail_type
   ,NULL as m_visit_detail_type_code
   ,NULL as m_visit_detail_type_code_type
   ,NULL as m_visit_detail_type_code_type_oid
   ,NULL as m_visit_detail_source
   ,NULL as m_visit_detail_source_code
   ,NULL as m_visit_detail_source_code_type
   ,NULL as m_visit_detail_source_code_type_oid
   ,NULL as k_provider --Foreign key to the provider
   ,NULL as i_exclude --Value of 1 instructs the mapper to skip the row
   ,NULL as s_id --Row source identifier
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as m_source_system --Mapped source system the row was extracted from
from SourceEncounterDetailObject;

select
    NULL as s_encounter_id --Source identifier for encounter or visit
   ,NULL as s_map_name
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as s_sequence_id
   ,NULL as s_alternative_id
from SourceEncounterMapObject;

select
    NULL as s_encounter_id --Source identifier for encounter or visit
   ,NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_visit_start_datetime
   ,NULL as s_visit_end_datetime
   ,NULL as s_visit_type
   ,NULL as s_visit_type_code
   ,NULL as s_visit_type_code_type
   ,NULL as s_visit_type_code_type_oid
   ,NULL as m_visit_type
   ,NULL as m_visit_type_code
   ,NULL as m_visit_type_code_type
   ,NULL as m_visit_type_code_type_oid
   ,NULL as m_visit_source
   ,NULL as m_visit_source_code
   ,NULL as m_visit_source_code_type
   ,NULL as m_visit_source_code_type_oid
   ,NULL as s_discharge_to
   ,NULL as s_discharge_to_code
   ,NULL as s_discharge_to_code_type
   ,NULL as s_discharge_to_code_type_oid
   ,NULL as m_discharge_to
   ,NULL as m_discharge_to_code
   ,NULL as m_discharge_to_code_type
   ,NULL as m_discharge_to_code_type_oid
   ,NULL as s_admitting_source
   ,NULL as s_admitting_source_code
   ,NULL as s_admitting_source_code_type
   ,NULL as s_admitting_source_code_type_oid
   ,NULL as m_admitting_source
   ,NULL as m_admitting_source_code
   ,NULL as m_admitting_source_code_type
   ,NULL as m_admitting_source_code_type_oid
   ,NULL as k_care_site --Foreign key to the care site
   ,NULL as k_provider --Foreign key to the provider
   ,NULL as i_exclude --Value of 1 instructs the mapper to skip the row
   ,NULL as s_id --Row source identifier
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as m_source_system --Mapped source system the row was extracted from
from SourceEncounterObject;

select
    NULL as k_location
   ,NULL as s_address_1
   ,NULL as s_address_2
   ,NULL as s_city
   ,NULL as s_state
   ,NULL as s_zip
   ,NULL as s_county
   ,NULL as s_country
   ,NULL as s_location_name
   ,NULL as s_latitude
   ,NULL as s_longitude
   ,NULL as s_geocoding_type
from SourceLocationObject;

select
    NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_encounter_id --Source identifier for encounter or visit
   ,NULL as s_drug_code
   ,NULL as s_drug_code_type
   ,NULL as s_drug_code_type_oid
   ,NULL as m_drug_code
   ,NULL as m_drug_code_type
   ,NULL as m_drug_code_type_oid
   ,NULL as s_drug_text
   ,NULL as m_drug_text
   ,NULL as g_drug_text
   ,NULL as g_drug_code
   ,NULL as g_drug_code_type
   ,NULL as g_drug_code_type_oid
   ,NULL as s_drug_alternative_text
   ,NULL as s_start_medication_datetime
   ,NULL as s_end_medication_datetime
   ,NULL as s_route
   ,NULL as s_route_code
   ,NULL as s_route_code_type
   ,NULL as s_route_code_type_oid
   ,NULL as m_route
   ,NULL as m_route_code
   ,NULL as m_route_code_type
   ,NULL as m_route_code_type_oid
   ,NULL as s_quantity
   ,NULL as s_dose
   ,NULL as m_dose
   ,NULL as s_dose_unit
   ,NULL as s_dose_unit_code
   ,NULL as s_dose_unit_code_type
   ,NULL as s_dose_unit_code_type_oid
   ,NULL as m_dose_unit
   ,NULL as m_dose_unit_code
   ,NULL as m_dose_unit_code_type
   ,NULL as m_dose_unit_code_type_oid
   ,NULL as s_status
   ,NULL as s_drug_type
   ,NULL as s_drug_type_code
   ,NULL as s_drug_type_code_type
   ,NULL as s_drug_type_code_type_oid
   ,NULL as m_drug_type
   ,NULL as m_drug_type_code
   ,NULL as m_drug_type_code_type
   ,NULL as m_drug_type_code_type_oid
   ,NULL as k_provider --Foreign key to the provider
   ,NULL as i_exclude --Value of 1 instructs the mapper to skip the row
   ,NULL as s_id --Row source identifier
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as m_source_system --Mapped source system the row was extracted from
from SourceMedicationObject;

select
    NULL as s_id --Row source identifier
   ,NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_encounter_id --Source identifier for encounter or visit
   ,NULL as s_note_datetime --The date the note was written or saved
   ,NULL as s_note_text --Plain text representation of the note
   ,NULL as s_note_title
   ,NULL as s_note_class --The class that a note belongs to e.g., radiology note, physician note
   ,NULL as s_note_class_code --The LOINC code to classify the note
   ,NULL as s_note_class_code_type --Should be LOINC code
   ,NULL as s_note_class_code_type_oid --Should be LOINC code type OID
   ,NULL as m_note_class
   ,NULL as m_note_class_code
   ,NULL as m_note_class_code_type
   ,NULL as m_note_class_code_type_oid
   ,NULL as k_provider --Foreign key to the provider
   ,NULL as s_note_type --The source of the note most likely EHR
   ,NULL as s_note_type_code --
   ,NULL as s_note_type_code_type --
   ,NULL as s_note_type_code_type_oid --
   ,NULL as s_language --The language of the note
   ,NULL as s_language_code --
   ,NULL as s_language_code_type --
   ,NULL as s_language_code_type_oid --
   ,NULL as s_encoding --The text encoding of the note (UTF8)
   ,NULL as s_encoding_code --
   ,NULL as s_encoding_code_type --
   ,NULL as s_encoding_code_type_oid --
   ,NULL as i_exclude --Value of 1 instructs the mapper to skip the row
from SourceNoteObject;

select
    NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_start_observation_datetime
   ,NULL as s_end_observation_datetime
   ,NULL as m_source
   ,NULL as m_source_code
   ,NULL as m_source_code_type
   ,NULL as m_source_code_type_oid
from SourceObservationPeriodObject;

select
    NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_payer_start_datetime --The date the plan's coverage started
   ,NULL as s_payer_end_datetime --The date the plan's contribution ended
   ,NULL as s_payer --The name of the payer
   ,NULL as s_payer_code
   ,NULL as s_payer_code_type
   ,NULL as s_payer_code_type_oid
   ,NULL as m_payer
   ,NULL as m_payer_code
   ,NULL as m_payer_code_type
   ,NULL as m_payer_code_type_oid
   ,NULL as s_plan --The plan type e.g, PPO, silver, bronze
   ,NULL as s_plan_code
   ,NULL as s_plan_code_type
   ,NULL as s_plan_code_type_oid
   ,NULL as m_plan
   ,NULL as m_plan_code
   ,NULL as m_plan_code_type
   ,NULL as m_plan_code_type_oid
   ,NULL as s_contributor
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as m_source_system --Mapped source system the row was extracted from
   ,NULL as i_exclude --Value of 1 instructs the mapper to skip the row
from SourcePayerObject;

select
    NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_start_datetime
   ,NULL as s_end_datetime
   ,NULL as k_location
   ,NULL as s_address_type
   ,NULL as s_address_type_code
   ,NULL as s_address_type_code_type
   ,NULL as s_address_type_code_type_oid
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as m_source_system --Mapped source system the row was extracted from
from SourcePersonAddressHistoryObject;

select
    NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_map_name
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as s_sequence_id
   ,NULL as s_alternative_id
from SourcePersonMapObject;

select
    NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_gender --Source gender description
   ,NULL as s_gender_code --Source gender code
   ,NULL as s_gender_code_type --Source gender code type (human-readable & OPTIONAL)
   ,NULL as s_gender_code_type_oid --Source gender code type specified by OID (used in OHDSI mapping)
   ,NULL as m_gender --Mapped by ETL gender description
   ,NULL as m_gender_code --Mapped by ETL gender code (See: https://phinvads.cdc.gov/vads/ViewCodeSystem.action?id=2.16.840.1.113883.12.1)
   ,NULL as m_gender_code_type --Mapped by ETL gender code type
   ,NULL as m_gender_code_type_oid --Mapped by ETL gender code type by OID (Gender 2.16.840.1.113883.12.1)
   ,NULL as s_birth_datetime
   ,NULL as s_death_datetime
   ,NULL as m_death_source
   ,NULL as m_death_source_code
   ,NULL as m_death_source_code_type
   ,NULL as m_death_source_code_type_oid
   ,NULL as s_race --Source race (Black, white, etc) description
   ,NULL as s_race_code --Source race code
   ,NULL as s_race_code_type --Source race code type (human-readable & OPTIONAL)
   ,NULL as s_race_code_type_oid
   ,NULL as m_race
   ,NULL as m_race_code --See: https://athena.ohdsi.org/search-terms/terms?domain=Race&standardConcept=Standard&page=1&pageSize=15&query=
   ,NULL as m_race_code_type
   ,NULL as m_race_code_type_oid --Use 'ohdsi.race' for OHDSI standard race codes
   ,NULL as s_ethnicity --Source ethnicity description (Hispanic, Not Hispanic)
   ,NULL as s_ethnicity_code --Source ethnicity code
   ,NULL as s_ethnicity_code_type --Source gender code type (human-readable & OPTIONAL)
   ,NULL as s_ethnicity_code_type_oid --Source ethnicity code type specified by OID (used in OHDSI mapping)
   ,NULL as m_ethnicity
   ,NULL as m_ethnicity_code --See: https://athena.ohdsi.org/search-terms/terms?domain=Ethnicity&standardConcept=Standard&page=1&pageSize=15&query=
   ,NULL as m_ethnicity_code_type
   ,NULL as m_ethnicity_code_type_oid --Use 'ohdsi.ethnicity' for standard OHDSI ethnicity codes
   ,NULL as k_location --Key value to location in source_location table
   ,NULL as i_exclude --Value of 1 instructs the mapper to skip the row
   ,NULL as s_id --Row source identifier
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as m_source_system --Mapped source system the row was extracted from
from SourcePersonObject;

select
    NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_encounter_id --Source identifier for encounter or visit
   ,NULL as s_start_procedure_datetime
   ,NULL as s_end_procedure_datetime
   ,NULL as s_procedure_code
   ,NULL as s_procedure_code_type
   ,NULL as s_procedure_code_type_oid
   ,NULL as m_procedure_type
   ,NULL as m_procedure_type_code
   ,NULL as m_procedure_type_code_type
   ,NULL as m_procedure_type_code_type_oid
   ,NULL as s_sequence_id
   ,NULL as s_modifier
   ,NULL as s_modifier_code
   ,NULL as s_modifier_code_type
   ,NULL as s_modifier_code_type_oid
   ,NULL as s_quantity
   ,NULL as k_provider --Foreign key to the provider
   ,NULL as i_exclude --Value of 1 instructs the mapper to skip the row
   ,NULL as s_id --Row source identifier
   ,NULL as s_place_of_service_code
   ,NULL as s_place_of_service_code_type
   ,NULL as s_place_of_service_code_type_oid
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as m_source_system --Mapped source system the row was extracted from
from SourceProcedureObject;

select
    NULL as k_provider --Foreign key to the provider
   ,NULL as s_map_name
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as s_sequence_id
   ,NULL as s_alternative_id
from SourceProviderMapObject;

select
    NULL as k_provider --Foreign key to the provider
   ,NULL as s_provider_name
   ,NULL as s_npi --National Provider Identifier
   ,NULL as s_dea_numbers_specialty
   ,NULL as s_specialty_code
   ,NULL as s_specialty_code_type
   ,NULL as s_specialty_code_type_oid
   ,NULL as m_specialty
   ,NULL as m_specialty_code
   ,NULL as m_specialty_code_type
   ,NULL as m_specialty_code_type_oid
   ,NULL as s_birth_datetime
   ,NULL as s_gender
   ,NULL as s_gender_code
   ,NULL as s_gender_code_type
   ,NULL as s_gender_code_type_oid
   ,NULL as m_gender
   ,NULL as m_gender_code
   ,NULL as m_gender_code_type
   ,NULL as m_gender_code_type_oid
   ,NULL as k_care_site --Foreign key to the care site
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as m_source_system --Mapped source system the row was extracted from
   ,NULL as i_exclude --Value of 1 instructs the mapper to skip the row
from SourceProviderObject;

select
    NULL as k_provider --Foreign key to the provider
   ,NULL as s_sequence_id
   ,NULL as s_specialty
   ,NULL as s_specialty_code
   ,NULL as s_specialty_code_type
   ,NULL as s_specialty_code_type_oid
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as m_source_system --Mapped source system the row was extracted from
from SourceProviderSpecialtyObject;

select
    NULL as s_id --Row source identifier
   ,NULL as s_relationship --The relationship type
   ,NULL as s_relationship_code --The code of the relationship
   ,NULL as s_relationship_code_type --The code type of the relationship
   ,NULL as s_relationship_code_type_oid --The code type OID
   ,NULL as s_target_from_table_name --The source table name to target
   ,NULL as s_target_from_table_field --The table field of the source table name to target
   ,NULL as s_target_from_value --The field value in the source table to target
   ,NULL as s_target_to_table_name
   ,NULL as s_target_to_table_field
   ,NULL as s_target_to_value
from SourceRelationshipObject;

select
    NULL as s_person_id --Source identifier for patient or person
   ,NULL as s_encounter_id --Source identifier for encounter or visit
   ,NULL as s_obtained_datetime
   ,NULL as s_name
   ,NULL as s_code
   ,NULL as s_code_type
   ,NULL as s_code_type_oid
   ,NULL as m_code
   ,NULL as m_code_type
   ,NULL as m_code_type_oid
   ,NULL as s_result_text
   ,NULL as m_result_text
   ,NULL as s_result_numeric
   ,NULL as m_result_numeric
   ,NULL as s_result_datetime
   ,NULL as s_result_code
   ,NULL as s_result_code_type
   ,NULL as s_result_code_type_oid
   ,NULL as m_result_code
   ,NULL as m_result_code_type
   ,NULL as m_result_code_type_oid
   ,NULL as s_result_unit
   ,NULL as s_result_unit_code
   ,NULL as s_result_unit_code_type
   ,NULL as s_result_unit_code_type_oid
   ,NULL as m_result_unit
   ,NULL as m_result_unit_code
   ,NULL as m_result_unit_code_type
   ,NULL as m_result_unit_code_type_oid
   ,NULL as s_result_numeric_lower --The source numeric lower limit for the normal range
   ,NULL as s_result_numeric_upper --The source numeric upper limit for the normal range
   ,NULL as m_result_numeric_lower --Lower bound of the result range for the normal range
   ,NULL as m_result_numeric_upper --Upper bound of the result range for the normal range
   ,NULL as s_operator
   ,NULL as m_operator
   ,NULL as m_operator_code
   ,NULL as m_operator_code_type
   ,NULL as m_operator_code_type_oid
   ,NULL as s_source
   ,NULL as m_source
   ,NULL as m_source_code
   ,NULL as m_source_code_type
   ,NULL as m_source_code_type_oid
   ,NULL as i_exclude --Value of 1 instructs the mapper to skip the row
   ,NULL as s_id --Row source identifier
   ,NULL as k_provider --Foreign key to the provider
   ,NULL as s_source_system --Source system row was extracted from
   ,NULL as m_source_system --Mapped source system the row was extracted from
from SourceResultObject;
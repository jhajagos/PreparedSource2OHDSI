""""
    Prepared source is an intermediary format for mapping data to the OHDSI format. It does
    not impose any data type restrictions. The goal is to get EHR/Claims based data into this format
    in a quick and straight forward manner and then have a single mapper for converting to OHDSI format.

    Fields have the following prefixes

    s_ source
    m_ mapped

    As an example for

    s_gender
    "M"

    m_gender
    "Male"

    s_race_code_type
    "Race (HL7)"

    s_race_code_type_oid
    "2.16.840.1.114222.4.11.6065"

    i_exclude : 1 indicates that this record should be ignored by the OHDSI mapper

    Initial concept derived from:
    https://github.com/jhajagos/CommonDataModelMapper/tree/master/omop_cdm

"""

#TODO: Add PreparedSourceNote()


class PreparedSourceObject(object):
    def fields(self):
        return self._fields()

    def table_name(self):
        return self._table_name()

    def meta_data(self):
        local_meta_data = self._meta_data()
        local_fields = self.fields()
        for key in self.global_meta_data():
            if key in local_fields:
                local_meta_data[key] = self.global_meta_data()[key]
        return local_meta_data

    def global_meta_data(self):
        return {"s_person_id": "Source identifier for patient or person",
                "s_encounter_id": "Source identifier for encounter or visit",
                "s_id": "Row source identifier",
                "s_source_system": "Source system row was extracted from",
                "m_source_system": "Mapped source system the row was extracted from",
                "i_exclude": "Value of 1 instructs the mapper to skip the row",
                "k_provider": "Foreign key to the provider",
                "k_care_site": "Foreign key to the care care site"
                }

    def _meta_data(self):
        return {}

    def dict_template(self):
        """Generate template"""

        return {c: None for c in self._fields()}


class SourcePersonObject(PreparedSourceObject):
    """Details about a person"""

    def _table_name(self):
        return "source_person"

    def _fields(self):
        return [
                "s_person_id",
                "s_gender",
                "s_gender_code",
                "s_gender_code_type",
                "s_gender_code_type_oid",
                "m_gender",
                "m_gender_code",
                "m_gender_code_type",
                "m_gender_code_type_oid",
                "s_birth_datetime",
                "s_death_datetime",
                "m_death_source",
                "m_death_source_code",
                "m_death_source_code_type",
                "m_death_source_code_type_oid",
                "s_race",
                "s_race_code",
                "s_race_code_type",
                "s_race_code_type_oid",
                "m_race",
                "m_race_code",
                "m_race_code_type",
                "m_race_code_type_oid",
                "s_ethnicity",
                "s_ethnicity_code",
                "s_ethnicity_code_type",
                "s_ethnicity_code_type_oid",
                "m_ethnicity",
                "m_ethnicity_code",
                "m_ethnicity_code_type",
                "m_ethnicity_code_type_oid",
                "k_location",
                "i_exclude",
                "s_id",
                "s_source_system",
                "m_source_system"
        ]

    def _meta_data(self):
        return {}

    def _meta_data(self):
        return {"s_gender": "Source gender description",
                "s_gender_code": "Source gender code",
                "s_gender_code_type": "Source gender code type (human readable & OPTIONAL)",
                "s_gender_code_type_oid": "Source gender code type specified by OID (used in OHDSI mapping)",
                "m_gender": "Mapped by ETL gender description",
                "m_gender_code": "Mapped by ETL gender code (See: https://phinvads.cdc.gov/vads/ViewCodeSystem.action?id=2.16.840.1.113883.12.1)",
                "m_gender_code_type": "Mapped by ETL gender code type",
                "m_gender_code_type_oid": "Mapped by ETL gender code type by OID (Gender 2.16.840.1.113883.12.1)",
                "s_race": "Source race (Black, white, etc) description",
                "s_race_code": "Source race code",
                "s_race_code_type": "Source race code type (human readable & OPTIONAL)",
                "m_race_code": "See: https://athena.ohdsi.org/search-terms/terms?domain=Race&standardConcept=Standard&page=1&pageSize=15&query=",
                "m_race_code_type_oid": "Use 'ohdsi.race' for OHDSI standard race codes",
                "s_ethnicity": "Source ethnicity description (Hispanic, Not Hispanic)",
                "s_ethnicity_code": "Source ethnicity code",
                "s_ethnicity_code_type": "Source gender code type (human readable & OPTIONAL)",
                "s_ethnicity_code_type_oid": "Source ethnicity code type specified by OID (used in OHDSI mapping)",
                "m_ethnicity_code": "See: https://athena.ohdsi.org/search-terms/terms?domain=Ethnicity&standardConcept=Standard&page=1&pageSize=15&query=",
                "m_ethnicity_code_type_oid": "Use 'ohdsi.ethnicity' for standard OHDSI ethnicity codes",
                "k_location": "Key value to location in source_location table"
                }


class SourcePersonMapObject(PreparedSourceObject):
    """Stores links to multiple identifiers for a person or patient e.g., an MRN"""
    def _fields(self):
        return [
                "s_person_id",
                "s_map_name",
                "s_source_system",
                "s_sequence_id",
                "s_alternative_id"
        ]

    def _meta_data(self):
        return {}


class SourceObservationPeriodObject(PreparedSourceObject):
    """An observation period for the person"""

    def _fields(self):
        return ["s_person_id",
                "s_start_observation_datetime",
                "s_end_observation_datetime",
                "m_source",
                "m_source_code",
                "m_source_code_type",
                "m_source_code_type_oid"
                ]
    def _meta_data(self):
        return {}


class SourceEncounterObject(PreparedSourceObject):
    """An encounter or visit"""

    def _fields(self):
        return ["s_encounter_id",
                "s_person_id",
                "s_visit_start_datetime",
                "s_visit_end_datetime",
                "s_visit_type",
                "s_visit_type_code",
                "s_visit_type_code_type",
                "s_visit_type_code_type_oid",
                "m_visit_type",
                "m_visit_type_code",
                "m_visit_type_code_type",
                "m_visit_type_code_type_oid",
                "m_visit_source",  # This is generated based on the source so there is no "s_" fields
                "m_visit_source_code",
                "m_visit_source_code_type",
                "m_visit_source_code_type_oid",
                "s_discharge_to",
                "s_discharge_to_code",
                "s_discharge_to_code_type",
                "s_discharge_to_code_type_oid",
                "m_discharge_to",
                "m_discharge_to_code",
                "m_discharge_to_code_type",
                "m_discharge_to_code_type_oid",
                "s_admitting_source",
                "s_admitting_source_code",
                "s_admitting_source_code_type",
                "s_admitting_source_code_type_oid",
                "m_admitting_source",
                "m_admitting_source_code",
                "m_admitting_source_code_type",
                "m_admitting_source_code_type_oid",
                "k_care_site",
                "k_provider",
                "i_exclude",
                "s_id",
                "s_source_system",
                "m_source_system"
                ]

    def _meta_data(self):
        return {}


class SourceEncounterMapObject(PreparedSourceObject):
    """Stores links to additionally encounter identifiers, e.g., transaction control numbers"""
    def _fields(self):
        return [
                "s_encounter_id",
                "s_map_name",
                "s_source_system",
                "s_sequence_id",
                "s_alternative_id"
        ]

    def _meta_data(self):
        return {}


class SourceEncounterDetailObject(PreparedSourceObject):
    """Detailed sub-visits (ADT style transactional data) during an encounter"""

    def _fields(self):
        return ["s_encounter_detail_id",
                "s_person_id",
                "s_encounter_id",
                "s_start_datetime",
                "s_end_datetime",
                "k_care_site",
                "s_visit_detail_type",
                "s_visit_detail_type_code",
                "s_visit_detail_type_code_type",
                "s_visit_detail_type_code_type_oid",
                "m_visit_detail_type",
                "m_visit_detail_type_code",
                "m_visit_detail_type_code_type",
                "m_visit_detail_type_code_type_oid",
                "m_visit_detail_source",  # This is generated based on the source so there is no "s_" fields
                "m_visit_detail_source_code",
                "m_visit_detail_source_code_type",
                "m_visit_detail_source_code_type_oid",
                "i_exclude",
                "s_id",
                "s_source_system",
                "m_source_system"
                ]

    def _meta_data(self):
        return {}


class SourceConditionObject(PreparedSourceObject):
    """Conditions or diagnosis codes (ICD10CM coding)"""

    def _meta_data(self):
        return {
          "s_start_condition_datetime": "Condition start date time",
          "s_end_condition_datetime": "Condition end date time",
          "s_condition_code": "Code for condition/diagnosis",
          "s_condition_code_type": "Condition code type (Human readable)",
          "s_condition_code_type_oid": "Condition code type by OID (used by OHDSI mapper) ICD10: 2.16.840.1.113883.6.90",
          "m_condition_type": "Source of concept (usually EHR, Claim)",
          "m_condition_type_code": "See: https://athena.ohdsi.org/search-terms/terms?domain=Type+Concept&standardConcept=Standard&page=1&pageSize=15&query=",
          "m_condition_type_code_type": "",
          "m_condition_type_code_type_oid": "Use 'ohdsi.type_concept",
          "s_condition_status": "Type of condition code (admit, discharge)",
          "s_condition_status_code": "Local source code",
          "s_condition_status_code_type": "Local source code type",
          "s_condition_status_code_type_oid": "",
          "m_condition_status": "",
          "m_condition_status_code": "",
          "m_condition_status_code_type": "See: https://athena.ohdsi.org/search-terms/terms?domain=Condition+Status&standardConcept=Standard&page=1&pageSize=15&query=",
          "m_condition_status_code_type_oid": "Use 'ohdsi.type_concept'",
          "s_present_on_admission_indicator": "Indicates whether diagnosis is POA (not mapped to OHDSI table)"
        }

    def _fields(self):
        return ["s_person_id",
                "s_encounter_id",
                "s_start_condition_datetime",
                "s_end_condition_datetime",
                "s_condition_code",
                "s_condition_code_type",
                "s_condition_code_type_oid",
                "m_condition_type",
                "m_condition_type_code",
                "m_condition_type_code_type",
                "m_condition_type_code_type_oid",
                "s_condition_status",
                "s_condition_status_code",
                "s_condition_status_code_type",
                "s_condition_status_code_type_oid",
                "m_condition_status",
                "m_condition_status_code",
                "m_condition_status_code_type",
                "m_condition_status_code_type_oid",
                "s_present_on_admission_indicator",
                "s_sequence_id",
                "k_provider",
                "i_exclude",
                "s_id",
                "s_source_system",
                "m_source_system"
                ]

    def _meta_data(self):
        return {}


class SourceProcedureObject(PreparedSourceObject):
    """Procedures e.g., CPT billing codes"""
    def _fields(self):
        return [
                "s_person_id",
                "s_encounter_id",
                "s_start_procedure_datetime",
                "s_end_procedure_datetime",
                "s_procedure_code",
                "s_procedure_code_type",
                "s_procedure_code_type_oid",
                "m_procedure_type",
                "m_procedure_type_code",
                "m_procedure_type_code_type",
                "m_procedure_type_code_type_oid",
                "s_sequence_id",
                "s_modifier",
                "s_modifier_code",
                "s_modifier_code_type",
                "s_modifier_code_type_oid",
                "s_quantity",
                "k_provider",
                "i_exclude",
                "s_id",
                "s_place_of_service_code",
                "s_place_of_service_code_type",
                "s_place_of_service_code_type_oid",
                "s_source_system",
                "m_source_system"
                ]

    def _meta_data(self):
        return {}


class SourceDeviceObject(PreparedSourceObject):
    def _fields(self):
        return [
            "s_id",
            "s_person_id",
            "s_encounter_id",
            "s_start_device_datetime",
            "s_end_device_datetime",
            "s_device",
            "s_device_code",
            "s_device_code_type",
            "s_device_code_type_oid",
            "m_device",
            "m_device_code",
            "m_device_code_type",
            "m_device_code_type_oid",
            "s_unique_device_identifier",
            "m_device_type",
            "m_device_type_code",
            "m_device_type_code_type",
            "m_device_type_code_type_oid",
            "i_exclude",
            "s_source_system",
            "m_source_system"
        ]

    def _meta_data(self):
        return {"s_unique_device_identifier": "A unique identifier for the device exposed to"}


class SourceResultObject(PreparedSourceObject):
    """Results include: vitals, lab results (numeric or coded), survey assessments"""

    def _fields(self):
        return ["s_person_id",
                "s_encounter_id",
                "s_obtained_datetime",
                "s_name",
                "s_code",
                "s_code_type",
                "s_code_type_oid",
                "m_code",
                "m_code_type",
                "m_code_type_oid",
                "s_result_text",
                "m_result_text",
                "s_result_numeric",
                "m_result_numeric",
                "s_result_datetime",
                "s_result_code",
                "s_result_code_type",
                "s_result_code_type_oid",
                "m_result_code",
                "m_result_code_type",
                "m_result_code_type_oid",
                "s_result_unit",
                "s_result_unit_code",
                "s_result_unit_code_type",
                "s_result_unit_code_type_oid",
                "m_result_unit",
                "m_result_unit_code",
                "m_result_unit_code_type",
                "m_result_unit_code_type_oid",
                "s_result_numeric_lower",
                "s_result_numeric_upper",
                "s_operator",
                "m_operator",
                "m_operator_code",
                "m_operator_code_type",
                "m_operator_code_type_oid",
                "s_source",
                "m_source",
                "m_source_code",
                "m_source_code_type",
                "m_source_code_type_oid",
                "i_exclude",
                "s_id",
                "s_source_system",
                "m_source_system"
                ]

    def _meta_data(self):
        return {}


class SourceMedicationObject(PreparedSourceObject):
    """Ordered, administered medications and drugs"""
    def _fields(self):
        return ["s_person_id",
                "s_encounter_id",
                "s_drug_code",
                "s_drug_code_type",
                "s_drug_code_type_oid",
                "m_drug_code",
                "m_drug_code_type",
                "m_drug_code_type_oid",
                "s_drug_text",
                "m_drug_text",
                "g_drug_text",
                "g_drug_code",
                "g_drug_code_type",
                "g_drug_code_type_oid",
                "s_drug_alternative_text",
                "s_start_medication_datetime",
                "s_end_medication_datetime",
                "s_route",
                "s_route_code",
                "s_route_code_type",
                "s_route_code_type_oid",
                "m_route",
                "m_route_code",
                "m_route_code_type",
                "m_route_code_type_oid",
                "s_quantity",
                "s_dose",
                "m_dose",
                "s_dose_unit",
                "s_dose_unit_code",
                "s_dose_unit_code_type",
                "s_dose_unit_code_type_oid",
                "m_dose_unit",
                "m_dose_unit_code",
                "m_dose_unit_code_type",
                "m_dose_unit_code_type_oid",
                "s_status",
                "s_drug_type",
                "s_drug_type_code",
                "s_drug_type_code_type",
                "s_drug_type_code_type_oid",
                "m_drug_type",
                "m_drug_type_code",
                "m_drug_type_code_type",
                "m_drug_type_code_type_oid",
                "k_provider",
                "i_exclude",
                "s_id",
                "s_source_system",
                "m_source_system"
                ]

    def _meta_data(self):
        return {}


class SourceCareSiteObject(PreparedSourceObject):
    def _fields(self):
        return ["k_care_site", "s_care_site_name"]

    def _meta_data(self):
        return {}


class SourceProviderObject(PreparedSourceObject):
    def _fields(self):
        return ["k_provider", "s_provider_name", "s_npi", "s_source_system", "m_source_system", "i_exclude"]

    def _meta_data(self):
        return {}


class SourceLocationObject(PreparedSourceObject):
    """Address and locations"""

    def _table_name(self):
        return "source_location"

    def _fields(self):
        return [
                "k_location",
                "s_address_1",
                "s_address_2",
                "s_city",
                "s_state",
                "s_zip",
                "s_county",
                "s_country",
                "s_location_name",
                "s_latitude",
                "s_longitude"
               ]

    def _meta_data(self):
        return {}


class SourcePayerObject(PreparedSourceObject):
    """Insurance and payer information"""

    def _fields(self):
        return [
            "s_person_id",
            "s_payer_start_datetime",
            "s_payer_end_datetime",
            "s_payer",
            "s_payer_code",
            "s_payer_code_type",
            "s_payer_code_type_oid",
            "m_payer",
            "m_payer_code",
            "m_payer_code_type",
            "m_payer_code_type_oid",
            "s_plan",
            "s_plan_code",
            "s_plan_code_type",
            "s_plan_code_type_oid",
            "m_plan",
            "m_plan_code",
            "m_plan_code_type",
            "m_plan_code_type_oid",
            "s_contributor",
            "s_source_system",
            "m_source_system",
            "i_exclude"
        ]

    def _meta_data(self):
        return {"s_payer_start_datetime": "The date the plan's coverage started",
                "s_payer_end_datetime": "The date the plan's contribution ended",
                "s_payer": "The name of the payer",
                "s_plan": "The plan type e.g, PPO, silver, bronze",
                }


class SourceNoteObject(PreparedSourceObject):
    def _fields(self):
        return ["s_id", "s_person_id", "s_encounter_id", "s_note_datetime", "s_note_text", "s_note_title",
                "s_note_class", "s_note_class_code", "s_note_class_code_type", "s_note_class_code_type_oid",
                "k_provider", "s_note_type", "s_note_type_code", "s_note_type_oid",
                "s_language", "s_language_code", "s_language_code_type", "s_language_code_type_oid",
                "s_encoding", "s_encoding_code", "s_encoding_code_type", "s_encoding_code_type_oid"]

    def _meta_data(self):
        return {}
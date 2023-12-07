from preparedsource2ohdsi.base_ohdsi_cdm import OHDSIOutputClass


class OHDSI54OutputClass(OHDSIOutputClass):
    """Child Class 5.4"""

    def _version(self):
        return "5.4"


class CareSiteObject(OHDSI54OutputClass):
    def table_name(self):
        return "care_site"

    def _fields(self):
        return ["care_site_id", "care_site_name", "place_of_service_concept_id", "location_id", "care_site_source_value", "place_of_service_source_value"]

    def _data_types(self):
        return {"care_site_id": "INTEGER", "care_site_name": "VARCHAR(255)", "place_of_service_concept_id": "INTEGER", "location_id": "INTEGER", "care_site_source_value": "VARCHAR(50)", "place_of_service_source_value": "VARCHAR(50)"}


class CdmSourceObject(OHDSI54OutputClass):
    def table_name(self):
        return "cdm_source"

    def _fields(self):
        return ["cdm_source_name", "cdm_source_abbreviation", "cdm_holder", "source_description", "source_documentation_reference", "cdm_etl_reference", "source_release_date", "cdm_release_date", "cdm_version", "cdm_version_concept_id", "vocabulary_version"]

    def _data_types(self):
        return {"cdm_source_name": "VARCHAR(255)", "cdm_source_abbreviation": "VARCHAR(25)", "cdm_holder": "VARCHAR(255)", "source_description": "STRING", "source_documentation_reference": "VARCHAR(255)", "cdm_etl_reference": "VARCHAR(255)", "source_release_date": "DATE", "cdm_release_date": "DATE", "cdm_version": "VARCHAR(10)", "cdm_version_concept_id": "INTEGER", "vocabulary_version": "VARCHAR(20)"}


class CohortObject(OHDSI54OutputClass):
    def table_name(self):
        return "cohort"

    def _fields(self):
        return ["cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"]

    def _data_types(self):
        return {"cohort_definition_id": "INTEGER", "subject_id": "INTEGER", "cohort_start_date": "DATE", "cohort_end_date": "DATE"}


class CohortDefinitionObject(OHDSI54OutputClass):
    def table_name(self):
        return "cohort_definition"

    def _fields(self):
        return ["cohort_definition_id", "cohort_definition_name", "cohort_definition_description", "definition_type_concept_id", "cohort_definition_syntax", "subject_concept_id", "cohort_initiation_date"]

    def _data_types(self):
        return {"cohort_definition_id": "INTEGER", "cohort_definition_name": "VARCHAR(255)", "cohort_definition_description": "STRING", "definition_type_concept_id": "INTEGER", "cohort_definition_syntax": "STRING", "subject_concept_id": "INTEGER", "cohort_initiation_date": "DATE"}


class ConceptObject(OHDSI54OutputClass):
    def table_name(self):
        return "concept"

    def _fields(self):
        return ["concept_id", "concept_name", "domain_id", "vocabulary_id", "concept_class_id", "standard_concept", "concept_code", "valid_start_date", "valid_end_date", "invalid_reason"]

    def _data_types(self):
        return {"concept_id": "INTEGER", "concept_name": "VARCHAR(255)", "domain_id": "VARCHAR(20)", "vocabulary_id": "VARCHAR(20)", "concept_class_id": "VARCHAR(20)", "standard_concept": "VARCHAR(1)", "concept_code": "VARCHAR(50)", "valid_start_date": "DATE", "valid_end_date": "DATE", "invalid_reason": "VARCHAR(1)"}


class ConceptAncestorObject(OHDSI54OutputClass):
    def table_name(self):
        return "concept_ancestor"

    def _fields(self):
        return ["ancestor_concept_id", "descendant_concept_id", "min_levels_of_separation", "max_levels_of_separation"]

    def _data_types(self):
        return {"ancestor_concept_id": "INTEGER", "descendant_concept_id": "INTEGER", "min_levels_of_separation": "INTEGER", "max_levels_of_separation": "INTEGER"}


class ConceptClassObject(OHDSI54OutputClass):
    def table_name(self):
        return "concept_class"

    def _fields(self):
        return ["concept_class_id", "concept_class_name", "concept_class_concept_id"]

    def _data_types(self):
        return {"concept_class_id": "VARCHAR(20)", "concept_class_name": "VARCHAR(255)", "concept_class_concept_id": "INTEGER"}


class ConceptRelationshipObject(OHDSI54OutputClass):
    def table_name(self):
        return "concept_relationship"

    def _fields(self):
        return ["concept_id_1", "concept_id_2", "relationship_id", "valid_start_date", "valid_end_date", "invalid_reason"]

    def _data_types(self):
        return {"concept_id_1": "INTEGER", "concept_id_2": "INTEGER", "relationship_id": "VARCHAR(20)", "valid_start_date": "DATE", "valid_end_date": "DATE", "invalid_reason": "VARCHAR(1)"}


class ConceptSynonymObject(OHDSI54OutputClass):
    def table_name(self):
        return "concept_synonym"

    def _fields(self):
        return ["concept_id", "concept_synonym_name", "language_concept_id"]

    def _data_types(self):
        return {"concept_id": "INTEGER", "concept_synonym_name": "VARCHAR(1000)", "language_concept_id": "INTEGER"}


class ConditionEraObject(OHDSI54OutputClass):
    def table_name(self):
        return "condition_era"

    def _fields(self):
        return ["condition_era_id", "person_id", "condition_concept_id", "condition_era_start_date", "condition_era_end_date", "condition_occurrence_count"]

    def _data_types(self):
        return {"condition_era_id": "INTEGER", "person_id": "INTEGER", "condition_concept_id": "INTEGER", "condition_era_start_date": "TIMESTAMP", "condition_era_end_date": "TIMESTAMP", "condition_occurrence_count": "INTEGER"}


class ConditionOccurrenceObject(OHDSI54OutputClass):
    def table_name(self):
        return "condition_occurrence"

    def _fields(self):
        return ["condition_occurrence_id", "person_id", "condition_concept_id", "condition_start_date", "condition_start_datetime", "condition_end_date", "condition_end_datetime", "condition_type_concept_id", "condition_status_concept_id", "stop_reason", "provider_id", "visit_occurrence_id", "visit_detail_id", "condition_source_value", "condition_source_concept_id", "condition_status_source_value"]

    def _data_types(self):
        return {"condition_occurrence_id": "INTEGER", "person_id": "INTEGER", "condition_concept_id": "INTEGER", "condition_start_date": "DATE", "condition_start_datetime": "TIMESTAMP", "condition_end_date": "DATE", "condition_end_datetime": "TIMESTAMP", "condition_type_concept_id": "INTEGER", "condition_status_concept_id": "INTEGER", "stop_reason": "VARCHAR(20)", "provider_id": "INTEGER", "visit_occurrence_id": "INTEGER", "visit_detail_id": "INTEGER", "condition_source_value": "VARCHAR(50)", "condition_source_concept_id": "INTEGER", "condition_status_source_value": "VARCHAR(50)"}


class CostObject(OHDSI54OutputClass):
    def table_name(self):
        return "cost"

    def _fields(self):
        return ["cost_id", "cost_event_id", "cost_domain_id", "cost_type_concept_id", "currency_concept_id", "total_charge", "total_cost", "total_paid", "paid_by_payer", "paid_by_patient", "paid_patient_copay", "paid_patient_coinsurance", "paid_patient_deductible", "paid_by_primary", "paid_ingredient_cost", "paid_dispensing_fee", "payer_plan_period_id", "amount_allowed", "revenue_code_concept_id", "revenue_code_source_value", "drg_concept_id", "drg_source_value"]

    def _data_types(self):
        return {"cost_id": "INTEGER", "cost_event_id": "INTEGER", "cost_domain_id": "VARCHAR(20)", "cost_type_concept_id": "INTEGER", "currency_concept_id": "INTEGER", "total_charge": "NUMERIC", "total_cost": "NUMERIC", "total_paid": "NUMERIC", "paid_by_payer": "NUMERIC", "paid_by_patient": "NUMERIC", "paid_patient_copay": "NUMERIC", "paid_patient_coinsurance": "NUMERIC", "paid_patient_deductible": "NUMERIC", "paid_by_primary": "NUMERIC", "paid_ingredient_cost": "NUMERIC", "paid_dispensing_fee": "NUMERIC", "payer_plan_period_id": "INTEGER", "amount_allowed": "NUMERIC", "revenue_code_concept_id": "INTEGER", "revenue_code_source_value": "VARCHAR(50)", "drg_concept_id": "INTEGER", "drg_source_value": "VARCHAR(3)"}


class DeathObject(OHDSI54OutputClass):
    def table_name(self):
        return "death"

    def _fields(self):
        return ["person_id", "death_date", "death_datetime", "death_type_concept_id", "cause_concept_id", "cause_source_value", "cause_source_concept_id"]

    def _data_types(self):
        return {"person_id": "INTEGER", "death_date": "DATE", "death_datetime": "TIMESTAMP", "death_type_concept_id": "INTEGER", "cause_concept_id": "INTEGER", "cause_source_value": "VARCHAR(50)", "cause_source_concept_id": "INTEGER"}


class DeviceExposureObject(OHDSI54OutputClass):
    def table_name(self):
        return "device_exposure"

    def _fields(self):
        return ["device_exposure_id", "person_id", "device_concept_id", "device_exposure_start_date", "device_exposure_start_datetime", "device_exposure_end_date", "device_exposure_end_datetime", "device_type_concept_id", "unique_device_id", "production_id", "quantity", "provider_id", "visit_occurrence_id", "visit_detail_id", "device_source_value", "device_source_concept_id", "unit_concept_id", "unit_source_value", "unit_source_concept_id"]

    def _data_types(self):
        return {"device_exposure_id": "INTEGER", "person_id": "INTEGER", "device_concept_id": "INTEGER", "device_exposure_start_date": "DATE", "device_exposure_start_datetime": "TIMESTAMP", "device_exposure_end_date": "DATE", "device_exposure_end_datetime": "TIMESTAMP", "device_type_concept_id": "INTEGER", "unique_device_id": "VARCHAR(255)", "production_id": "VARCHAR(255)", "quantity": "INTEGER", "provider_id": "INTEGER", "visit_occurrence_id": "INTEGER", "visit_detail_id": "INTEGER", "device_source_value": "VARCHAR(50)", "device_source_concept_id": "INTEGER", "unit_concept_id": "INTEGER", "unit_source_value": "VARCHAR(50)", "unit_source_concept_id": "INTEGER"}


class DomainObject(OHDSI54OutputClass):
    def table_name(self):
        return "domain"

    def _fields(self):
        return ["domain_id", "domain_name", "domain_concept_id"]

    def _data_types(self):
        return {"domain_id": "VARCHAR(20)", "domain_name": "VARCHAR(255)", "domain_concept_id": "INTEGER"}


class DoseEraObject(OHDSI54OutputClass):
    def table_name(self):
        return "dose_era"

    def _fields(self):
        return ["dose_era_id", "person_id", "drug_concept_id", "unit_concept_id", "dose_value", "dose_era_start_date", "dose_era_end_date"]

    def _data_types(self):
        return {"dose_era_id": "INTEGER", "person_id": "INTEGER", "drug_concept_id": "INTEGER", "unit_concept_id": "INTEGER", "dose_value": "NUMERIC", "dose_era_start_date": "TIMESTAMP", "dose_era_end_date": "TIMESTAMP"}


class DrugEraObject(OHDSI54OutputClass):
    def table_name(self):
        return "drug_era"

    def _fields(self):
        return ["drug_era_id", "person_id", "drug_concept_id", "drug_era_start_date", "drug_era_end_date", "drug_exposure_count", "gap_days"]

    def _data_types(self):
        return {"drug_era_id": "INTEGER", "person_id": "INTEGER", "drug_concept_id": "INTEGER", "drug_era_start_date": "TIMESTAMP", "drug_era_end_date": "TIMESTAMP", "drug_exposure_count": "INTEGER", "gap_days": "INTEGER"}


class DrugExposureObject(OHDSI54OutputClass):
    def table_name(self):
        return "drug_exposure"

    def _fields(self):
        return ["drug_exposure_id", "person_id", "drug_concept_id", "drug_exposure_start_date", "drug_exposure_start_datetime", "drug_exposure_end_date", "drug_exposure_end_datetime", "verbatim_end_date", "drug_type_concept_id", "stop_reason", "refills", "quantity", "days_supply", "sig", "route_concept_id", "lot_number", "provider_id", "visit_occurrence_id", "visit_detail_id", "drug_source_value", "drug_source_concept_id", "route_source_value", "dose_unit_source_value"]

    def _data_types(self):
        return {"drug_exposure_id": "INTEGER", "person_id": "INTEGER", "drug_concept_id": "INTEGER", "drug_exposure_start_date": "DATE", "drug_exposure_start_datetime": "TIMESTAMP", "drug_exposure_end_date": "DATE", "drug_exposure_end_datetime": "TIMESTAMP", "verbatim_end_date": "DATE", "drug_type_concept_id": "INTEGER", "stop_reason": "VARCHAR(20)", "refills": "INTEGER", "quantity": "NUMERIC", "days_supply": "INTEGER", "sig": "STRING", "route_concept_id": "INTEGER", "lot_number": "VARCHAR(50)", "provider_id": "INTEGER", "visit_occurrence_id": "INTEGER", "visit_detail_id": "INTEGER", "drug_source_value": "VARCHAR(50)", "drug_source_concept_id": "INTEGER", "route_source_value": "VARCHAR(50)", "dose_unit_source_value": "VARCHAR(50)"}


class DrugStrengthObject(OHDSI54OutputClass):
    def table_name(self):
        return "drug_strength"

    def _fields(self):
        return ["drug_concept_id", "ingredient_concept_id", "amount_value", "amount_unit_concept_id", "numerator_value", "numerator_unit_concept_id", "denominator_value", "denominator_unit_concept_id", "box_size", "valid_start_date", "valid_end_date", "invalid_reason"]

    def _data_types(self):
        return {"drug_concept_id": "INTEGER", "ingredient_concept_id": "INTEGER", "amount_value": "NUMERIC", "amount_unit_concept_id": "INTEGER", "numerator_value": "NUMERIC", "numerator_unit_concept_id": "INTEGER", "denominator_value": "NUMERIC", "denominator_unit_concept_id": "INTEGER", "box_size": "INTEGER", "valid_start_date": "DATE", "valid_end_date": "DATE", "invalid_reason": "VARCHAR(1)"}


class EpisodeObject(OHDSI54OutputClass):
    def table_name(self):
        return "episode"

    def _fields(self):
        return ["episode_id", "person_id", "episode_concept_id", "episode_start_date", "episode_start_datetime", "episode_end_date", "episode_end_datetime", "episode_parent_id", "episode_number", "episode_object_concept_id", "episode_type_concept_id", "episode_source_value", "episode_source_concept_id"]

    def _data_types(self):
        return {"episode_id": "BIGINT", "person_id": "BIGINT", "episode_concept_id": "INTEGER", "episode_start_date": "DATE", "episode_start_datetime": "TIMESTAMP", "episode_end_date": "DATE", "episode_end_datetime": "TIMESTAMP", "episode_parent_id": "BIGINT", "episode_number": "INTEGER", "episode_object_concept_id": "INTEGER", "episode_type_concept_id": "INTEGER", "episode_source_value": "VARCHAR(50)", "episode_source_concept_id": "INTEGER"}


class EpisodeEventObject(OHDSI54OutputClass):
    def table_name(self):
        return "episode_event"

    def _fields(self):
        return ["episode_id", "event_id", "episode_event_field_concept_id"]

    def _data_types(self):
        return {"episode_id": "BIGINT", "event_id": "BIGINT", "episode_event_field_concept_id": "INTEGER"}


class FactRelationshipObject(OHDSI54OutputClass):
    def table_name(self):
        return "fact_relationship"

    def _fields(self):
        return ["domain_concept_id_1", "fact_id_1", "domain_concept_id_2", "fact_id_2", "relationship_concept_id"]

    def _data_types(self):
        return {"domain_concept_id_1": "INTEGER", "fact_id_1": "INTEGER", "domain_concept_id_2": "INTEGER", "fact_id_2": "INTEGER", "relationship_concept_id": "INTEGER"}


class LocationObject(OHDSI54OutputClass):
    def table_name(self):
        return "location"

    def _fields(self):
        return ["location_id", "address_1", "address_2", "city", "state", "zip", "county", "location_source_value", "country_concept_id", "country_source_value", "latitude", "longitude"]

    def _data_types(self):
        return {"location_id": "INTEGER", "address_1": "VARCHAR(50)", "address_2": "VARCHAR(50)", "city": "VARCHAR(50)", "state": "VARCHAR(2)", "zip": "VARCHAR(9)", "county": "VARCHAR(20)", "location_source_value": "VARCHAR(50)", "country_concept_id": "INTEGER", "country_source_value": "VARCHAR(80)", "latitude": "NUMERIC", "longitude": "NUMERIC"}


class MeasurementObject(OHDSI54OutputClass):
    def table_name(self):
        return "measurement"

    def _fields(self):
        return ["measurement_id", "person_id", "measurement_concept_id", "measurement_date", "measurement_datetime", "measurement_time", "measurement_type_concept_id", "operator_concept_id", "value_as_number", "value_as_concept_id", "unit_concept_id", "range_low", "range_high", "provider_id", "visit_occurrence_id", "visit_detail_id", "measurement_source_value", "measurement_source_concept_id", "unit_source_value", "unit_source_concept_id", "value_source_value", "measurement_event_id", "meas_event_field_concept_id"]

    def _data_types(self):
        return {"measurement_id": "INTEGER", "person_id": "INTEGER", "measurement_concept_id": "INTEGER", "measurement_date": "DATE", "measurement_datetime": "TIMESTAMP", "measurement_time": "VARCHAR(10)", "measurement_type_concept_id": "INTEGER", "operator_concept_id": "INTEGER", "value_as_number": "NUMERIC", "value_as_concept_id": "INTEGER", "unit_concept_id": "INTEGER", "range_low": "NUMERIC", "range_high": "NUMERIC", "provider_id": "INTEGER", "visit_occurrence_id": "INTEGER", "visit_detail_id": "INTEGER", "measurement_source_value": "VARCHAR(50)", "measurement_source_concept_id": "INTEGER", "unit_source_value": "VARCHAR(50)", "unit_source_concept_id": "INTEGER", "value_source_value": "VARCHAR(50)", "measurement_event_id": "BIGINT", "meas_event_field_concept_id": "INTEGER"}


class MetadataObject(OHDSI54OutputClass):
    def table_name(self):
        return "metadata"

    def _fields(self):
        return ["metadata_id", "metadata_concept_id", "metadata_type_concept_id", "name", "value_as_string", "value_as_concept_id", "value_as_number", "metadata_date", "metadata_datetime"]

    def _data_types(self):
        return {"metadata_id": "INTEGER", "metadata_concept_id": "INTEGER", "metadata_type_concept_id": "INTEGER", "name": "VARCHAR(250)", "value_as_string": "VARCHAR(250)", "value_as_concept_id": "INTEGER", "value_as_number": "NUMERIC", "metadata_date": "DATE", "metadata_datetime": "TIMESTAMP"}


class NoteObject(OHDSI54OutputClass):
    def table_name(self):
        return "note"

    def _fields(self):
        return ["note_id", "person_id", "note_date", "note_datetime", "note_type_concept_id", "note_class_concept_id", "note_title", "note_text", "encoding_concept_id", "language_concept_id", "provider_id", "visit_occurrence_id", "visit_detail_id", "note_source_value", "note_event_id", "note_event_field_concept_id"]

    def _data_types(self):
        return {"note_id": "INTEGER", "person_id": "INTEGER", "note_date": "DATE", "note_datetime": "TIMESTAMP", "note_type_concept_id": "INTEGER", "note_class_concept_id": "INTEGER", "note_title": "VARCHAR(250)", "note_text": "STRING", "encoding_concept_id": "INTEGER", "language_concept_id": "INTEGER", "provider_id": "INTEGER", "visit_occurrence_id": "INTEGER", "visit_detail_id": "INTEGER", "note_source_value": "VARCHAR(50)", "note_event_id": "BIGINT", "note_event_field_concept_id": "INTEGER"}


class NoteNlpObject(OHDSI54OutputClass):
    def table_name(self):
        return "note_nlp"

    def _fields(self):
        return ["note_nlp_id", "note_id", "section_concept_id", "snippet", "offset", "lexical_variant", "note_nlp_concept_id", "note_nlp_source_concept_id", "nlp_system", "nlp_date", "nlp_datetime", "term_exists", "term_temporal", "term_modifiers"]

    def _data_types(self):
        return {"note_nlp_id": "INTEGER", "note_id": "INTEGER", "section_concept_id": "INTEGER", "snippet": "VARCHAR(250)", "offset": "VARCHAR(50)", "lexical_variant": "VARCHAR(250)", "note_nlp_concept_id": "INTEGER", "note_nlp_source_concept_id": "INTEGER", "nlp_system": "VARCHAR(250)", "nlp_date": "DATE", "nlp_datetime": "TIMESTAMP", "term_exists": "VARCHAR(1)", "term_temporal": "VARCHAR(50)", "term_modifiers": "VARCHAR(2000)"}


class ObservationObject(OHDSI54OutputClass):
    def table_name(self):
        return "observation"

    def _fields(self):
        return ["observation_id", "person_id", "observation_concept_id", "observation_date", "observation_datetime", "observation_type_concept_id", "value_as_number", "value_as_string", "value_as_concept_id", "qualifier_concept_id", "unit_concept_id", "provider_id", "visit_occurrence_id", "visit_detail_id", "observation_source_value", "observation_source_concept_id", "unit_source_value", "qualifier_source_value", "value_source_value", "observation_event_id", "obs_event_field_concept_id"]

    def _data_types(self):
        return {"observation_id": "INTEGER", "person_id": "INTEGER", "observation_concept_id": "INTEGER", "observation_date": "DATE", "observation_datetime": "TIMESTAMP", "observation_type_concept_id": "INTEGER", "value_as_number": "NUMERIC", "value_as_string": "VARCHAR(60)", "value_as_concept_id": "INTEGER", "qualifier_concept_id": "INTEGER", "unit_concept_id": "INTEGER", "provider_id": "INTEGER", "visit_occurrence_id": "INTEGER", "visit_detail_id": "INTEGER", "observation_source_value": "VARCHAR(50)", "observation_source_concept_id": "INTEGER", "unit_source_value": "VARCHAR(50)", "qualifier_source_value": "VARCHAR(50)", "value_source_value": "VARCHAR(50)", "observation_event_id": "BIGINT", "obs_event_field_concept_id": "INTEGER"}


class ObservationPeriodObject(OHDSI54OutputClass):
    def table_name(self):
        return "observation_period"

    def _fields(self):
        return ["observation_period_id", "person_id", "observation_period_start_date", "observation_period_end_date", "period_type_concept_id"]

    def _data_types(self):
        return {"observation_period_id": "INTEGER", "person_id": "INTEGER", "observation_period_start_date": "DATE", "observation_period_end_date": "DATE", "period_type_concept_id": "INTEGER"}


class PayerPlanPeriodObject(OHDSI54OutputClass):
    def table_name(self):
        return "payer_plan_period"

    def _fields(self):
        return ["payer_plan_period_id", "person_id", "payer_plan_period_start_date", "payer_plan_period_end_date", "payer_concept_id", "payer_source_value", "payer_source_concept_id", "plan_concept_id", "plan_source_value", "plan_source_concept_id", "sponsor_concept_id", "sponsor_source_value", "sponsor_source_concept_id", "family_source_value", "stop_reason_concept_id", "stop_reason_source_value", "stop_reason_source_concept_id"]

    def _data_types(self):
        return {"payer_plan_period_id": "INTEGER", "person_id": "INTEGER", "payer_plan_period_start_date": "DATE", "payer_plan_period_end_date": "DATE", "payer_concept_id": "INTEGER", "payer_source_value": "VARCHAR(50)", "payer_source_concept_id": "INTEGER", "plan_concept_id": "INTEGER", "plan_source_value": "VARCHAR(50)", "plan_source_concept_id": "INTEGER", "sponsor_concept_id": "INTEGER", "sponsor_source_value": "VARCHAR(50)", "sponsor_source_concept_id": "INTEGER", "family_source_value": "VARCHAR(50)", "stop_reason_concept_id": "INTEGER", "stop_reason_source_value": "VARCHAR(50)", "stop_reason_source_concept_id": "INTEGER"}


class PersonObject(OHDSI54OutputClass):
    def table_name(self):
        return "person"

    def _fields(self):
        return ["person_id", "gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime", "race_concept_id", "ethnicity_concept_id", "location_id", "provider_id", "care_site_id", "person_source_value", "gender_source_value", "gender_source_concept_id", "race_source_value", "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"]

    def _data_types(self):
        return {"person_id": "INTEGER", "gender_concept_id": "INTEGER", "year_of_birth": "INTEGER", "month_of_birth": "INTEGER", "day_of_birth": "INTEGER", "birth_datetime": "TIMESTAMP", "race_concept_id": "INTEGER", "ethnicity_concept_id": "INTEGER", "location_id": "INTEGER", "provider_id": "INTEGER", "care_site_id": "INTEGER", "person_source_value": "VARCHAR(50)", "gender_source_value": "VARCHAR(50)", "gender_source_concept_id": "INTEGER", "race_source_value": "VARCHAR(50)", "race_source_concept_id": "INTEGER", "ethnicity_source_value": "VARCHAR(50)", "ethnicity_source_concept_id": "INTEGER"}


class ProcedureOccurrenceObject(OHDSI54OutputClass):
    def table_name(self):
        return "procedure_occurrence"

    def _fields(self):
        return ["procedure_occurrence_id", "person_id", "procedure_concept_id", "procedure_date", "procedure_datetime", "procedure_end_date", "procedure_end_datetime", "procedure_type_concept_id", "modifier_concept_id", "quantity", "provider_id", "visit_occurrence_id", "visit_detail_id", "procedure_source_value", "procedure_source_concept_id", "modifier_source_value"]

    def _data_types(self):
        return {"procedure_occurrence_id": "INTEGER", "person_id": "INTEGER", "procedure_concept_id": "INTEGER", "procedure_date": "DATE", "procedure_datetime": "TIMESTAMP", "procedure_end_date": "DATE", "procedure_end_datetime": "TIMESTAMP", "procedure_type_concept_id": "INTEGER", "modifier_concept_id": "INTEGER", "quantity": "INTEGER", "provider_id": "INTEGER", "visit_occurrence_id": "INTEGER", "visit_detail_id": "INTEGER", "procedure_source_value": "VARCHAR(50)", "procedure_source_concept_id": "INTEGER", "modifier_source_value": "VARCHAR(50)"}


class ProviderObject(OHDSI54OutputClass):
    def table_name(self):
        return "provider"

    def _fields(self):
        return ["provider_id", "provider_name", "npi", "dea", "specialty_concept_id", "care_site_id", "year_of_birth", "gender_concept_id", "provider_source_value", "specialty_source_value", "specialty_source_concept_id", "gender_source_value", "gender_source_concept_id"]

    def _data_types(self):
        return {"provider_id": "INTEGER", "provider_name": "VARCHAR(255)", "npi": "VARCHAR(20)", "dea": "VARCHAR(20)", "specialty_concept_id": "INTEGER", "care_site_id": "INTEGER", "year_of_birth": "INTEGER", "gender_concept_id": "INTEGER", "provider_source_value": "VARCHAR(50)", "specialty_source_value": "VARCHAR(50)", "specialty_source_concept_id": "INTEGER", "gender_source_value": "VARCHAR(50)", "gender_source_concept_id": "INTEGER"}


class RelationshipObject(OHDSI54OutputClass):
    def table_name(self):
        return "relationship"

    def _fields(self):
        return ["relationship_id", "relationship_name", "is_hierarchical", "defines_ancestry", "reverse_relationship_id", "relationship_concept_id"]

    def _data_types(self):
        return {"relationship_id": "VARCHAR(20)", "relationship_name": "VARCHAR(255)", "is_hierarchical": "VARCHAR(1)", "defines_ancestry": "VARCHAR(1)", "reverse_relationship_id": "VARCHAR(20)", "relationship_concept_id": "INTEGER"}


class SourceToConceptMapObject(OHDSI54OutputClass):
    def table_name(self):
        return "source_to_concept_map"

    def _fields(self):
        return ["source_code", "source_concept_id", "source_vocabulary_id", "source_code_description", "target_concept_id", "target_vocabulary_id", "valid_start_date", "valid_end_date", "invalid_reason"]

    def _data_types(self):
        return {"source_code": "VARCHAR(50)", "source_concept_id": "INTEGER", "source_vocabulary_id": "VARCHAR(20)", "source_code_description": "VARCHAR(255)", "target_concept_id": "INTEGER", "target_vocabulary_id": "VARCHAR(20)", "valid_start_date": "DATE", "valid_end_date": "DATE", "invalid_reason": "VARCHAR(1)"}


class SpecimenObject(OHDSI54OutputClass):
    def table_name(self):
        return "specimen"

    def _fields(self):
        return ["specimen_id", "person_id", "specimen_concept_id", "specimen_type_concept_id", "specimen_date", "specimen_datetime", "quantity", "unit_concept_id", "anatomic_site_concept_id", "disease_status_concept_id", "specimen_source_id", "specimen_source_value", "unit_source_value", "anatomic_site_source_value", "disease_status_source_value"]

    def _data_types(self):
        return {"specimen_id": "INTEGER", "person_id": "INTEGER", "specimen_concept_id": "INTEGER", "specimen_type_concept_id": "INTEGER", "specimen_date": "DATE", "specimen_datetime": "TIMESTAMP", "quantity": "NUMERIC", "unit_concept_id": "INTEGER", "anatomic_site_concept_id": "INTEGER", "disease_status_concept_id": "INTEGER", "specimen_source_id": "VARCHAR(50)", "specimen_source_value": "VARCHAR(50)", "unit_source_value": "VARCHAR(50)", "anatomic_site_source_value": "VARCHAR(50)", "disease_status_source_value": "VARCHAR(50)"}


class VisitDetailObject(OHDSI54OutputClass):
    def table_name(self):
        return "visit_detail"

    def _fields(self):
        return ["visit_detail_id", "person_id", "visit_detail_concept_id", "visit_detail_start_date", "visit_detail_start_datetime", "visit_detail_end_date", "visit_detail_end_datetime", "visit_detail_type_concept_id", "provider_id", "care_site_id", "visit_detail_source_value", "visit_detail_source_concept_id", "admitted_from_concept_id", "admitted_from_source_value", "discharged_to_source_value", "discharged_to_concept_id", "preceding_visit_detail_id", "parent_visit_detail_id", "visit_occurrence_id"]

    def _data_types(self):
        return {"visit_detail_id": "INTEGER", "person_id": "INTEGER", "visit_detail_concept_id": "INTEGER", "visit_detail_start_date": "DATE", "visit_detail_start_datetime": "TIMESTAMP", "visit_detail_end_date": "DATE", "visit_detail_end_datetime": "TIMESTAMP", "visit_detail_type_concept_id": "INTEGER", "provider_id": "INTEGER", "care_site_id": "INTEGER", "visit_detail_source_value": "VARCHAR(50)", "visit_detail_source_concept_id": "INTEGER", "admitted_from_concept_id": "INTEGER", "admitted_from_source_value": "VARCHAR(50)", "discharged_to_source_value": "VARCHAR(50)", "discharged_to_concept_id": "INTEGER", "preceding_visit_detail_id": "INTEGER", "parent_visit_detail_id": "INTEGER", "visit_occurrence_id": "INTEGER"}


class VisitOccurrenceObject(OHDSI54OutputClass):
    def table_name(self):
        return "visit_occurrence"

    def _fields(self):
        return ["visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date", "visit_start_datetime", "visit_end_date", "visit_end_datetime", "visit_type_concept_id", "provider_id", "care_site_id", "visit_source_value", "visit_source_concept_id", "admitted_from_concept_id", "admitted_from_source_value", "discharged_to_concept_id", "discharged_to_source_value", "preceding_visit_occurrence_id"]

    def _data_types(self):
        return {"visit_occurrence_id": "INTEGER", "person_id": "INTEGER", "visit_concept_id": "INTEGER", "visit_start_date": "DATE", "visit_start_datetime": "TIMESTAMP", "visit_end_date": "DATE", "visit_end_datetime": "TIMESTAMP", "visit_type_concept_id": "INTEGER", "provider_id": "INTEGER", "care_site_id": "INTEGER", "visit_source_value": "VARCHAR(50)", "visit_source_concept_id": "INTEGER", "admitted_from_concept_id": "INTEGER", "admitted_from_source_value": "VARCHAR(50)", "discharged_to_concept_id": "INTEGER", "discharged_to_source_value": "VARCHAR(50)", "preceding_visit_occurrence_id": "INTEGER"}


class VocabularyObject(OHDSI54OutputClass):
    def table_name(self):
        return "vocabulary"

    def _fields(self):
        return ["vocabulary_id", "vocabulary_name", "vocabulary_reference", "vocabulary_version", "vocabulary_concept_id"]

    def _data_types(self):
        return {"vocabulary_id": "VARCHAR(20)", "vocabulary_name": "VARCHAR(255)", "vocabulary_reference": "VARCHAR(255)", "vocabulary_version": "VARCHAR(255)", "vocabulary_concept_id": "INTEGER"}



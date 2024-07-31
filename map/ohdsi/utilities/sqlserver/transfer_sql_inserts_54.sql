/* Script to load transferTables to a standard OHDSI CDM */


--Truncate tables
truncate table [dbo].[PAYER_PLAN_PERIOD];
truncate table [dbo].[DEVICE_EXPOSURE];
truncate table [dbo].[DRUG_EXPOSURE];
truncate table [dbo].[OBSERVATION];
truncate table [dbo].[MEASUREMENT];
truncate table [dbo].[PROCEDURE_OCCURRENCE];
truncate table [dbo].[CONDITION_OCCURRENCE];
truncate table [dbo].[VISIT_DETAIL];
truncate table [dbo].[VISIT_OCCURRENCE];
truncate table [dbo].[OBSERVATION_PERIOD];
truncate table [dbo].[DEATH];
truncate table [dbo].[PERSON];
truncate table [dbo].[PROVIDER];
truncate table [dbo].[CARE_SITE];
truncate table [dbo].[LOCATION];
truncate table [dbo].[DRUG_STRENGTH];
truncate table [dbo].[CONCEPT_RELATIONSHIP];
truncate table [dbo].[CONCEPT_ANCESTOR];
truncate table [dbo].[VOCABULARY];
truncate table [dbo].[CONCEPT];
truncate table [dbo].[DOMAIN];

--Alter table domain

insert into [dbo].[DOMAIN] (domain_id,domain_name,domain_concept_id)
select [domain_id],[domain_name],[domain_concept_id] 
    from [dbo].[transferDOMAIN];


--Alter table concept

insert into [dbo].[CONCEPT] (concept_id,concept_name,domain_id,vocabulary_id,concept_class_id,standard_concept,concept_code,valid_start_date,valid_end_date,invalid_reason)
select [concept_id],[concept_name],[domain_id],[vocabulary_id],[concept_class_id],[standard_concept],[concept_code],cast(cast(valid_start_date as varchar(8)) as date),cast(cast(valid_start_date as varchar(8)) as date),[invalid_reason] 
    from [dbo].[transferCONCEPT];


--Alter table vocabulary

insert into [dbo].[VOCABULARY] (vocabulary_id,vocabulary_name,vocabulary_reference,vocabulary_version,vocabulary_concept_id)
select [vocabulary_id],[vocabulary_name],[vocabulary_reference],[vocabulary_version],[vocabulary_concept_id] 
    from [dbo].[transferVOCABULARY];


--Alter table concept_ancestor

insert into [dbo].[CONCEPT_ANCESTOR] (ancestor_concept_id,descendant_concept_id,min_levels_of_separation,max_levels_of_separation)
select [ancestor_concept_id],[descendant_concept_id],[min_levels_of_separation],[max_levels_of_separation] 
    from [dbo].[transferCONCEPT_ANCESTOR];


--Alter table concept_relationship

insert into [dbo].[CONCEPT_RELATIONSHIP] (concept_id_1,concept_id_2,relationship_id,valid_start_date,valid_end_date,invalid_reason)
select [concept_id_1],[concept_id_2],[relationship_id],cast(cast(valid_start_date as varchar(8)) as date),cast(cast(valid_start_date as varchar(8)) as date),[invalid_reason] 
    from [dbo].[transferCONCEPT_RELATIONSHIP];


--Alter table drug_strength

insert into [dbo].[DRUG_STRENGTH] (drug_concept_id,ingredient_concept_id,amount_value,amount_unit_concept_id,numerator_value,numerator_unit_concept_id,denominator_value,denominator_unit_concept_id,box_size,valid_start_date,valid_end_date,invalid_reason)
select [drug_concept_id],[ingredient_concept_id],[amount_value],[amount_unit_concept_id],[numerator_value],[numerator_unit_concept_id],[denominator_value],[denominator_unit_concept_id],[box_size],cast(cast(valid_start_date as varchar(8)) as date),cast(cast(valid_start_date as varchar(8)) as date),[invalid_reason] 
    from [dbo].[transferDRUG_STRENGTH];


--Alter table location
alter table [dbo].[LOCATION] alter column [state] VARCHAR(512);
alter table [dbo].[LOCATION] alter column [zip] VARCHAR(512);

insert into [dbo].[LOCATION] (location_id,address_1,address_2,city,state,zip,county,location_source_value,country_concept_id,country_source_value,latitude,longitude)
select [location_id],[address_1],[address_2],[city],[state],[zip],[county],[location_source_value],[country_concept_id],[country_source_value],[latitude],[longitude] 
    from [dbo].[transferLOCATION];


--Alter table care_site

insert into [dbo].[CARE_SITE] (care_site_id,care_site_name,place_of_service_concept_id,location_id,care_site_source_value,place_of_service_source_value)
select [care_site_id],[care_site_name],[place_of_service_concept_id],[location_id],[care_site_source_value],[place_of_service_source_value] 
    from [dbo].[transferCARE_SITE];


--Alter table provider
alter table [dbo].[PROVIDER] alter column [provider_id] BIGINT;

insert into [dbo].[PROVIDER] (provider_id,provider_name,npi,dea,specialty_concept_id,care_site_id,year_of_birth,gender_concept_id,provider_source_value,specialty_source_value,specialty_source_concept_id,gender_source_value,gender_source_concept_id)
select [provider_id],[provider_name],[npi],[dea],[specialty_concept_id],[care_site_id],[year_of_birth],[gender_concept_id],[provider_source_value],[specialty_source_value],[specialty_source_concept_id],[gender_source_value],[gender_source_concept_id] 
    from [dbo].[transferPROVIDER];


--Alter table person
alter table [dbo].[PERSON] alter column [person_id] BIGINT;
alter table [dbo].[PERSON] alter column [provider_id] BIGINT;
alter table [dbo].[PERSON] alter column [race_source_value] VARCHAR(512);
alter table [dbo].[PERSON] alter column [ethnicity_source_value] VARCHAR(512);

insert into [dbo].[PERSON] (person_id,gender_concept_id,year_of_birth,month_of_birth,day_of_birth,birth_datetime,race_concept_id,ethnicity_concept_id,location_id,provider_id,care_site_id,person_source_value,gender_source_value,gender_source_concept_id,race_source_value,race_source_concept_id,ethnicity_source_value,ethnicity_source_concept_id)
select [person_id],[gender_concept_id],[year_of_birth],[month_of_birth],[day_of_birth],[birth_datetime],[race_concept_id],[ethnicity_concept_id],[location_id],[provider_id],[care_site_id],[person_source_value],[gender_source_value],[gender_source_concept_id],[race_source_value],[race_source_concept_id],[ethnicity_source_value],[ethnicity_source_concept_id] 
    from [dbo].[transferPERSON];


--Alter table death
alter table [dbo].[DEATH] alter column [person_id] BIGINT;

insert into [dbo].[DEATH] (person_id,death_date,death_datetime,death_type_concept_id,cause_concept_id,cause_source_value,cause_source_concept_id)
select [person_id],[death_date],[death_datetime],[death_type_concept_id],[cause_concept_id],[cause_source_value],[cause_source_concept_id] 
    from [dbo].[transferDEATH];


--Alter table observation_period
alter table [dbo].[OBSERVATION_PERIOD] alter column [observation_period_id] BIGINT;
alter table [dbo].[OBSERVATION_PERIOD] alter column [person_id] BIGINT;

insert into [dbo].[OBSERVATION_PERIOD] (observation_period_id,person_id,observation_period_start_date,observation_period_end_date,period_type_concept_id)
select [observation_period_id],[person_id],[observation_period_start_date],[observation_period_end_date],[period_type_concept_id] 
    from [dbo].[transferOBSERVATION_PERIOD];


--Alter table visit_occurrence
alter table [dbo].[VISIT_OCCURRENCE] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[VISIT_OCCURRENCE] alter column [person_id] BIGINT;
alter table [dbo].[VISIT_OCCURRENCE] alter column [provider_id] BIGINT;
alter table [dbo].[VISIT_OCCURRENCE] alter column [admitted_from_source_value] VARCHAR(512);
alter table [dbo].[VISIT_OCCURRENCE] alter column [discharged_to_source_value] VARCHAR(512);

insert into [dbo].[VISIT_OCCURRENCE] (visit_occurrence_id,person_id,visit_concept_id,visit_start_date,visit_start_datetime,visit_end_date,visit_end_datetime,visit_type_concept_id,provider_id,care_site_id,visit_source_value,visit_source_concept_id,admitted_from_concept_id,admitted_from_source_value,discharged_to_concept_id,discharged_to_source_value,preceding_visit_occurrence_id)
select [visit_occurrence_id],[person_id],[visit_concept_id],[visit_start_date],[visit_start_datetime],[visit_end_date],[visit_end_datetime],[visit_type_concept_id],[provider_id],[care_site_id],[visit_source_value],[visit_source_concept_id],[admitted_from_concept_id],[admitted_from_source_value],[discharged_to_concept_id],[discharged_to_source_value],[preceding_visit_occurrence_id] 
    from [dbo].[transferVISIT_OCCURRENCE];


--Alter table visit_detail
alter table [dbo].[VISIT_DETAIL] alter column [visit_detail_id] BIGINT;
alter table [dbo].[VISIT_DETAIL] alter column [person_id] BIGINT;
alter table [dbo].[VISIT_DETAIL] alter column [provider_id] BIGINT;
alter table [dbo].[VISIT_DETAIL] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[VISIT_DETAIL] alter column [admitted_from_source_value] VARCHAR(512);
alter table [dbo].[VISIT_DETAIL] alter column [discharged_to_source_value] VARCHAR(512);

insert into [dbo].[VISIT_DETAIL] (visit_detail_id,person_id,visit_detail_concept_id,visit_detail_start_date,visit_detail_start_datetime,visit_detail_end_date,visit_detail_end_datetime,visit_detail_type_concept_id,provider_id,care_site_id,visit_detail_source_value,visit_detail_source_concept_id,admitted_from_concept_id,admitted_from_source_value,discharged_to_source_value,discharged_to_concept_id,preceding_visit_detail_id,parent_visit_detail_id,visit_occurrence_id)
select [visit_detail_id],[person_id],[visit_detail_concept_id],[visit_detail_start_date],[visit_detail_start_datetime],[visit_detail_end_date],[visit_detail_end_datetime],[visit_detail_type_concept_id],[provider_id],[care_site_id],[visit_detail_source_value],[visit_detail_source_concept_id],[admitted_from_concept_id],[admitted_from_source_value],[discharged_to_source_value],[discharged_to_concept_id],[preceding_visit_detail_id],[parent_visit_detail_id],[visit_occurrence_id] 
    from [dbo].[transferVISIT_DETAIL];


--Alter table condition_occurrence
alter table [dbo].[CONDITION_OCCURRENCE] alter column [condition_occurrence_id] BIGINT;
alter table [dbo].[CONDITION_OCCURRENCE] alter column [person_id] BIGINT;
alter table [dbo].[CONDITION_OCCURRENCE] alter column [provider_id] BIGINT;
alter table [dbo].[CONDITION_OCCURRENCE] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[CONDITION_OCCURRENCE] alter column [visit_detail_id] BIGINT;
alter table [dbo].[CONDITION_OCCURRENCE] alter column [condition_status_source_value] VARCHAR(512);

insert into [dbo].[CONDITION_OCCURRENCE] (condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_start_datetime,condition_end_date,condition_end_datetime,condition_type_concept_id,condition_status_concept_id,stop_reason,provider_id,visit_occurrence_id,visit_detail_id,condition_source_value,condition_source_concept_id,condition_status_source_value)
select [condition_occurrence_id],[person_id],[condition_concept_id],[condition_start_date],[condition_start_datetime],[condition_end_date],[condition_end_datetime],[condition_type_concept_id],[condition_status_concept_id],[stop_reason],[provider_id],[visit_occurrence_id],[visit_detail_id],[condition_source_value],[condition_source_concept_id],[condition_status_source_value] 
    from [dbo].[transferCONDITION_OCCURRENCE];


--Alter table procedure_occurrence
alter table [dbo].[PROCEDURE_OCCURRENCE] alter column [procedure_occurrence_id] BIGINT;
alter table [dbo].[PROCEDURE_OCCURRENCE] alter column [person_id] BIGINT;
alter table [dbo].[PROCEDURE_OCCURRENCE] alter column [provider_id] BIGINT;
alter table [dbo].[PROCEDURE_OCCURRENCE] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[PROCEDURE_OCCURRENCE] alter column [visit_detail_id] BIGINT;

insert into [dbo].[PROCEDURE_OCCURRENCE] (procedure_occurrence_id,person_id,procedure_concept_id,procedure_date,procedure_datetime,procedure_end_date,procedure_end_datetime,procedure_type_concept_id,modifier_concept_id,quantity,provider_id,visit_occurrence_id,visit_detail_id,procedure_source_value,procedure_source_concept_id,modifier_source_value)
select [procedure_occurrence_id],[person_id],[procedure_concept_id],[procedure_date],[procedure_datetime],[procedure_end_date],[procedure_end_datetime],[procedure_type_concept_id],[modifier_concept_id],[quantity],[provider_id],[visit_occurrence_id],[visit_detail_id],[procedure_source_value],[procedure_source_concept_id],[modifier_source_value] 
    from [dbo].[transferPROCEDURE_OCCURRENCE];


--Alter table measurement
alter table [dbo].[MEASUREMENT] alter column [measurement_id] BIGINT;
alter table [dbo].[MEASUREMENT] alter column [person_id] BIGINT;
alter table [dbo].[MEASUREMENT] alter column [provider_id] BIGINT;
alter table [dbo].[MEASUREMENT] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[MEASUREMENT] alter column [visit_detail_id] BIGINT;
alter table [dbo].[MEASUREMENT] alter column [value_source_value] VARCHAR(512);

insert into [dbo].[MEASUREMENT] (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_datetime,measurement_time,measurement_type_concept_id,operator_concept_id,value_as_number,value_as_concept_id,unit_concept_id,range_low,range_high,provider_id,visit_occurrence_id,visit_detail_id,measurement_source_value,measurement_source_concept_id,unit_source_value,unit_source_concept_id,value_source_value,measurement_event_id,meas_event_field_concept_id)
select [measurement_id],[person_id],[measurement_concept_id],[measurement_date],[measurement_datetime],[measurement_time],[measurement_type_concept_id],[operator_concept_id],[value_as_number],[value_as_concept_id],[unit_concept_id],[range_low],[range_high],[provider_id],[visit_occurrence_id],[visit_detail_id],[measurement_source_value],[measurement_source_concept_id],[unit_source_value],[unit_source_concept_id],[value_source_value],[measurement_event_id],[meas_event_field_concept_id] 
    from [dbo].[transferMEASUREMENT];


--Alter table observation
alter table [dbo].[OBSERVATION] alter column [observation_id] BIGINT;
alter table [dbo].[OBSERVATION] alter column [person_id] BIGINT;
alter table [dbo].[OBSERVATION] alter column [provider_id] BIGINT;
alter table [dbo].[OBSERVATION] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[OBSERVATION] alter column [visit_detail_id] BIGINT;
alter table [dbo].[OBSERVATION] alter column [value_source_value] VARCHAR(512);

insert into [dbo].[OBSERVATION] (observation_id,person_id,observation_concept_id,observation_date,observation_datetime,observation_type_concept_id,value_as_number,value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,visit_detail_id,observation_source_value,observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_value,observation_event_id,obs_event_field_concept_id)
select [observation_id],[person_id],[observation_concept_id],[observation_date],[observation_datetime],[observation_type_concept_id],[value_as_number],left([value_as_string],60),[value_as_concept_id],[qualifier_concept_id],[unit_concept_id],[provider_id],[visit_occurrence_id],[visit_detail_id],[observation_source_value],[observation_source_concept_id],[unit_source_value],[qualifier_source_value],[value_source_value],[observation_event_id],[obs_event_field_concept_id] 
    from [dbo].[transferOBSERVATION];


--Alter table drug_exposure
alter table [dbo].[DRUG_EXPOSURE] alter column [drug_exposure_id] BIGINT;
alter table [dbo].[DRUG_EXPOSURE] alter column [person_id] BIGINT;
alter table [dbo].[DRUG_EXPOSURE] alter column [provider_id] BIGINT;
alter table [dbo].[DRUG_EXPOSURE] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[DRUG_EXPOSURE] alter column [visit_detail_id] BIGINT;
alter table [dbo].[DRUG_EXPOSURE] alter column [drug_source_value] VARCHAR(512);

insert into [dbo].[DRUG_EXPOSURE] (drug_exposure_id,person_id,drug_concept_id,drug_exposure_start_date,drug_exposure_start_datetime,drug_exposure_end_date,drug_exposure_end_datetime,verbatim_end_date,drug_type_concept_id,stop_reason,refills,quantity,days_supply,sig,route_concept_id,lot_number,provider_id,visit_occurrence_id,visit_detail_id,drug_source_value,drug_source_concept_id,route_source_value,dose_unit_source_value)
select [drug_exposure_id],[person_id],[drug_concept_id],[drug_exposure_start_date],[drug_exposure_start_datetime],coalesce(drug_exposure_end_date, drug_exposure_start_date),[drug_exposure_end_datetime],[verbatim_end_date],[drug_type_concept_id],[stop_reason],[refills],[quantity],[days_supply],[sig],[route_concept_id],[lot_number],[provider_id],[visit_occurrence_id],[visit_detail_id],left([drug_source_value],512),[drug_source_concept_id],[route_source_value],[dose_unit_source_value] 
    from [dbo].[transferDRUG_EXPOSURE]
 where drug_exposure_start_date is not NULL;


--Alter table device_exposure
alter table [dbo].[DEVICE_EXPOSURE] alter column [device_exposure_id] BIGINT;
alter table [dbo].[DEVICE_EXPOSURE] alter column [person_id] BIGINT;
alter table [dbo].[DEVICE_EXPOSURE] alter column [provider_id] BIGINT;
alter table [dbo].[DEVICE_EXPOSURE] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[DEVICE_EXPOSURE] alter column [visit_detail_id] BIGINT;

insert into [dbo].[DEVICE_EXPOSURE] (device_exposure_id,person_id,device_concept_id,device_exposure_start_date,device_exposure_start_datetime,device_exposure_end_date,device_exposure_end_datetime,device_type_concept_id,unique_device_id,production_id,quantity,provider_id,visit_occurrence_id,visit_detail_id,device_source_value,device_source_concept_id,unit_concept_id,unit_source_value,unit_source_concept_id)
select [device_exposure_id],[person_id],[device_concept_id],[device_exposure_start_date],[device_exposure_start_datetime],[device_exposure_end_date],[device_exposure_end_datetime],[device_type_concept_id],[unique_device_id],[production_id],[quantity],[provider_id],[visit_occurrence_id],[visit_detail_id],[device_source_value],[device_source_concept_id],[unit_concept_id],[unit_source_value],[unit_source_concept_id] 
    from [dbo].[transferDEVICE_EXPOSURE];


--Alter table payer_plan_period
alter table [dbo].[PAYER_PLAN_PERIOD] alter column [payer_plan_period_id] BIGINT;
alter table [dbo].[PAYER_PLAN_PERIOD] alter column [person_id] BIGINT;
alter table [dbo].[PAYER_PLAN_PERIOD] alter column [payer_source_value] VARCHAR(512);

insert into [dbo].[PAYER_PLAN_PERIOD] (payer_plan_period_id,person_id,payer_plan_period_start_date,payer_plan_period_end_date,payer_concept_id,payer_source_value,payer_source_concept_id,plan_concept_id,plan_source_value,plan_source_concept_id,sponsor_concept_id,sponsor_source_value,sponsor_source_concept_id,family_source_value,stop_reason_concept_id,stop_reason_source_value,stop_reason_source_concept_id)
select [payer_plan_period_id],[person_id],[payer_plan_period_start_date],[payer_plan_period_end_date],[payer_concept_id],left([payer_source_value],512),[payer_source_concept_id],[plan_concept_id],[plan_source_value],[plan_source_concept_id],[sponsor_concept_id],[sponsor_source_value],[sponsor_source_concept_id],[family_source_value],[stop_reason_concept_id],[stop_reason_source_value],[stop_reason_source_concept_id] 
    from [dbo].[transferPAYER_PLAN_PERIOD];


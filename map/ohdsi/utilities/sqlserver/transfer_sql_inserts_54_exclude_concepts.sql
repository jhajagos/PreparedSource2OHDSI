/* Script to load transferTables to a standard OHDSI CDM */


--Truncate tables
truncate table [dbo].[fact_relationship];
truncate table [dbo].[payer_plan_period];
truncate table [dbo].[device_exposure];
truncate table [dbo].[drug_exposure];
truncate table [dbo].[observation];
truncate table [dbo].[measurement];
truncate table [dbo].[procedure_occurrence];
truncate table [dbo].[condition_occurrence];
truncate table [dbo].[visit_detail];
truncate table [dbo].[visit_occurrence];
truncate table [dbo].[observation_period];
truncate table [dbo].[death];
truncate table [dbo].[person];
truncate table [dbo].[provider];
truncate table [dbo].[care_site];
truncate table [dbo].[location];
truncate table [dbo].[cdm_source];

--Alter table cdm_source

insert into [dbo].[cdm_source] (cdm_source_name,cdm_source_abbreviation,cdm_holder,source_description,source_documentation_reference,cdm_etl_reference,source_release_date,cdm_release_date,cdm_version,cdm_version_concept_id,vocabulary_version)
select [cdm_source_name],[cdm_source_abbreviation],[cdm_holder],[source_description],[source_documentation_reference],[cdm_etl_reference],[source_release_date],[cdm_release_date],[cdm_version],[cdm_version_concept_id],[vocabulary_version] 
    from [dbo].[transfercdm_source];


--Alter table location
alter table [dbo].[location] alter column [location_id] BIGINT;
alter table [dbo].[location] alter column [address_1] VARCHAR(512);
alter table [dbo].[location] alter column [address_2] VARCHAR(512);
alter table [dbo].[location] alter column [state] VARCHAR(512);
alter table [dbo].[location] alter column [zip] VARCHAR(512);
alter table [dbo].[location] alter column [county] VARCHAR(512);
alter table [dbo].[location] alter column [country_source_value] VARCHAR(512);

insert into [dbo].[location] (location_id,address_1,address_2,city,state,zip,county,location_source_value,country_concept_id,country_source_value,latitude,longitude)
select [location_id],[address_1],[address_2],[city],[state],[zip],[county],[location_source_value],[country_concept_id],[country_source_value],cast(latitude as float),cast(longitude as float) 
    from [dbo].[transferlocation];


--Alter table care_site
alter table [dbo].[care_site] alter column [location_id] BIGINT;

insert into [dbo].[care_site] (care_site_id,care_site_name,place_of_service_concept_id,location_id,care_site_source_value,place_of_service_source_value)
select [care_site_id],[care_site_name],[place_of_service_concept_id],[location_id],[care_site_source_value],[place_of_service_source_value] 
    from [dbo].[transfercare_site];


--Alter table provider
alter table [dbo].[provider] alter column [provider_id] BIGINT;

insert into [dbo].[provider] (provider_id,provider_name,npi,dea,specialty_concept_id,care_site_id,year_of_birth,gender_concept_id,provider_source_value,specialty_source_value,specialty_source_concept_id,gender_source_value,gender_source_concept_id)
select [provider_id],[provider_name],[npi],[dea],[specialty_concept_id],[care_site_id],[year_of_birth],[gender_concept_id],[provider_source_value],[specialty_source_value],[specialty_source_concept_id],[gender_source_value],[gender_source_concept_id] 
    from [dbo].[transferprovider];


--Alter table person
alter table [dbo].[person] alter column [person_id] BIGINT;
alter table [dbo].[person] alter column [location_id] BIGINT;
alter table [dbo].[person] alter column [provider_id] BIGINT;
alter table [dbo].[person] alter column [race_source_value] VARCHAR(512);
alter table [dbo].[person] alter column [ethnicity_source_value] VARCHAR(512);

insert into [dbo].[person] (person_id,gender_concept_id,year_of_birth,month_of_birth,day_of_birth,birth_datetime,race_concept_id,ethnicity_concept_id,location_id,provider_id,care_site_id,person_source_value,gender_source_value,gender_source_concept_id,race_source_value,race_source_concept_id,ethnicity_source_value,ethnicity_source_concept_id)
select [person_id],[gender_concept_id],[year_of_birth],[month_of_birth],[day_of_birth],[birth_datetime],[race_concept_id],[ethnicity_concept_id],[location_id],[provider_id],[care_site_id],[person_source_value],[gender_source_value],[gender_source_concept_id],[race_source_value],[race_source_concept_id],[ethnicity_source_value],[ethnicity_source_concept_id] 
    from [dbo].[transferperson];


--Alter table death
alter table [dbo].[death] alter column [person_id] BIGINT;

insert into [dbo].[death] (person_id,death_date,death_datetime,death_type_concept_id,cause_concept_id,cause_source_value,cause_source_concept_id)
select [person_id],[death_date],[death_datetime],[death_type_concept_id],[cause_concept_id],[cause_source_value],[cause_source_concept_id] 
    from [dbo].[transferdeath];


--Alter table observation_period
alter table [dbo].[observation_period] alter column [observation_period_id] BIGINT;
alter table [dbo].[observation_period] alter column [person_id] BIGINT;

insert into [dbo].[observation_period] (observation_period_id,person_id,observation_period_start_date,observation_period_end_date,period_type_concept_id)
select [observation_period_id],[person_id],[observation_period_start_date],[observation_period_end_date],[period_type_concept_id] 
    from [dbo].[transferobservation_period];


--Alter table visit_occurrence
alter table [dbo].[visit_occurrence] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[visit_occurrence] alter column [person_id] BIGINT;
alter table [dbo].[visit_occurrence] alter column [provider_id] BIGINT;
alter table [dbo].[visit_occurrence] alter column [admitted_from_source_value] VARCHAR(512);
alter table [dbo].[visit_occurrence] alter column [discharged_to_source_value] VARCHAR(512);

insert into [dbo].[visit_occurrence] (visit_occurrence_id,person_id,visit_concept_id,visit_start_date,visit_start_datetime,visit_end_date,visit_end_datetime,visit_type_concept_id,provider_id,care_site_id,visit_source_value,visit_source_concept_id,admitted_from_concept_id,admitted_from_source_value,discharged_to_concept_id,discharged_to_source_value,preceding_visit_occurrence_id)
select [visit_occurrence_id],[person_id],[visit_concept_id],[visit_start_date],[visit_start_datetime],[visit_end_date],[visit_end_datetime],[visit_type_concept_id],[provider_id],[care_site_id],[visit_source_value],[visit_source_concept_id],[admitted_from_concept_id],[admitted_from_source_value],[discharged_to_concept_id],[discharged_to_source_value],[preceding_visit_occurrence_id] 
    from [dbo].[transfervisit_occurrence];


--Alter table visit_detail
alter table [dbo].[visit_detail] alter column [visit_detail_id] BIGINT;
alter table [dbo].[visit_detail] alter column [person_id] BIGINT;
alter table [dbo].[visit_detail] alter column [provider_id] BIGINT;
alter table [dbo].[visit_detail] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[visit_detail] alter column [admitted_from_source_value] VARCHAR(512);
alter table [dbo].[visit_detail] alter column [discharged_to_source_value] VARCHAR(512);

insert into [dbo].[visit_detail] (visit_detail_id,person_id,visit_detail_concept_id,visit_detail_start_date,visit_detail_start_datetime,visit_detail_end_date,visit_detail_end_datetime,visit_detail_type_concept_id,provider_id,care_site_id,visit_detail_source_value,visit_detail_source_concept_id,admitted_from_concept_id,admitted_from_source_value,discharged_to_source_value,discharged_to_concept_id,preceding_visit_detail_id,parent_visit_detail_id,visit_occurrence_id)
select [visit_detail_id],[person_id],[visit_detail_concept_id],[visit_detail_start_date],[visit_detail_start_datetime],[visit_detail_end_date],[visit_detail_end_datetime],[visit_detail_type_concept_id],[provider_id],[care_site_id],[visit_detail_source_value],[visit_detail_source_concept_id],[admitted_from_concept_id],[admitted_from_source_value],[discharged_to_source_value],[discharged_to_concept_id],[preceding_visit_detail_id],[parent_visit_detail_id],[visit_occurrence_id] 
    from [dbo].[transfervisit_detail];


--Alter table condition_occurrence
alter table [dbo].[condition_occurrence] alter column [condition_occurrence_id] BIGINT;
alter table [dbo].[condition_occurrence] alter column [person_id] BIGINT;
alter table [dbo].[condition_occurrence] alter column [provider_id] BIGINT;
alter table [dbo].[condition_occurrence] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[condition_occurrence] alter column [visit_detail_id] BIGINT;
alter table [dbo].[condition_occurrence] alter column [condition_status_source_value] VARCHAR(512);

insert into [dbo].[condition_occurrence] (condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_start_datetime,condition_end_date,condition_end_datetime,condition_type_concept_id,condition_status_concept_id,stop_reason,provider_id,visit_occurrence_id,visit_detail_id,condition_source_value,condition_source_concept_id,condition_status_source_value)
select [condition_occurrence_id],[person_id],[condition_concept_id],[condition_start_date],[condition_start_datetime],[condition_end_date],[condition_end_datetime],[condition_type_concept_id],[condition_status_concept_id],[stop_reason],[provider_id],[visit_occurrence_id],[visit_detail_id],[condition_source_value],[condition_source_concept_id],[condition_status_source_value] 
    from [dbo].[transfercondition_occurrence];


--Alter table procedure_occurrence
alter table [dbo].[procedure_occurrence] alter column [procedure_occurrence_id] BIGINT;
alter table [dbo].[procedure_occurrence] alter column [person_id] BIGINT;
alter table [dbo].[procedure_occurrence] alter column [provider_id] BIGINT;
alter table [dbo].[procedure_occurrence] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[procedure_occurrence] alter column [visit_detail_id] BIGINT;

insert into [dbo].[procedure_occurrence] (procedure_occurrence_id,person_id,procedure_concept_id,procedure_date,procedure_datetime,procedure_end_date,procedure_end_datetime,procedure_type_concept_id,modifier_concept_id,quantity,provider_id,visit_occurrence_id,visit_detail_id,procedure_source_value,procedure_source_concept_id,modifier_source_value)
select [procedure_occurrence_id],[person_id],[procedure_concept_id],[procedure_date],[procedure_datetime],[procedure_end_date],[procedure_end_datetime],[procedure_type_concept_id],[modifier_concept_id],[quantity],[provider_id],[visit_occurrence_id],[visit_detail_id],[procedure_source_value],[procedure_source_concept_id],[modifier_source_value] 
    from [dbo].[transferprocedure_occurrence];


--Alter table measurement
alter table [dbo].[measurement] alter column [measurement_id] BIGINT;
alter table [dbo].[measurement] alter column [person_id] BIGINT;
alter table [dbo].[measurement] alter column [provider_id] BIGINT;
alter table [dbo].[measurement] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[measurement] alter column [visit_detail_id] BIGINT;
alter table [dbo].[measurement] alter column [value_source_value] VARCHAR(512);

insert into [dbo].[measurement] (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_datetime,measurement_time,measurement_type_concept_id,operator_concept_id,value_as_number,value_as_concept_id,unit_concept_id,range_low,range_high,provider_id,visit_occurrence_id,visit_detail_id,measurement_source_value,measurement_source_concept_id,unit_source_value,unit_source_concept_id,value_source_value,measurement_event_id,meas_event_field_concept_id)
select [measurement_id],[person_id],[measurement_concept_id],[measurement_date],[measurement_datetime],[measurement_time],[measurement_type_concept_id],[operator_concept_id],[value_as_number],[value_as_concept_id],[unit_concept_id],[range_low],[range_high],[provider_id],[visit_occurrence_id],[visit_detail_id],[measurement_source_value],[measurement_source_concept_id],[unit_source_value],[unit_source_concept_id],[value_source_value],[measurement_event_id],[meas_event_field_concept_id] 
    from [dbo].[transfermeasurement];


--Alter table observation
alter table [dbo].[observation] alter column [observation_id] BIGINT;
alter table [dbo].[observation] alter column [person_id] BIGINT;
alter table [dbo].[observation] alter column [provider_id] BIGINT;
alter table [dbo].[observation] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[observation] alter column [visit_detail_id] BIGINT;
alter table [dbo].[observation] alter column [value_source_value] VARCHAR(512);

insert into [dbo].[observation] (observation_id,person_id,observation_concept_id,observation_date,observation_datetime,observation_type_concept_id,value_as_number,value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,visit_detail_id,observation_source_value,observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_value,observation_event_id,obs_event_field_concept_id)
select [observation_id],[person_id],[observation_concept_id],[observation_date],[observation_datetime],[observation_type_concept_id],[value_as_number],left([value_as_string],60),[value_as_concept_id],[qualifier_concept_id],[unit_concept_id],[provider_id],[visit_occurrence_id],[visit_detail_id],[observation_source_value],[observation_source_concept_id],[unit_source_value],[qualifier_source_value],[value_source_value],[observation_event_id],[obs_event_field_concept_id] 
    from [dbo].[transferobservation];


--Alter table drug_exposure
alter table [dbo].[drug_exposure] alter column [drug_exposure_id] BIGINT;
alter table [dbo].[drug_exposure] alter column [person_id] BIGINT;
alter table [dbo].[drug_exposure] alter column [provider_id] BIGINT;
alter table [dbo].[drug_exposure] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[drug_exposure] alter column [visit_detail_id] BIGINT;
alter table [dbo].[drug_exposure] alter column [drug_source_value] VARCHAR(512);

insert into [dbo].[drug_exposure] (drug_exposure_id,person_id,drug_concept_id,drug_exposure_start_date,drug_exposure_start_datetime,drug_exposure_end_date,drug_exposure_end_datetime,verbatim_end_date,drug_type_concept_id,stop_reason,refills,quantity,days_supply,sig,route_concept_id,lot_number,provider_id,visit_occurrence_id,visit_detail_id,drug_source_value,drug_source_concept_id,route_source_value,dose_unit_source_value)
select [drug_exposure_id],[person_id],[drug_concept_id],[drug_exposure_start_date],[drug_exposure_start_datetime],coalesce(drug_exposure_end_date, drug_exposure_start_date),[drug_exposure_end_datetime],[verbatim_end_date],[drug_type_concept_id],[stop_reason],[refills],[quantity],[days_supply],[sig],[route_concept_id],[lot_number],[provider_id],[visit_occurrence_id],[visit_detail_id],left([drug_source_value],512),[drug_source_concept_id],[route_source_value],[dose_unit_source_value] 
    from [dbo].[transferdrug_exposure]
 where drug_exposure_start_date is not NULL;


--Alter table device_exposure
alter table [dbo].[device_exposure] alter column [device_exposure_id] BIGINT;
alter table [dbo].[device_exposure] alter column [person_id] BIGINT;
alter table [dbo].[device_exposure] alter column [provider_id] BIGINT;
alter table [dbo].[device_exposure] alter column [visit_occurrence_id] BIGINT;
alter table [dbo].[device_exposure] alter column [visit_detail_id] BIGINT;

insert into [dbo].[device_exposure] (device_exposure_id,person_id,device_concept_id,device_exposure_start_date,device_exposure_start_datetime,device_exposure_end_date,device_exposure_end_datetime,device_type_concept_id,unique_device_id,production_id,quantity,provider_id,visit_occurrence_id,visit_detail_id,device_source_value,device_source_concept_id,unit_concept_id,unit_source_value,unit_source_concept_id)
select [device_exposure_id],[person_id],[device_concept_id],[device_exposure_start_date],[device_exposure_start_datetime],[device_exposure_end_date],[device_exposure_end_datetime],[device_type_concept_id],[unique_device_id],[production_id],[quantity],[provider_id],[visit_occurrence_id],[visit_detail_id],[device_source_value],[device_source_concept_id],[unit_concept_id],[unit_source_value],[unit_source_concept_id] 
    from [dbo].[transferdevice_exposure];


--Alter table payer_plan_period
alter table [dbo].[payer_plan_period] alter column [payer_plan_period_id] BIGINT;
alter table [dbo].[payer_plan_period] alter column [person_id] BIGINT;
alter table [dbo].[payer_plan_period] alter column [payer_source_value] VARCHAR(512);

insert into [dbo].[payer_plan_period] (payer_plan_period_id,person_id,payer_plan_period_start_date,payer_plan_period_end_date,payer_concept_id,payer_source_value,payer_source_concept_id,plan_concept_id,plan_source_value,plan_source_concept_id,sponsor_concept_id,sponsor_source_value,sponsor_source_concept_id,family_source_value,stop_reason_concept_id,stop_reason_source_value,stop_reason_source_concept_id)
select [payer_plan_period_id],[person_id],[payer_plan_period_start_date],[payer_plan_period_end_date],[payer_concept_id],left([payer_source_value],512),[payer_source_concept_id],[plan_concept_id],[plan_source_value],[plan_source_concept_id],[sponsor_concept_id],[sponsor_source_value],[sponsor_source_concept_id],[family_source_value],[stop_reason_concept_id],[stop_reason_source_value],[stop_reason_source_concept_id] 
    from [dbo].[transferpayer_plan_period];


--Alter table fact_relationship
alter table [dbo].[fact_relationship] alter column [fact_id_1] BIGINT;
alter table [dbo].[fact_relationship] alter column [fact_id_2] BIGINT;

insert into [dbo].[fact_relationship] (domain_concept_id_1,fact_id_1,domain_concept_id_2,fact_id_2,relationship_concept_id)
select [domain_concept_id_1],[fact_id_1],[domain_concept_id_2],[fact_id_2],[relationship_concept_id] 
    from [dbo].[transferfact_relationship];


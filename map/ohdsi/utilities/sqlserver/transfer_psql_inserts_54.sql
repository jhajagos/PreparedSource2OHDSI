/* Script to load transferTables to a standard OHDSI CDM */


--Truncate tables
truncate table "ohdsi"."payer_plan_period";
truncate table "ohdsi"."device_exposure";
truncate table "ohdsi"."drug_exposure";
truncate table "ohdsi"."observation";
truncate table "ohdsi"."measurement";
truncate table "ohdsi"."procedure_occurrence";
truncate table "ohdsi"."condition_occurrence";
truncate table "ohdsi"."visit_detail";
truncate table "ohdsi"."visit_occurrence";
truncate table "ohdsi"."observation_period";
truncate table "ohdsi"."death";
truncate table "ohdsi"."person";
truncate table "ohdsi"."provider";
truncate table "ohdsi"."care_site";
truncate table "ohdsi"."location";
truncate table "ohdsi"."cdm_source";
truncate table "ohdsi"."drug_strength";
truncate table "ohdsi"."concept_relationship";
truncate table "ohdsi"."relationship";
truncate table "ohdsi"."concept_ancestor";
truncate table "ohdsi"."vocabulary";
truncate table "ohdsi"."concept";
truncate table "ohdsi"."concept_class";
truncate table "ohdsi"."domain";

--Alter table domain

insert into "ohdsi"."domain" (domain_id,domain_name,domain_concept_id)
select "domain_id","domain_name","domain_concept_id" 
    from "ohdsi"."transferdomain";


--Alter table concept_class

insert into "ohdsi"."concept_class" (concept_class_id,concept_class_name,concept_class_concept_id)
select "concept_class_id","concept_class_name","concept_class_concept_id" 
    from "ohdsi"."transferconcept_class";


--Alter table concept

insert into "ohdsi"."concept" (concept_id,concept_name,domain_id,vocabulary_id,concept_class_id,standard_concept,concept_code,valid_start_date,valid_end_date,invalid_reason)
select "concept_id","concept_name","domain_id","vocabulary_id","concept_class_id","standard_concept","concept_code",cast(cast(valid_start_date as varchar(8)) as date),cast(cast(valid_start_date as varchar(8)) as date),"invalid_reason" 
    from "ohdsi"."transferconcept";


--Alter table vocabulary

insert into "ohdsi"."vocabulary" (vocabulary_id,vocabulary_name,vocabulary_reference,vocabulary_version,vocabulary_concept_id)
select "vocabulary_id","vocabulary_name","vocabulary_reference","vocabulary_version","vocabulary_concept_id" 
    from "ohdsi"."transfervocabulary";


--Alter table concept_ancestor

insert into "ohdsi"."concept_ancestor" (ancestor_concept_id,descendant_concept_id,min_levels_of_separation,max_levels_of_separation)
select "ancestor_concept_id","descendant_concept_id","min_levels_of_separation","max_levels_of_separation" 
    from "ohdsi"."transferconcept_ancestor";


--Alter table relationship

insert into "ohdsi"."relationship" (relationship_id,relationship_name,is_hierarchical,defines_ancestry,reverse_relationship_id,relationship_concept_id)
select "relationship_id","relationship_name","is_hierarchical","defines_ancestry","reverse_relationship_id","relationship_concept_id" 
    from "ohdsi"."transferrelationship";


--Alter table concept_relationship

insert into "ohdsi"."concept_relationship" (concept_id_1,concept_id_2,relationship_id,valid_start_date,valid_end_date,invalid_reason)
select "concept_id_1","concept_id_2","relationship_id",cast(cast(valid_start_date as varchar(8)) as date),cast(cast(valid_start_date as varchar(8)) as date),"invalid_reason" 
    from "ohdsi"."transferconcept_relationship";


--Alter table drug_strength

insert into "ohdsi"."drug_strength" (drug_concept_id,ingredient_concept_id,amount_value,amount_unit_concept_id,numerator_value,numerator_unit_concept_id,denominator_value,denominator_unit_concept_id,box_size,valid_start_date,valid_end_date,invalid_reason)
select "drug_concept_id","ingredient_concept_id","amount_value","amount_unit_concept_id","numerator_value","numerator_unit_concept_id","denominator_value","denominator_unit_concept_id","box_size",cast(cast(valid_start_date as varchar(8)) as date),cast(cast(valid_start_date as varchar(8)) as date),"invalid_reason" 
    from "ohdsi"."transferdrug_strength";


--Alter table cdm_source

insert into "ohdsi"."cdm_source" (cdm_source_name,cdm_source_abbreviation,cdm_holder,source_description,source_documentation_reference,cdm_etl_reference,source_release_date,cdm_release_date,cdm_version,cdm_version_concept_id,vocabulary_version)
select "cdm_source_name","cdm_source_abbreviation","cdm_holder","source_description","source_documentation_reference","cdm_etl_reference","source_release_date","cdm_release_date","cdm_version","cdm_version_concept_id","vocabulary_version" 
    from "ohdsi"."transfercdm_source";


--Alter table location
alter table "ohdsi"."location" alter column "location_id" type BIGINT;
alter table "ohdsi"."location" alter column "address_1" type VARCHAR(512);
alter table "ohdsi"."location" alter column "address_2" type VARCHAR(512);
alter table "ohdsi"."location" alter column "state" type VARCHAR(512);
alter table "ohdsi"."location" alter column "zip" type VARCHAR(512);
alter table "ohdsi"."location" alter column "county" type VARCHAR(512);
alter table "ohdsi"."location" alter column "country_source_value" type VARCHAR(512);

insert into "ohdsi"."location" (location_id,address_1,address_2,city,state,zip,county,location_source_value,country_concept_id,country_source_value,latitude,longitude)
select "location_id","address_1","address_2","city","state","zip","county","location_source_value","country_concept_id","country_source_value",cast(latitude as float),cast(longitude as float) 
    from "ohdsi"."transferlocation";


--Alter table care_site
alter table "ohdsi"."care_site" alter column "location_id" type BIGINT;

insert into "ohdsi"."care_site" (care_site_id,care_site_name,place_of_service_concept_id,location_id,care_site_source_value,place_of_service_source_value)
select "care_site_id","care_site_name","place_of_service_concept_id","location_id","care_site_source_value","place_of_service_source_value" 
    from "ohdsi"."transfercare_site";


--Alter table provider
alter table "ohdsi"."provider" alter column "provider_id" type BIGINT;

insert into "ohdsi"."provider" (provider_id,provider_name,npi,dea,specialty_concept_id,care_site_id,year_of_birth,gender_concept_id,provider_source_value,specialty_source_value,specialty_source_concept_id,gender_source_value,gender_source_concept_id)
select "provider_id","provider_name","npi","dea","specialty_concept_id","care_site_id","year_of_birth","gender_concept_id","provider_source_value","specialty_source_value","specialty_source_concept_id","gender_source_value","gender_source_concept_id" 
    from "ohdsi"."transferprovider";


--Alter table person
alter table "ohdsi"."person" alter column "person_id" type BIGINT;
alter table "ohdsi"."person" alter column "location_id" type BIGINT;
alter table "ohdsi"."person" alter column "provider_id" type BIGINT;
alter table "ohdsi"."person" alter column "race_source_value" type VARCHAR(512);
alter table "ohdsi"."person" alter column "ethnicity_source_value" type VARCHAR(512);

insert into "ohdsi"."person" (person_id,gender_concept_id,year_of_birth,month_of_birth,day_of_birth,birth_datetime,race_concept_id,ethnicity_concept_id,location_id,provider_id,care_site_id,person_source_value,gender_source_value,gender_source_concept_id,race_source_value,race_source_concept_id,ethnicity_source_value,ethnicity_source_concept_id)
select "person_id","gender_concept_id","year_of_birth","month_of_birth","day_of_birth",cast(birth_datetime as timestamp),"race_concept_id","ethnicity_concept_id","location_id","provider_id","care_site_id","person_source_value","gender_source_value","gender_source_concept_id","race_source_value","race_source_concept_id","ethnicity_source_value","ethnicity_source_concept_id" 
    from "ohdsi"."transferperson";


--Alter table death
alter table "ohdsi"."death" alter column "person_id" type BIGINT;

insert into "ohdsi"."death" (person_id,death_date,death_datetime,death_type_concept_id,cause_concept_id,cause_source_value,cause_source_concept_id)
select "person_id","death_date",cast(death_datetime as timestamp),"death_type_concept_id","cause_concept_id","cause_source_value","cause_source_concept_id" 
    from "ohdsi"."transferdeath";


--Alter table observation_period
alter table "ohdsi"."observation_period" alter column "observation_period_id" type BIGINT;
alter table "ohdsi"."observation_period" alter column "person_id" type BIGINT;

insert into "ohdsi"."observation_period" (observation_period_id,person_id,observation_period_start_date,observation_period_end_date,period_type_concept_id)
select "observation_period_id","person_id","observation_period_start_date","observation_period_end_date","period_type_concept_id" 
    from "ohdsi"."transferobservation_period";


--Alter table visit_occurrence
alter table "ohdsi"."visit_occurrence" alter column "visit_occurrence_id" type BIGINT;
alter table "ohdsi"."visit_occurrence" alter column "person_id" type BIGINT;
alter table "ohdsi"."visit_occurrence" alter column "provider_id" type BIGINT;
alter table "ohdsi"."visit_occurrence" alter column "admitted_from_source_value" type VARCHAR(512);
alter table "ohdsi"."visit_occurrence" alter column "discharged_to_source_value" type VARCHAR(512);

insert into "ohdsi"."visit_occurrence" (visit_occurrence_id,person_id,visit_concept_id,visit_start_date,visit_start_datetime,visit_end_date,visit_end_datetime,visit_type_concept_id,provider_id,care_site_id,visit_source_value,visit_source_concept_id,admitted_from_concept_id,admitted_from_source_value,discharged_to_concept_id,discharged_to_source_value,preceding_visit_occurrence_id)
select "visit_occurrence_id","person_id","visit_concept_id","visit_start_date",cast(visit_start_datetime as timestamp),"visit_end_date",cast(visit_end_datetime as timestamp),"visit_type_concept_id","provider_id","care_site_id","visit_source_value","visit_source_concept_id","admitted_from_concept_id","admitted_from_source_value","discharged_to_concept_id","discharged_to_source_value","preceding_visit_occurrence_id" 
    from "ohdsi"."transfervisit_occurrence";


--Alter table visit_detail
alter table "ohdsi"."visit_detail" alter column "visit_detail_id" type BIGINT;
alter table "ohdsi"."visit_detail" alter column "person_id" type BIGINT;
alter table "ohdsi"."visit_detail" alter column "provider_id" type BIGINT;
alter table "ohdsi"."visit_detail" alter column "visit_occurrence_id" type BIGINT;
alter table "ohdsi"."visit_detail" alter column "admitted_from_source_value" type VARCHAR(512);
alter table "ohdsi"."visit_detail" alter column "discharged_to_source_value" type VARCHAR(512);

insert into "ohdsi"."visit_detail" (visit_detail_id,person_id,visit_detail_concept_id,visit_detail_start_date,visit_detail_start_datetime,visit_detail_end_date,visit_detail_end_datetime,visit_detail_type_concept_id,provider_id,care_site_id,visit_detail_source_value,visit_detail_source_concept_id,admitted_from_concept_id,admitted_from_source_value,discharged_to_source_value,discharged_to_concept_id,preceding_visit_detail_id,parent_visit_detail_id,visit_occurrence_id)
select "visit_detail_id","person_id","visit_detail_concept_id","visit_detail_start_date",cast(visit_detail_start_datetime as timestamp),"visit_detail_end_date",cast(visit_detail_end_datetime as timestamp),"visit_detail_type_concept_id","provider_id","care_site_id","visit_detail_source_value","visit_detail_source_concept_id","admitted_from_concept_id","admitted_from_source_value","discharged_to_source_value","discharged_to_concept_id","preceding_visit_detail_id","parent_visit_detail_id","visit_occurrence_id" 
    from "ohdsi"."transfervisit_detail";


--Alter table condition_occurrence
alter table "ohdsi"."condition_occurrence" alter column "condition_occurrence_id" type BIGINT;
alter table "ohdsi"."condition_occurrence" alter column "person_id" type BIGINT;
alter table "ohdsi"."condition_occurrence" alter column "provider_id" type BIGINT;
alter table "ohdsi"."condition_occurrence" alter column "visit_occurrence_id" type BIGINT;
alter table "ohdsi"."condition_occurrence" alter column "visit_detail_id" type BIGINT;
alter table "ohdsi"."condition_occurrence" alter column "condition_status_source_value" type VARCHAR(512);

insert into "ohdsi"."condition_occurrence" (condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_start_datetime,condition_end_date,condition_end_datetime,condition_type_concept_id,condition_status_concept_id,stop_reason,provider_id,visit_occurrence_id,visit_detail_id,condition_source_value,condition_source_concept_id,condition_status_source_value)
select "condition_occurrence_id","person_id","condition_concept_id","condition_start_date",cast(condition_start_datetime as timestamp),"condition_end_date",cast(condition_end_datetime as timestamp),"condition_type_concept_id","condition_status_concept_id","stop_reason","provider_id","visit_occurrence_id","visit_detail_id","condition_source_value","condition_source_concept_id","condition_status_source_value" 
    from "ohdsi"."transfercondition_occurrence";


--Alter table procedure_occurrence
alter table "ohdsi"."procedure_occurrence" alter column "procedure_occurrence_id" type BIGINT;
alter table "ohdsi"."procedure_occurrence" alter column "person_id" type BIGINT;
alter table "ohdsi"."procedure_occurrence" alter column "provider_id" type BIGINT;
alter table "ohdsi"."procedure_occurrence" alter column "visit_occurrence_id" type BIGINT;
alter table "ohdsi"."procedure_occurrence" alter column "visit_detail_id" type BIGINT;

insert into "ohdsi"."procedure_occurrence" (procedure_occurrence_id,person_id,procedure_concept_id,procedure_date,procedure_datetime,procedure_end_date,procedure_end_datetime,procedure_type_concept_id,modifier_concept_id,quantity,provider_id,visit_occurrence_id,visit_detail_id,procedure_source_value,procedure_source_concept_id,modifier_source_value)
select "procedure_occurrence_id","person_id","procedure_concept_id","procedure_date",cast(procedure_datetime as timestamp),"procedure_end_date",cast(procedure_end_datetime as timestamp),"procedure_type_concept_id","modifier_concept_id",cast(quantity as float),"provider_id","visit_occurrence_id","visit_detail_id","procedure_source_value","procedure_source_concept_id","modifier_source_value" 
    from "ohdsi"."transferprocedure_occurrence";


--Alter table measurement
alter table "ohdsi"."measurement" alter column "measurement_id" type BIGINT;
alter table "ohdsi"."measurement" alter column "person_id" type BIGINT;
alter table "ohdsi"."measurement" alter column "provider_id" type BIGINT;
alter table "ohdsi"."measurement" alter column "visit_occurrence_id" type BIGINT;
alter table "ohdsi"."measurement" alter column "visit_detail_id" type BIGINT;
alter table "ohdsi"."measurement" alter column "value_source_value" type VARCHAR(512);

insert into "ohdsi"."measurement" (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_datetime,measurement_time,measurement_type_concept_id,operator_concept_id,value_as_number,value_as_concept_id,unit_concept_id,range_low,range_high,provider_id,visit_occurrence_id,visit_detail_id,measurement_source_value,measurement_source_concept_id,unit_source_value,unit_source_concept_id,value_source_value,measurement_event_id,meas_event_field_concept_id)
select "measurement_id","person_id","measurement_concept_id","measurement_date",cast(measurement_datetime as timestamp),"measurement_time","measurement_type_concept_id","operator_concept_id","value_as_number","value_as_concept_id","unit_concept_id","range_low","range_high","provider_id","visit_occurrence_id","visit_detail_id","measurement_source_value","measurement_source_concept_id","unit_source_value","unit_source_concept_id","value_source_value","measurement_event_id","meas_event_field_concept_id" 
    from "ohdsi"."transfermeasurement";


--Alter table observation
alter table "ohdsi"."observation" alter column "observation_id" type BIGINT;
alter table "ohdsi"."observation" alter column "person_id" type BIGINT;
alter table "ohdsi"."observation" alter column "provider_id" type BIGINT;
alter table "ohdsi"."observation" alter column "visit_occurrence_id" type BIGINT;
alter table "ohdsi"."observation" alter column "visit_detail_id" type BIGINT;
alter table "ohdsi"."observation" alter column "value_source_value" type VARCHAR(512);

insert into "ohdsi"."observation" (observation_id,person_id,observation_concept_id,observation_date,observation_datetime,observation_type_concept_id,value_as_number,value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,visit_detail_id,observation_source_value,observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_value,observation_event_id,obs_event_field_concept_id)
select "observation_id","person_id","observation_concept_id","observation_date",cast(observation_datetime as timestamp),"observation_type_concept_id","value_as_number",left("value_as_string",60),"value_as_concept_id","qualifier_concept_id","unit_concept_id","provider_id","visit_occurrence_id","visit_detail_id","observation_source_value","observation_source_concept_id","unit_source_value","qualifier_source_value","value_source_value","observation_event_id","obs_event_field_concept_id" 
    from "ohdsi"."transferobservation";


--Alter table drug_exposure
alter table "ohdsi"."drug_exposure" alter column "drug_exposure_id" type BIGINT;
alter table "ohdsi"."drug_exposure" alter column "person_id" type BIGINT;
alter table "ohdsi"."drug_exposure" alter column "provider_id" type BIGINT;
alter table "ohdsi"."drug_exposure" alter column "visit_occurrence_id" type BIGINT;
alter table "ohdsi"."drug_exposure" alter column "visit_detail_id" type BIGINT;
alter table "ohdsi"."drug_exposure" alter column "drug_source_value" type VARCHAR(512);

insert into "ohdsi"."drug_exposure" (drug_exposure_id,person_id,drug_concept_id,drug_exposure_start_date,drug_exposure_start_datetime,drug_exposure_end_date,drug_exposure_end_datetime,verbatim_end_date,drug_type_concept_id,stop_reason,refills,quantity,days_supply,sig,route_concept_id,lot_number,provider_id,visit_occurrence_id,visit_detail_id,drug_source_value,drug_source_concept_id,route_source_value,dose_unit_source_value)
select "drug_exposure_id","person_id","drug_concept_id","drug_exposure_start_date",cast(drug_exposure_start_datetime as timestamp),coalesce(drug_exposure_end_date, drug_exposure_start_date),cast(drug_exposure_end_datetime as timestamp),"verbatim_end_date","drug_type_concept_id","stop_reason","refills",cast(quantity as float),"days_supply","sig","route_concept_id","lot_number","provider_id","visit_occurrence_id","visit_detail_id",left("drug_source_value",512),"drug_source_concept_id","route_source_value","dose_unit_source_value" 
    from "ohdsi"."transferdrug_exposure"
 where drug_exposure_start_date is not NULL;


--Alter table device_exposure
alter table "ohdsi"."device_exposure" alter column "device_exposure_id" type BIGINT;
alter table "ohdsi"."device_exposure" alter column "person_id" type BIGINT;
alter table "ohdsi"."device_exposure" alter column "provider_id" type BIGINT;
alter table "ohdsi"."device_exposure" alter column "visit_occurrence_id" type BIGINT;
alter table "ohdsi"."device_exposure" alter column "visit_detail_id" type BIGINT;

insert into "ohdsi"."device_exposure" (device_exposure_id,person_id,device_concept_id,device_exposure_start_date,device_exposure_start_datetime,device_exposure_end_date,device_exposure_end_datetime,device_type_concept_id,unique_device_id,production_id,quantity,provider_id,visit_occurrence_id,visit_detail_id,device_source_value,device_source_concept_id,unit_concept_id,unit_source_value,unit_source_concept_id)
select "device_exposure_id","person_id","device_concept_id","device_exposure_start_date",cast(device_exposure_start_datetime as timestamp),"device_exposure_end_date",cast(device_exposure_end_datetime as timestamp),"device_type_concept_id","unique_device_id","production_id",cast(quantity as float),"provider_id","visit_occurrence_id","visit_detail_id","device_source_value","device_source_concept_id","unit_concept_id","unit_source_value","unit_source_concept_id" 
    from "ohdsi"."transferdevice_exposure";


--Alter table payer_plan_period
alter table "ohdsi"."payer_plan_period" alter column "payer_plan_period_id" type BIGINT;
alter table "ohdsi"."payer_plan_period" alter column "person_id" type BIGINT;
alter table "ohdsi"."payer_plan_period" alter column "payer_source_value" type VARCHAR(512);

insert into "ohdsi"."payer_plan_period" (payer_plan_period_id,person_id,payer_plan_period_start_date,payer_plan_period_end_date,payer_concept_id,payer_source_value,payer_source_concept_id,plan_concept_id,plan_source_value,plan_source_concept_id,sponsor_concept_id,sponsor_source_value,sponsor_source_concept_id,family_source_value,stop_reason_concept_id,stop_reason_source_value,stop_reason_source_concept_id)
select "payer_plan_period_id","person_id","payer_plan_period_start_date","payer_plan_period_end_date","payer_concept_id",left("payer_source_value",512),"payer_source_concept_id","plan_concept_id","plan_source_value","plan_source_concept_id","sponsor_concept_id","sponsor_source_value","sponsor_source_concept_id","family_source_value","stop_reason_concept_id","stop_reason_source_value","stop_reason_source_concept_id" 
    from "ohdsi"."transferpayer_plan_period";


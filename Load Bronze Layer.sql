-- Databricks notebook source
-- DBTITLE 1,Load Allergies Table
insert into dbrx1.healthcare_silver.allergies select *,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.allergies

-- COMMAND ----------

-- DBTITLE 1,Load Careplans Table
insert into dbrx1.healthcare_silver.careplans select *,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.careplans

-- COMMAND ----------

-- DBTITLE 1,Load Conditions Table
insert into dbrx1.healthcare_silver.conditions select *,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.conditions

-- COMMAND ----------

-- DBTITLE 1,Load Device Table
insert into dbrx1.healthcare_silver.device select string(date(`START`)) as `START`,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.device

-- COMMAND ----------

-- DBTITLE 1,Load Encounters Table
insert into dbrx1.healthcare_silver.encounters select id,string(date(`START`)) as `START`,string(date(`STOP`)) as `STOP`,PATIENT,ORGANIZATION,`PROVIDER`,PAYER,ENCOUNTERCLASS,CODE,DESCRIPTION,BASE_ENCOUNTER_COST,TOTAL_CLAIM_COST,PAYER_COVERAGE,REASONCODE,REASONDESCRIPTION,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.encounters

-- COMMAND ----------

-- DBTITLE 1,Load Imaging Studies Table
insert into dbrx1.healthcare_silver.imaging_studies select id,string(date(`DATE`)) as `DATE`,PATIENT,ENCOUNTER,BODYSITE_CODE,BODYSITE_DESCRIPTION,modality_code,modality_description,SOP_CODE,SOP_DESCRIPTION,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.imaging_studies

-- COMMAND ----------

-- DBTITLE 1,Load Immunizations  Table
insert into dbrx1.healthcare_silver.immunizations select string(date(`DATE`)) as `DATE`,PATIENT,ENCOUNTER,code,DESCRIPTION,base_cost,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.immunizations

-- COMMAND ----------

-- DBTITLE 1,Load Medications Table
insert into dbrx1.healthcare_silver.medications select string(date(`start`)) as `START`,string(date(`stop`)) as `STOP`,PATIENT,payer,ENCOUNTER,code,DESCRIPTION,base_cost,payer_coverage,dispenses,totalcost,reasoncode,reasondescription,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.medications

-- COMMAND ----------

-- DBTITLE 1,Load Observations Table
insert into dbrx1.healthcare_silver.observations select *,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.observations

-- COMMAND ----------

-- DBTITLE 1,Load Organizations Table
insert into dbrx1.healthcare_silver.organizations select id,name,address,city,state,zip,string(phone),revenue,utilization,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.organizations

-- COMMAND ----------

select * from dbrx1.healthcare_bronze.organizations

-- COMMAND ----------

-- DBTITLE 1,Load Patients Table
insert into dbrx1.healthcare_silver.patients select id,string(BIRTHDATE),string(deathdate),ssn,drivers,passport,prefix,`first`,`last`,suffix,maiden,marital,race,ethnicity,GENDER,BIRTHPLACE,ADDRESS,CITY,state,county,zip,lat,lon,HEALTHCARE_expenses,healthcare_coverage,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.patients

-- COMMAND ----------

-- DBTITLE 1,Load payer_transitions table
insert into dbrx1.healthcare_silver.payer_transitions select PATIENT,start_year,end_year,payer,ownership,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.payer_transitions

-- COMMAND ----------

-- DBTITLE 1,Load Payers Table
insert into dbrx1.healthcare_silver.payers select *,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.payers

-- COMMAND ----------

-- DBTITLE 1,Load Procedures Table
insert into dbrx1.healthcare_silver.procedures select *,current_timestamp(),current_timestamp() from dbrx1.healthcare_bronze.procedures

-- COMMAND ----------

-- DBTITLE 1,Load Providers Table
insert into dbrx1.healthcare_silver.providers select * from dbrx1.healthcare_bronze.providers

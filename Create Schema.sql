-- Databricks notebook source
create schema  if not exists healthcare_bronze;
create schema if not exists healthcare_silver;

-- COMMAND ----------

create table if not exists healthcare_bronze.patients (
  Id STRING,
  BIRTHDATE STRING,
  DEATHDATE STRING,
  SSN STRING,
  DRIVERS STRING,
  PASSPORT STRING,
  PREFIX STRING,
  FIRST STRING,
  LAST STRING,
  SUFFIX STRING,
  MAIDEN STRING,
  MARITAL STRING,
  RACE STRING,
  ETHNICITY STRING,
  GENDER STRING,
  BIRTHPLACE STRING,
  ADDRESS STRING,
  CITY STRING,
  STATE STRING,
  COUNTY STRING,
  ZIP int,
  LAT DOUBLE,
  LON DOUBLE,
  HEALTHCARE_EXPENSES FLOAT,
  HEALTHCARE_COVERAGE FLOAT
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_bronze.allergies (
  START STRING,
  STOP STRING,
  PATIENT STRING,
  ENCOUNTER STRING,
  CODE STRING,
  DESCRIPTION STRING
)

-- COMMAND ----------

create table if not exists healthcare_bronze.careplans (
  Id STRING,
  START STRING,
  STOP STRING,
  PATIENT STRING,
  ENCOUNTER STRING,
  CODE STRING,
  DESCRIPTION STRING,
  REASONCODE STRING,
  REASONDESCRIPTION STRING
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_bronze.conditions (
  START STRING,
  STOP STRING,
  PATIENT STRING,
  ENCOUNTER STRING,
  CODE STRING,
  DESCRIPTION STRING
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_bronze.device (
  START STRING,
  STOP STRING,
  PATIENT STRING,
  ENCOUNTER STRING,
  CODE STRING,
  DESCRIPTION STRING,
  UDI STRING
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_bronze.encounters (
  Id STRING,
  START STRING,
  STOP STRING,
  PATIENT STRING,
  ORGANIZATION STRING,
  PROVIDER STRING,
  PAYER STRING,
  ENCOUNTERCLASS STRING,
  CODE STRING,
  DESCRIPTION STRING,
  BASE_ENCOUNTER_COST FLOAT,
  TOTAL_CLAIM_COST FLOAT,
  PAYER_COVERAGE FLOAT,
  REASONCODE STRING,
  REASONDESCRIPTION STRING
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_bronze.imaging_studies (
  Id STRING,
  DATE STRING,
  PATIENT STRING,
  ENCOUNTER STRING,
  BODYSITE_CODE STRING,
  BODYSITE_DESCRIPTION STRING,
  MODALITY_CODE STRING,
  MODALITY_DESCRIPTION STRING,
  SOP_CODE STRING,
  SOP_DESCRIPTION STRING
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_bronze.immunizations (
    DATE STRING,
    PATIENT STRING,
    ENCOUNTER STRING,
    CODE INT,
    DESCRIPTION STRING,
    BASE_COST DOUBLE
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_bronze.medications (
    START STRING,
    STOP STRING,
    PATIENT STRING,
    PAYER STRING,
    ENCOUNTER STRING,
    CODE INT,
    DESCRIPTION STRING,
    BASE_COST DOUBLE,
    PAYER_COVERAGE DOUBLE,
    DISPENSES INT,
    TOTALCOST DOUBLE,
    REASONCODE STRING,
    REASONDESCRIPTION STRING
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS healthcare_bronze.observations (
    date TIMESTAMP,
    patient STRING,
    encounter STRING,
    code STRING,
    description STRING,
    value string,
    units STRING,
    type STRING
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS healthcare_bronze.organizations (
    id STRING,
    name STRING,
    address STRING,
    city STRING,
    state STRING,
    zip STRING,
    lat DOUBLE,
    lon DOUBLE,
    phone STRING,
    revenue DOUBLE,
    utilization INT
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_bronze.payer_transitions (
    PatientID STRING,
    StartYear INT,
    EndYear INT,
    PayerID STRING,
    Ownership STRING
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_bronze.payers (
    Id STRING,
    Name STRING,
    Address STRING,
    City STRING,
    StateHeadquartered STRING,
    Zip STRING,
    Phone STRING,
    AmountCovered DOUBLE,
    AmountUncovered DOUBLE,
    Revenue DOUBLE,
    CoveredEncounters INT,
    UncoveredEncounters INT,
    CoveredMedications INT,
    UncoveredMedications INT,
    CoveredProcedures INT,
    UncoveredProcedures INT,
    CoveredImmunizations INT,
    UncoveredImmunizations INT,
    UniqueCustomers INT,
    QolsAvg DOUBLE,
    MemberMonths INT
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS healthcare_bronze.procedures (
    DATE TIMESTAMP,
    PATIENT STRING,
    ENCOUNTER STRING,
    CODE STRING,
    DESCRIPTION STRING,
    BASE_COST DOUBLE,
    REASONCODE STRING,
    REASONDESCRIPTION STRING
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS healthcare_bronze.providers (
    Id STRING,
    ORGANIZATION STRING,
    NAME STRING,
    GENDER STRING,
    SPECIALITY STRING,
    ADDRESS STRING,
    CITY STRING,
    STATE STRING,
    ZIP STRING,
    LAT DOUBLE,
    LON DOUBLE,
    UTILIZATION INT
)

-- COMMAND ----------

create table if not exists healthcare_silver.patients (
  Id STRING,
  BIRTHDATE STRING,
  DEATHDATE STRING,
  SSN STRING,
  DRIVERS STRING,
  PASSPORT STRING,
  PREFIX STRING,
  FIRST STRING,
  LAST STRING,
  SUFFIX STRING,
  MAIDEN STRING,
  MARITAL STRING,
  RACE STRING,
  ETHNICITY STRING,
  GENDER STRING,
  BIRTHPLACE STRING,
  ADDRESS STRING,
  CITY STRING,
  STATE STRING,
  COUNTY STRING,
  ZIP int,
  LAT DOUBLE,
  LON DOUBLE,
  HEALTHCARE_EXPENSES FLOAT,
  HEALTHCARE_COVERAGE FLOAT,
  last_added_dt_time timestamp,
  last_updated_dt_time timestamp
)

-- COMMAND ----------

-- DBTITLE 1,Silver.Allergies
CREATE TABLE if not exists healthcare_silver.allergies (
  START STRING,
  STOP STRING,
  PATIENT STRING,
  ENCOUNTER STRING,
  CODE STRING,
  DESCRIPTION STRING,
  last_added_dt_time timestamp,
  last_updated_dt_time timestamp
)

-- COMMAND ----------

create table if not exists healthcare_silver.careplans (
  Id STRING,
  START STRING,
  STOP STRING,
  PATIENT STRING,
  ENCOUNTER STRING,
  CODE STRING,
  DESCRIPTION STRING,
  REASONCODE STRING,
  REASONDESCRIPTION STRING,
  last_added_dt_time timestamp,
  last_updated_dt_time timestamp
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_silver.conditions (
  START STRING,
  STOP STRING,
  PATIENT STRING,
  ENCOUNTER STRING,
  CODE STRING,
  DESCRIPTION STRING,
  last_added_dt_time timestamp,
  last_updated_dt_time timestamp
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_silver.device (
  START STRING,
  STOP STRING,
  PATIENT STRING,
  ENCOUNTER STRING,
  CODE STRING,
  DESCRIPTION STRING,
  last_added_dt_time timestamp,
  last_updated_dt_time timestamp
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_silver.encounters (
  Id STRING,
  START STRING,
  STOP STRING,
  PATIENT STRING,
  ORGANIZATION STRING,
  PROVIDER STRING,
  PAYER STRING,
  ENCOUNTERCLASS STRING,
  CODE STRING,
  DESCRIPTION STRING,
  BASE_ENCOUNTER_COST FLOAT,
  TOTAL_CLAIM_COST FLOAT,
  PAYER_COVERAGE FLOAT,
  REASONCODE STRING,
  REASONDESCRIPTION STRING,
  last_added_dt_time timestamp,
  last_updated_dt_time timestamp
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_silver.imaging_studies (
  Id STRING,
  DATE STRING,
  PATIENT STRING,
  ENCOUNTER STRING,
  BODYSITE_CODE STRING,
  BODYSITE_DESCRIPTION STRING,
  MODALITY_CODE STRING,
  MODALITY_DESCRIPTION STRING,
  SOP_CODE STRING,
  SOP_DESCRIPTION STRING,
  last_added_dt_time timestamp,
  last_updated_dt_time timestamp
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_silver.immunizations (
    DATE STRING,
    PATIENT STRING,
    ENCOUNTER STRING,
    CODE INT,
    DESCRIPTION STRING,
    BASE_COST DOUBLE,
    last_added_dt_time timestamp,
    last_updated_dt_time timestamp
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_silver.medications (
    START STRING,
    STOP STRING,
    PATIENT STRING,
    PAYER STRING,
    ENCOUNTER STRING,
    CODE INT,
    DESCRIPTION STRING,
    BASE_COST DOUBLE,
    PAYER_COVERAGE DOUBLE,
    DISPENSES INT,
    TOTALCOST DOUBLE,
    REASONCODE STRING,
    REASONDESCRIPTION STRING,
     last_added_dt_time timestamp,
    last_updated_dt_time timestamp
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS healthcare_silver.observations (
    date TIMESTAMP,
    patient STRING,
    encounter STRING,
    code STRING,
    description STRING,
    value string,
    units STRING,
    type STRING,
    last_added_dt_time timestamp,
    last_updated_dt_time timestamp
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS healthcare_silver.organizations (
    id STRING,
    name STRING,
    address STRING,
    city STRING,
    state STRING,
    zip STRING,
    phone STRING,
    revenue DOUBLE,
    utilization INT,
    last_added_dt_time timestamp,
    last_updated_dt_time timestamp
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_silver.payer_transitions (
    PatientID STRING,
    StartYear INT,
    EndYear INT,
    PayerID STRING,
    Ownership STRING,
    last_added_dt_time timestamp,
    last_updated_dt_time timestamp
)

-- COMMAND ----------

CREATE TABLE if not exists healthcare_silver.payers (
    Id STRING,
    Name STRING,
    Address STRING,
    City STRING,
    StateHeadquartered STRING,
    Zip STRING,
    Phone STRING,
    AmountCovered DOUBLE,
    AmountUncovered DOUBLE,
    Revenue DOUBLE,
    CoveredEncounters INT,
    UncoveredEncounters INT,
    CoveredMedications INT,
    UncoveredMedications INT,
    CoveredProcedures INT,
    UncoveredProcedures INT,
    CoveredImmunizations INT,
    UncoveredImmunizations INT,
    UniqueCustomers INT,
    QolsAvg DOUBLE,
    MemberMonths INT,
    last_added_dt_time timestamp,
    last_updated_dt_time timestamp
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS healthcare_silver.procedures (
    DATE TIMESTAMP,
    PATIENT STRING,
    ENCOUNTER STRING,
    CODE STRING,
    DESCRIPTION STRING,
    BASE_COST DOUBLE,
    REASONCODE STRING,
    REASONDESCRIPTION STRING,
    last_added_dt_time timestamp,
    last_updated_dt_time timestamp
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS healthcare_silver.providers (
    Id STRING,
    ORGANIZATION STRING,
    NAME STRING,
    GENDER STRING,
    SPECIALITY STRING,
    ADDRESS STRING,
    CITY STRING,
    STATE STRING,
    ZIP STRING,
    UTILIZATION INT,
    last_added_dt_time timestamp,
    last_updated_dt_time timestamp
)

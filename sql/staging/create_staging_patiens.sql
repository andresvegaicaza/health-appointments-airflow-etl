-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS staging;

-- Staging table
CREATE TABLE IF NOT EXISTS staging.stg_patients (
    patient_id        VARCHAR PRIMARY KEY,
    first_name        VARCHAR,
    last_name         VARCHAR,
    email             VARCHAR,
    phone             VARCHAR,
    address           VARCHAR,
    city              VARCHAR,
    state             VARCHAR,
    country           VARCHAR,
    created_at_utc    TIMESTAMP
);

-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS marts;

-- Fact table
CREATE TABLE IF NOT EXISTS marts.fact_appointments (
    appointment_id        VARCHAR PRIMARY KEY,
    patient_id            VARCHAR REFERENCES staging.stg_patients(patient_id),
    doctor_name           VARCHAR,
    specialty             VARCHAR,
    status                VARCHAR,
    appointment_start_utc TIMESTAMP,
    appointment_end_utc   TIMESTAMP,
    created_at_utc        TIMESTAMP
);

-- Analytics view in marts schema
CREATE OR REPLACE VIEW marts.vw_appointments_by_doctor AS
SELECT
    doctor_name,
    specialty,
    COUNT(*) AS total_appointments,
    SUM(CASE WHEN status = 'no_show' THEN 1 ELSE 0 END) AS no_shows
FROM marts.fact_appointments
GROUP BY doctor_name, specialty;

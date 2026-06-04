from fastapi import APIRouter
import db

router = APIRouter()


@router.get("/summary")
def summary():
    row = db.query("""
        SELECT
            COUNT(*)                                                        AS total_alerts,
            SUM(severity = 'CRITICAL')                                      AS critical_count,
            SUM(severity = 'HIGH')                                          AS high_count,
            SUM(severity = 'WARNING')                                       AS warning_count,
            SUM(is_email_sent = 1)                                          AS emails_sent,
            SUM(ts >= NOW() - INTERVAL 1 HOUR)                              AS last_1h,
            SUM(ts >= NOW() - INTERVAL 24 HOUR)                             AS last_24h,
            (SELECT COALESCE(SUM(anomaly_count), 0)    FROM analytics_vitals_patient_summary) AS vitals_anomalies,
            (SELECT COALESCE(SUM(critical_count), 0)   FROM analytics_lab_test_summary)       AS critical_labs,
            (SELECT COALESCE(SUM(code_count), 0)       FROM analytics_icu_code_summary)       AS icu_activations
        FROM patient_alerts
    """)
    return row[0] if row else {}


@router.get("/alerts-by-type")
def alerts_by_type():
    return db.query("""
        SELECT alert_type, severity, COUNT(*) AS count
        FROM patient_alerts
        GROUP BY alert_type, severity
        ORDER BY count DESC
    """)


@router.get("/recent-alerts")
def recent_alerts():
    return db.query("""
        SELECT alert_id, alert_type, severity, patient_id, doctor_id,
               hospital, ward, alert_message, source_topic, is_email_sent,
               ts
        FROM patient_alerts
        ORDER BY ts DESC
        LIMIT 50
    """)


@router.get("/vitals-summary")
def vitals_summary():
    return db.query("""
        SELECT patient_id, total_readings, anomaly_count, anomaly_rate_pct,
               avg_heart_rate, avg_spo2, avg_systolic, avg_diastolic,
               avg_temperature, avg_respiratory_rate
        FROM analytics_vitals_patient_summary
        ORDER BY anomaly_rate_pct DESC
    """)


@router.get("/lab-summary")
def lab_summary():
    return db.query("""
        SELECT test_name, total_tests, normal_count, low_count,
               high_count, critical_count, critical_rate_pct,
               avg_amount, total_revenue
        FROM analytics_lab_test_summary
        ORDER BY critical_count DESC
    """)


@router.get("/icu-summary")
def icu_summary():
    return db.query("""
        SELECT code_type, severity, code_count, total_amount, avg_amount
        FROM analytics_icu_code_summary
        ORDER BY code_count DESC
    """)

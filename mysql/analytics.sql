-- ─────────────────────────────────────────────────────────────────────────────
-- Analytics Tables Schema
-- Written by: Spark jobs (Airflow DAG: healthcare_analytics_pipeline)
-- Spark write mode: overwrite + truncateTable=true (schema preserved on re-run)
-- ─────────────────────────────────────────────────────────────────────────────

USE healthcare;

-- ═════════════════════════════════════════════════════════════════════════════
-- FINANCIAL ANALYTICS  (Spark job: financial_analytics.py)
-- ═════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_revenue_by_doctor
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_revenue_by_doctor (
    doctor_id               VARCHAR(10)      DEFAULT NULL,
    full_name               VARCHAR(100)     NOT NULL,
    specialization          VARCHAR(100)     DEFAULT NULL,
    hospital_branch         VARCHAR(100)     DEFAULT NULL,
    total_bills             BIGINT           NOT NULL,
    total_revenue           DECIMAL(21, 2)   DEFAULT NULL,
    avg_bill_amount         DECIMAL(11, 2)   DEFAULT NULL,
    max_bill_amount         DECIMAL(11, 2)   DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_revenue_by_specialization
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_revenue_by_specialization (
    specialization          VARCHAR(100)     DEFAULT NULL,
    doctor_count            BIGINT           NOT NULL,
    total_appointments      BIGINT           NOT NULL,
    total_revenue           DECIMAL(21, 2)   DEFAULT NULL,
    avg_revenue_per_appt    DECIMAL(11, 2)   DEFAULT NULL,
    avg_revenue_per_doc     DECIMAL(22, 2)   DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_revenue_by_branch
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_revenue_by_branch (
    hospital_branch         VARCHAR(100)     DEFAULT NULL,
    doctor_count            BIGINT           NOT NULL,
    total_appointments      BIGINT           NOT NULL,
    total_revenue           DECIMAL(21, 2)   DEFAULT NULL,
    avg_revenue_per_appt    DECIMAL(11, 2)   DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_monthly_revenue
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_monthly_revenue (
    year                    INT              DEFAULT NULL,
    month                   INT              DEFAULT NULL,
    bill_count              BIGINT           NOT NULL,
    total_revenue           DECIMAL(21, 2)   DEFAULT NULL,
    avg_revenue             DECIMAL(11, 2)   DEFAULT NULL,
    mom_growth_pct          DECIMAL(29, 2)   DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_billing_payment
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_billing_payment (
    payment_method          VARCHAR(50)      DEFAULT NULL,
    payment_status          VARCHAR(20)      DEFAULT NULL,
    bill_count              BIGINT           NOT NULL,
    total_amount            DECIMAL(21, 2)   DEFAULT NULL,
    avg_amount              DECIMAL(11, 2)   DEFAULT NULL,
    pct_of_total_revenue    DECIMAL(28, 2)   DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_treatment_cost
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_treatment_cost (
    treatment_type          VARCHAR(100)     DEFAULT NULL,
    treatment_count         BIGINT           NOT NULL,
    avg_cost                DECIMAL(11, 2)   DEFAULT NULL,
    min_cost                DECIMAL(11, 2)   DEFAULT NULL,
    max_cost                DECIMAL(11, 2)   DEFAULT NULL,
    total_cost              DECIMAL(21, 2)   DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_outstanding_payments
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_outstanding_payments (
    payment_status          VARCHAR(20)      DEFAULT NULL,
    bill_count              BIGINT           NOT NULL,
    total_outstanding       DECIMAL(21, 2)   DEFAULT NULL,
    avg_outstanding         DECIMAL(11, 2)   DEFAULT NULL,
    oldest_bill_date        DATE             DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ═════════════════════════════════════════════════════════════════════════════
-- OPERATIONAL ANALYTICS  (Spark job: operational_analytics.py)
-- ═════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_doctor_workload
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_doctor_workload (
    doctor_id               VARCHAR(10)      DEFAULT NULL,
    full_name               VARCHAR(100)     NOT NULL,
    specialization          VARCHAR(100)     DEFAULT NULL,
    hospital_branch         VARCHAR(100)     DEFAULT NULL,
    total_appointments      BIGINT           NOT NULL,
    completed_appointments  BIGINT           DEFAULT NULL,
    unique_patients         BIGINT           NOT NULL,
    no_show_count           BIGINT           DEFAULT NULL,
    cancellation_count      BIGINT           DEFAULT NULL,
    no_show_rate_pct        DOUBLE           DEFAULT NULL,
    cancellation_rate_pct   DOUBLE           DEFAULT NULL,
    completion_rate_pct     DOUBLE           DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_appointment_status
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_appointment_status (
    doctor_id               VARCHAR(10)      DEFAULT NULL,
    full_name               VARCHAR(100)     NOT NULL,
    specialization          VARCHAR(100)     DEFAULT NULL,
    status                  VARCHAR(20)      DEFAULT NULL,
    count                   BIGINT           NOT NULL,
    pct_of_total            DOUBLE           DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_peak_hours
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_peak_hours (
    hour_of_day             INT              DEFAULT NULL,
    appointment_count       BIGINT           NOT NULL,
    completed_count         BIGINT           DEFAULT NULL,
    no_show_count           BIGINT           DEFAULT NULL,
    completion_rate_pct     DOUBLE           DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_top_doctors_scorecard
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_top_doctors_scorecard (
    doctor_id               VARCHAR(10)      DEFAULT NULL,
    full_name               VARCHAR(100)     NOT NULL,
    specialization          VARCHAR(100)     DEFAULT NULL,
    hospital_branch         VARCHAR(100)     DEFAULT NULL,
    total_revenue           DECIMAL(21, 2)   DEFAULT NULL,
    unique_patients         BIGINT           NOT NULL,
    completion_rate_pct     DOUBLE           DEFAULT NULL,
    revenue_rank            INT              NOT NULL,
    completion_rank         INT              NOT NULL,
    overall_score           DOUBLE           DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ═════════════════════════════════════════════════════════════════════════════
-- PATIENT ANALYTICS  (Spark job: patient_analytics.py)
-- ═════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_patient_age_groups
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_patient_age_groups (
    age_group               VARCHAR(20)      NOT NULL,
    patient_count           BIGINT           NOT NULL,
    total_appointments      BIGINT           DEFAULT NULL,
    total_spend             DECIMAL(32, 2)   DEFAULT NULL,
    avg_spend               DECIMAL(22, 2)   DEFAULT NULL,
    most_common_reason      VARCHAR(255)     DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_patient_retention
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_patient_retention (
    visit_segment           VARCHAR(50)      NOT NULL,
    patient_count           BIGINT           NOT NULL,
    avg_spend               DECIMAL(22, 2)   DEFAULT NULL,
    total_revenue           DECIMAL(32, 2)   DEFAULT NULL,
    pct_of_patients         DOUBLE           DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_new_patient_trend
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_new_patient_trend (
    year                    INT              DEFAULT NULL,
    month                   INT              DEFAULT NULL,
    new_patients            BIGINT           NOT NULL,
    male_count              BIGINT           DEFAULT NULL,
    female_count            BIGINT           DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_patient_spending
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_patient_spending (
    insurance_provider      VARCHAR(100)     DEFAULT NULL,
    patient_count           BIGINT           NOT NULL,
    avg_age                 DOUBLE           DEFAULT NULL,
    male_count              BIGINT           DEFAULT NULL,
    female_count            BIGINT           DEFAULT NULL,
    avg_spend               DECIMAL(22, 2)   DEFAULT NULL,
    total_spend             DECIMAL(32, 2)   DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ═════════════════════════════════════════════════════════════════════════════
-- MONITORING ANALYTICS  (Spark job: monitoring_analytics.py)
-- Source data: Flink streaming jobs (monitoring_job + alerts_job)
-- ═════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_vitals_patient_summary
-- Anomaly thresholds (matching Flink monitoring_job):
--   HR < 40 or > 150 bpm | SpO2 < 90% | Temp < 35 or > 39 °C
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_vitals_patient_summary (
    patient_id              VARCHAR(10)      NOT NULL,
    total_readings          INT              NOT NULL DEFAULT 0,
    anomaly_count           INT              NOT NULL DEFAULT 0,
    hr_anomaly_count        INT              NOT NULL DEFAULT 0,
    spo2_anomaly_count      INT              NOT NULL DEFAULT 0,
    temp_anomaly_count      INT              NOT NULL DEFAULT 0,
    anomaly_rate_pct        DECIMAL(5, 2)    NOT NULL DEFAULT 0.00,
    avg_heart_rate          DECIMAL(6, 1)    NOT NULL DEFAULT 0.0,
    avg_spo2                DECIMAL(5, 1)    NOT NULL DEFAULT 0.0,
    avg_systolic            DECIMAL(6, 1)    NOT NULL DEFAULT 0.0,
    avg_diastolic           DECIMAL(6, 1)    NOT NULL DEFAULT 0.0,
    avg_temperature         DECIMAL(5, 2)    NOT NULL DEFAULT 0.00,
    avg_respiratory_rate    DECIMAL(5, 1)    NOT NULL DEFAULT 0.0,
    last_updated            TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_lab_test_summary
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_lab_test_summary (
    test_name               VARCHAR(100)     NOT NULL,
    total_tests             INT              NOT NULL DEFAULT 0,
    normal_count            INT              NOT NULL DEFAULT 0,
    low_count               INT              NOT NULL DEFAULT 0,
    high_count              INT              NOT NULL DEFAULT 0,
    critical_count          INT              NOT NULL DEFAULT 0,
    critical_rate_pct       DECIMAL(5, 2)    NOT NULL DEFAULT 0.00,
    avg_amount              DECIMAL(10, 2)   NOT NULL DEFAULT 0.00,
    total_revenue           DECIMAL(12, 2)   NOT NULL DEFAULT 0.00,
    last_updated            TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (test_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_hospital_event_summary
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_hospital_event_summary (
    event_type              VARCHAR(50)      NOT NULL,
    event_count             INT              NOT NULL DEFAULT 0,
    total_amount            DECIMAL(12, 2)   NOT NULL DEFAULT 0.00,
    avg_amount              DECIMAL(10, 2)   NOT NULL DEFAULT 0.00,
    last_updated            TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (event_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_icu_code_summary
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_icu_code_summary (
    code_type               VARCHAR(50)      NOT NULL,
    severity                VARCHAR(20)      NOT NULL,
    code_count              INT              NOT NULL DEFAULT 0,
    total_amount            DECIMAL(12, 2)   NOT NULL DEFAULT 0.00,
    avg_amount              DECIMAL(10, 2)   NOT NULL DEFAULT 0.00,
    last_updated            TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (code_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_department_activity
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_department_activity (
    department_id           VARCHAR(10)      NOT NULL,
    department_name         VARCHAR(100)     NOT NULL,
    hospital_branch         VARCHAR(100)     DEFAULT NULL,
    total_events            INT              NOT NULL DEFAULT 0,
    total_icu_codes         INT              NOT NULL DEFAULT 0,
    critical_icu_count      INT              NOT NULL DEFAULT 0,
    total_event_amount      DECIMAL(12, 2)   NOT NULL DEFAULT 0.00,
    total_icu_amount        DECIMAL(12, 2)   NOT NULL DEFAULT 0.00,
    total_amount            DECIMAL(12, 2)   NOT NULL DEFAULT 0.00,
    last_updated            TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (department_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: analytics_patient_alert_summary
-- Source: patient_alerts (written by Flink alerts_job)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_patient_alert_summary (
    patient_id              VARCHAR(10)      DEFAULT NULL,
    total_alerts            BIGINT           NOT NULL,
    critical_count          BIGINT           DEFAULT NULL,
    high_count              BIGINT           DEFAULT NULL,
    warning_count           BIGINT           DEFAULT NULL,
    emails_sent             BIGINT           DEFAULT NULL,
    distinct_alert_types    BIGINT           NOT NULL,
    latest_alert_ts         TIMESTAMP        NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

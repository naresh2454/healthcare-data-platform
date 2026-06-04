-- ─────────────────────────────────────────────────────────────────────────────
-- Healthcare Database Schema
-- ─────────────────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS healthcare
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE healthcare;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: patients
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS patients (
    patient_id          VARCHAR(10)      NOT NULL,
    first_name          VARCHAR(50)      NOT NULL,
    last_name           VARCHAR(50)      NOT NULL,
    gender              CHAR(1)          NOT NULL,
    date_of_birth       DATE             NOT NULL,
    contact_number      VARCHAR(15)      NOT NULL,
    address             VARCHAR(255)     DEFAULT NULL,
    registration_date   DATE             NOT NULL,
    insurance_provider  VARCHAR(100)     DEFAULT NULL,
    insurance_number    VARCHAR(20)      DEFAULT NULL,
    email               VARCHAR(100)     DEFAULT NULL,
    PRIMARY KEY (patient_id),
    UNIQUE KEY uq_patient_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: doctors
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS doctors (
    doctor_id           VARCHAR(10)      NOT NULL,
    first_name          VARCHAR(50)      NOT NULL,
    last_name           VARCHAR(50)      NOT NULL,
    specialization      VARCHAR(100)     NOT NULL,
    phone_number        VARCHAR(15)      NOT NULL,
    years_experience    TINYINT UNSIGNED NOT NULL DEFAULT 0,
    hospital_branch     VARCHAR(100)     DEFAULT NULL,
    email               VARCHAR(100)     DEFAULT NULL,
    PRIMARY KEY (doctor_id),
    UNIQUE KEY uq_doctor_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: appointments
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS appointments (
    appointment_id      VARCHAR(10)      NOT NULL,
    patient_id          VARCHAR(10)      NOT NULL,
    doctor_id           VARCHAR(10)      NOT NULL,
    appointment_date    DATE             NOT NULL,
    appointment_time    TIME             NOT NULL,
    reason_for_visit    VARCHAR(255)     DEFAULT NULL,
    status              VARCHAR(20)      NOT NULL,
    PRIMARY KEY (appointment_id),
    KEY idx_appt_patient  (patient_id),
    KEY idx_appt_doctor   (doctor_id),
    KEY idx_appt_date     (appointment_date),
    CONSTRAINT fk_appt_patient FOREIGN KEY (patient_id)
        REFERENCES patients (patient_id) ON DELETE CASCADE,
    CONSTRAINT fk_appt_doctor  FOREIGN KEY (doctor_id)
        REFERENCES doctors  (doctor_id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: treatments
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS treatments (
    treatment_id        VARCHAR(10)      NOT NULL,
    appointment_id      VARCHAR(10)      NOT NULL,
    treatment_type      VARCHAR(100)     NOT NULL,
    description         VARCHAR(255)     DEFAULT NULL,
    cost                DECIMAL(10, 2)   NOT NULL DEFAULT 0.00,
    treatment_date      DATE             NOT NULL,
    PRIMARY KEY (treatment_id),
    KEY idx_treat_appointment (appointment_id),
    KEY idx_treat_date        (treatment_date),
    CONSTRAINT fk_treat_appointment FOREIGN KEY (appointment_id)
        REFERENCES appointments (appointment_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: billing
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS billing (
    bill_id             VARCHAR(10)      NOT NULL,
    patient_id          VARCHAR(10)      NOT NULL,
    treatment_id        VARCHAR(10)      NOT NULL,
    bill_date           DATE             NOT NULL,
    amount              DECIMAL(10, 2)   NOT NULL DEFAULT 0.00,
    payment_method      VARCHAR(50)      DEFAULT NULL,
    payment_status      VARCHAR(20)      NOT NULL,
    PRIMARY KEY (bill_id),
    KEY idx_bill_patient   (patient_id),
    KEY idx_bill_treatment (treatment_id),
    KEY idx_bill_date      (bill_date),
    CONSTRAINT fk_bill_patient   FOREIGN KEY (patient_id)
        REFERENCES patients   (patient_id) ON DELETE CASCADE,
    CONSTRAINT fk_bill_treatment FOREIGN KEY (treatment_id)
        REFERENCES treatments (treatment_id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: departments  (master data — must exist before hospital_events/icu_codes)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS departments (
    department_id    VARCHAR(10)      NOT NULL,
    department_name  VARCHAR(100)     NOT NULL,
    hospital_branch  VARCHAR(100)     DEFAULT NULL,
    PRIMARY KEY (department_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: patient_vitals
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS patient_vitals (
    event_id              VARCHAR(36)      NOT NULL,
    patient_id            VARCHAR(10)      NOT NULL,
    hospital              VARCHAR(100)     DEFAULT NULL,
    ward                  VARCHAR(50)      DEFAULT NULL,
    heart_rate            INT              NOT NULL,
    spo2                  DECIMAL(5, 1)    NOT NULL,
    systolic              INT              NOT NULL,
    diastolic             INT              NOT NULL,
    temperature_celsius   DECIMAL(4, 1)    NOT NULL,
    respiratory_rate      INT              NOT NULL,
    is_anomaly            TINYINT(1)       NOT NULL DEFAULT 0,
    ts                    DATETIME         NOT NULL,
    PRIMARY KEY (event_id),
    KEY idx_vitals_patient  (patient_id),
    KEY idx_vitals_ts       (ts),
    CONSTRAINT fk_vitals_patient FOREIGN KEY (patient_id)
        REFERENCES patients (patient_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: lab_reports
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lab_reports (
    report_id       VARCHAR(36)      NOT NULL,
    patient_id      VARCHAR(10)      NOT NULL,
    doctor_id       VARCHAR(10)      NOT NULL,
    hospital        VARCHAR(100)     DEFAULT NULL,
    test_name       VARCHAR(100)     NOT NULL,
    value           DECIMAL(10, 3)   NOT NULL,
    unit            VARCHAR(20)      DEFAULT NULL,
    normal_range    VARCHAR(50)      DEFAULT NULL,
    flag            VARCHAR(20)      NOT NULL,
    amount          DECIMAL(10, 2)   NOT NULL DEFAULT 0.00,
    ts              DATETIME         NOT NULL,
    PRIMARY KEY (report_id),
    KEY idx_lab_patient  (patient_id),
    KEY idx_lab_doctor   (doctor_id),
    KEY idx_lab_ts       (ts),
    CONSTRAINT fk_lab_patient FOREIGN KEY (patient_id)
        REFERENCES patients (patient_id) ON DELETE CASCADE,
    CONSTRAINT fk_lab_doctor  FOREIGN KEY (doctor_id)
        REFERENCES doctors  (doctor_id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: hospital_events
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hospital_events (
    event_id        VARCHAR(36)      NOT NULL,
    patient_id      VARCHAR(10)      NOT NULL,
    department_id   VARCHAR(10)      NOT NULL,
    hospital        VARCHAR(100)     DEFAULT NULL,
    ward            VARCHAR(50)      DEFAULT NULL,
    event_type      VARCHAR(50)      NOT NULL,
    amount          DECIMAL(10, 2)   NOT NULL DEFAULT 0.00,
    ts              DATETIME         NOT NULL,
    PRIMARY KEY (event_id),
    KEY idx_hevt_patient  (patient_id),
    KEY idx_hevt_dept     (department_id),
    KEY idx_hevt_ts       (ts),
    CONSTRAINT fk_hevt_patient FOREIGN KEY (patient_id)
        REFERENCES patients     (patient_id)    ON DELETE CASCADE,
    CONSTRAINT fk_hevt_dept    FOREIGN KEY (department_id)
        REFERENCES departments  (department_id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: icu_codes
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS icu_codes (
    code_id         VARCHAR(36)      NOT NULL,
    patient_id      VARCHAR(10)      NOT NULL,
    department_id   VARCHAR(10)      NOT NULL,
    hospital        VARCHAR(100)     DEFAULT NULL,
    ward            VARCHAR(50)      DEFAULT NULL,
    code_type       VARCHAR(50)      NOT NULL,
    severity        VARCHAR(20)      NOT NULL,
    amount          DECIMAL(10, 2)   NOT NULL DEFAULT 0.00,
    status          VARCHAR(20)      NOT NULL DEFAULT 'Activated',
    ts              DATETIME         NOT NULL,
    PRIMARY KEY (code_id),
    KEY idx_icu_patient  (patient_id),
    KEY idx_icu_dept     (department_id),
    KEY idx_icu_ts       (ts),
    CONSTRAINT fk_icu_patient FOREIGN KEY (patient_id)
        REFERENCES patients     (patient_id)    ON DELETE CASCADE,
    CONSTRAINT fk_icu_dept    FOREIGN KEY (department_id)
        REFERENCES departments  (department_id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: patient_alerts  (generated by Flink monitoring_job → persisted by alerts_job)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS patient_alerts (
    alert_id        VARCHAR(36)      NOT NULL,
    alert_type      VARCHAR(30)      NOT NULL,
    severity        VARCHAR(20)      NOT NULL,
    patient_id      VARCHAR(10)      NOT NULL,
    doctor_id       VARCHAR(10)      DEFAULT NULL,
    hospital        VARCHAR(100)     DEFAULT NULL,
    ward            VARCHAR(50)      DEFAULT NULL,
    alert_message   TEXT             NOT NULL,
    source_topic    VARCHAR(50)      DEFAULT NULL,
    is_email_sent   TINYINT(1)       NOT NULL DEFAULT 0,
    ts              DATETIME         NOT NULL,
    created_at      DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (alert_id),
    KEY idx_pa_patient   (patient_id),
    KEY idx_pa_severity  (severity),
    KEY idx_pa_type      (alert_type),
    KEY idx_pa_ts        (ts),
    CONSTRAINT fk_pa_patient FOREIGN KEY (patient_id)
        REFERENCES patients (patient_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

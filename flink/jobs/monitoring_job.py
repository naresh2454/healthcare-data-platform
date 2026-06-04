"""
Monitoring DataStream Pipeline
  Kafka topics → validate → MySQL tables
  + Real-time alert detection → alerts Kafka topic

Detection logic implemented
────────────────────────────
1. HEART_RATE    — KeyedProcessFunction + ValueState
                   Immediate CRITICAL: HR < 40 or > 150
                   Sustained WARNING : HR outside 60-100 for 3 consecutive readings
                   Window aggregate  : TumblingEventTimeWindow(60s) — if >2 abnormal
                                       readings in the window, escalate to WARNING

2. SPO2          — KeyedProcessFunction + ValueState
                   Immediate CRITICAL: SpO2 < 90%
                   Sustained WARNING : SpO2 < 94% for 2 consecutive readings
                   Window aggregate  : SlidingEventTimeWindow(5min/1min) — avg SpO2
                                       < 94% over window → sustained WARNING

3. ICU           — KeyedProcessFunction + MapState (dedup per code_type)
                   All CRITICAL codes (Code_Blue, STEMI, Stroke, Trauma) → CRITICAL
                   Rapid_Response → HIGH
                   Deduplication: suppress same code_type within 5 min per patient

4. COMPOSITE     — KeyedProcessFunction + MapState (active alert type tracking)
                   Fires when patient has both HR and SpO2 simultaneously abnormal
                   Replaces individual alerts with single escalated CRITICAL

5. LAB_CRITICAL  — KeyedProcessFunction + MapState (dedup per test name)
                   flag=critical → CRITICAL immediately
                   flag=high     → WARNING (deduped within 10 min per test)
                   doctor_id carried directly from lab_reports event

Streaming concepts
──────────────────
  Event-time processing  : BoundedOutOfOrdernessWatermarks(2s) on vitals/labs/icu
  Stateful monitoring    : ValueState + MapState in KeyedProcessFunctions
  Window aggregation     : TumblingEventTimeWindows(60s) for HR count
                           SlidingEventTimeWindows(5min,1min) for avg SpO2
  Alert generation       : All alerts union → KafkaSink → 'alerts' topic
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Iterator, Optional, Tuple

from pyflink.common import Duration, Row, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer,
    KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee,
)
from pyflink.datastream.connectors.jdbc import (
    JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions,
)
from pyflink.datastream.functions import (
    KeyedProcessFunction, ProcessFunction, ProcessWindowFunction,
)
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows, Time

import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MYSQL_URL           = os.getenv("MYSQL_URL",
                          "jdbc:mysql://mysql:3306/healthcare"
                          "?useSSL=false&allowPublicKeyRetrieval=true"
                          "&serverTimezone=UTC&sessionVariables=foreign_key_checks=0")
MYSQL_USER          = os.getenv("MYSQL_USER",     "hc_user")
MYSQL_PASSWORD      = os.getenv("MYSQL_PASSWORD", "hc_pass")
CHECKPOINT_INTERVAL = int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "10000"))
ALERTS_TOPIC        = "alerts"

INVALID_TAG = OutputTag("invalid-records", Types.STRING())


# ── Alert Thresholds ──────────────────────────────────────────────────────────

# Heart Rate
HR_CRITICAL_LOW    = 40       # bpm  — immediate CRITICAL
HR_CRITICAL_HIGH   = 150      # bpm  — immediate CRITICAL
HR_WARNING_LOW     = 60       # bpm  — sustained WARNING
HR_WARNING_HIGH    = 100      # bpm  — sustained WARNING
HR_SUSTAINED_COUNT = 3        # consecutive readings to fire sustained WARNING
HR_WINDOW_ABNORMAL_COUNT = 2  # abnormal readings in 60s window to fire WARNING

# SpO2
SPO2_CRITICAL           = 90.0   # % — immediate CRITICAL
SPO2_WARNING            = 94.0   # % — sustained WARNING
SPO2_SUSTAINED_COUNT    = 2      # consecutive readings to fire sustained WARNING
SPO2_WINDOW_AVG_WARNING = 94.0   # % — avg SpO2 below this in 5-min window → WARNING

# ICU
ICU_DEDUP_MS    = 5 * 60 * 1000    # suppress same code_type within 5 min
ICU_CRITICAL_CODES = frozenset({
    "Code_Blue", "STEMI_Alert", "Stroke_Alert", "Trauma_Activation"
})

# Lab
LAB_DEDUP_MS = 10 * 60 * 1000  # suppress same test_name within 10 min


# ── Type Infos ────────────────────────────────────────────────────────────────

DEPARTMENT_TYPE = Types.ROW_NAMED(
    ["department_id", "department_name", "hospital_branch"],
    [Types.STRING(), Types.STRING(), Types.STRING()]
)

VITALS_TYPE = Types.ROW_NAMED(
    ["event_id", "patient_id", "hospital", "ward",
     "heart_rate", "spo2", "systolic", "diastolic",
     "temperature_celsius", "respiratory_rate", "is_anomaly", "ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.INT(), Types.DOUBLE(), Types.INT(), Types.INT(),
     Types.DOUBLE(), Types.INT(), Types.BOOLEAN(), Types.SQL_TIMESTAMP()]
)

LAB_TYPE = Types.ROW_NAMED(
    ["report_id", "patient_id", "doctor_id", "hospital",
     "test_name", "value", "unit", "normal_range", "flag", "amount", "ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.DOUBLE(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.DOUBLE(), Types.SQL_TIMESTAMP()]
)

HOSPITAL_EVENT_TYPE = Types.ROW_NAMED(
    ["event_id", "patient_id", "department_id", "hospital",
     "ward", "event_type", "amount", "ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.SQL_TIMESTAMP()]
)

ICU_CODE_TYPE = Types.ROW_NAMED(
    ["code_id", "patient_id", "department_id", "hospital",
     "ward", "code_type", "severity", "amount", "status", "ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.STRING(), Types.STRING(),
     Types.DOUBLE(), Types.STRING(), Types.SQL_TIMESTAMP()]
)


# ── SQL ───────────────────────────────────────────────────────────────────────

DEPARTMENT_SQL = """
    INSERT INTO departments (department_id, department_name, hospital_branch)
    VALUES (?, ?, ?)
    ON DUPLICATE KEY UPDATE
        department_name=VALUES(department_name),
        hospital_branch=VALUES(hospital_branch)
"""

VITALS_SQL = """
    INSERT INTO patient_vitals
        (event_id, patient_id, hospital, ward,
         heart_rate, spo2, systolic, diastolic,
         temperature_celsius, respiratory_rate, is_anomaly, ts)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        heart_rate=VALUES(heart_rate), spo2=VALUES(spo2),
        systolic=VALUES(systolic), diastolic=VALUES(diastolic),
        temperature_celsius=VALUES(temperature_celsius),
        respiratory_rate=VALUES(respiratory_rate),
        is_anomaly=VALUES(is_anomaly)
"""

LAB_SQL = """
    INSERT INTO lab_reports
        (report_id, patient_id, doctor_id, hospital,
         test_name, value, unit, normal_range, flag, amount, ts)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        test_name=VALUES(test_name), value=VALUES(value),
        unit=VALUES(unit), normal_range=VALUES(normal_range),
        flag=VALUES(flag), amount=VALUES(amount)
"""

HOSPITAL_EVENT_SQL = """
    INSERT INTO hospital_events
        (event_id, patient_id, department_id, hospital,
         ward, event_type, amount, ts)
    VALUES (?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        event_type=VALUES(event_type), amount=VALUES(amount)
"""

ICU_CODE_SQL = """
    INSERT INTO icu_codes
        (code_id, patient_id, department_id, hospital,
         ward, code_type, severity, amount, status, ts)
    VALUES (?,?,?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        code_type=VALUES(code_type), severity=VALUES(severity),
        amount=VALUES(amount), status=VALUES(status)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_ts(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None

def _now_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)

def _make_alert(alert_type: str, severity: str, patient_id: str,
                message: str, data: dict, source_topic: str) -> dict:
    return {
        "alert_id":      str(uuid.uuid4()),
        "alert_type":    alert_type,
        "severity":      severity,
        "patient_id":    patient_id,
        "doctor_id":     data.get("doctor_id"),
        "hospital":      data.get("hospital"),
        "ward":          data.get("ward"),
        "alert_message": message,
        "vitals_snapshot": {
            "heart_rate":          data.get("heart_rate"),
            "spo2":                data.get("spo2"),
            "systolic":            data.get("systolic"),
            "diastolic":           data.get("diastolic"),
            "temperature_celsius": data.get("temperature_celsius"),
            "respiratory_rate":    data.get("respiratory_rate"),
        },
        "source_topic": source_topic,
        "ts":           data.get("ts"),
    }

def _get_patient_id(value: str) -> str:
    try:
        return json.loads(value).get("patient_id", "unknown")
    except Exception:
        return "unknown"


# ── Validators ────────────────────────────────────────────────────────────────

def validate_department(d: dict) -> Tuple[bool, str]:
    if not d.get("department_id"):
        return False, "missing department_id"
    if not d.get("department_name"):
        return False, "missing department_name"
    return True, ""

def validate_vitals(d: dict) -> Tuple[bool, str]:
    if not d.get("event_id"):
        return False, "missing event_id"
    if not d.get("patient_id"):
        return False, "missing patient_id"
    try:
        hr = int(d.get("heart_rate", 0))
        if not (20 <= hr <= 300):
            return False, f"heart_rate out of range: {hr}"
    except (ValueError, TypeError):
        return False, f"invalid heart_rate '{d.get('heart_rate')}'"
    try:
        spo2 = float(d.get("spo2", -1))
        if not (0.0 <= spo2 <= 100.0):
            return False, f"spo2 out of range: {spo2}"
    except (ValueError, TypeError):
        return False, f"invalid spo2 '{d.get('spo2')}'"
    try:
        temp = float(d.get("temperature_celsius", 0))
        if not (30.0 <= temp <= 45.0):
            return False, f"temperature_celsius out of range: {temp}"
    except (ValueError, TypeError):
        return False, f"invalid temperature_celsius '{d.get('temperature_celsius')}'"
    if not _parse_ts(d.get("ts", "")):
        return False, f"invalid ts '{d.get('ts')}'"
    return True, ""

def validate_lab(d: dict) -> Tuple[bool, str]:
    if not d.get("report_id"):
        return False, "missing report_id"
    if not d.get("patient_id"):
        return False, "missing patient_id"
    if not d.get("doctor_id"):
        return False, "missing doctor_id"
    if not d.get("test_name"):
        return False, "missing test_name"
    try:
        value = float(d.get("value", -1))
        if value < 0:
            return False, f"value must be >= 0, got {value}"
    except (ValueError, TypeError):
        return False, f"invalid value '{d.get('value')}'"
    valid_flags = {"normal", "low", "high", "critical"}
    if d.get("flag") not in valid_flags:
        return False, f"invalid flag '{d.get('flag')}'"
    if not _parse_ts(d.get("ts", "")):
        return False, f"invalid ts '{d.get('ts')}'"
    return True, ""

def validate_hospital_event(d: dict) -> Tuple[bool, str]:
    if not d.get("event_id"):
        return False, "missing event_id"
    if not d.get("patient_id"):
        return False, "missing patient_id"
    if not d.get("department_id"):
        return False, "missing department_id"
    if not d.get("event_type"):
        return False, "missing event_type"
    try:
        amount = float(d.get("amount", -1))
        if amount < 0:
            return False, f"amount must be >= 0, got {amount}"
    except (ValueError, TypeError):
        return False, f"invalid amount '{d.get('amount')}'"
    if not _parse_ts(d.get("ts", "")):
        return False, f"invalid ts '{d.get('ts')}'"
    return True, ""

def validate_icu_code(d: dict) -> Tuple[bool, str]:
    if not d.get("code_id"):
        return False, "missing code_id"
    if not d.get("patient_id"):
        return False, "missing patient_id"
    if not d.get("department_id"):
        return False, "missing department_id"
    if not d.get("code_type"):
        return False, "missing code_type"
    valid_severities = {"CRITICAL", "HIGH"}
    if d.get("severity") not in valid_severities:
        return False, f"invalid severity '{d.get('severity')}'"
    if not _parse_ts(d.get("ts", "")):
        return False, f"invalid ts '{d.get('ts')}'"
    return True, ""


# ── Converters ────────────────────────────────────────────────────────────────

def convert_department(d: dict) -> Row:
    return Row(d["department_id"], d["department_name"], d.get("hospital_branch"))

def convert_vitals(d: dict) -> Row:
    return Row(
        d["event_id"], d["patient_id"], d.get("hospital"), d.get("ward"),
        int(d["heart_rate"]), float(d["spo2"]),
        int(d["systolic"]), int(d["diastolic"]),
        float(d["temperature_celsius"]), int(d["respiratory_rate"]),
        bool(d["is_anomaly"]), _parse_ts(d["ts"])
    )

def convert_lab(d: dict) -> Row:
    return Row(
        d["report_id"], d["patient_id"], d["doctor_id"], d.get("hospital"),
        d["test_name"], float(d["value"]), d.get("unit"), d.get("normal_range"),
        d["flag"], float(d["amount"]), _parse_ts(d["ts"])
    )

def convert_hospital_event(d: dict) -> Row:
    return Row(
        d["event_id"], d["patient_id"], d["department_id"],
        d.get("hospital"), d.get("ward"),
        d["event_type"], float(d["amount"]), _parse_ts(d["ts"])
    )

def convert_icu_code(d: dict) -> Row:
    return Row(
        d["code_id"], d["patient_id"], d["department_id"],
        d.get("hospital"), d.get("ward"),
        d["code_type"], d["severity"],
        float(d["amount"]), d["status"], _parse_ts(d["ts"])
    )


# ── ValidateAndConvert ────────────────────────────────────────────────────────

class ValidateAndConvert(ProcessFunction):
    def __init__(self, topic: str, validator, converter):
        self.topic     = topic
        self.validator = validator
        self.converter = converter

    def process_element(self, value, ctx: "ProcessFunction.Context"):
        try:
            data = json.loads(value)
        except json.JSONDecodeError as e:
            ctx.output(INVALID_TAG, json.dumps({
                "topic": self.topic, "error": f"JSON parse error: {e}", "raw": value
            }))
            return
        valid, error = self.validator(data)
        if valid:
            yield self.converter(data)
        else:
            ctx.output(INVALID_TAG, json.dumps({
                "topic": self.topic, "error": error,
                "record_id": data.get(list(data.keys())[0], "unknown")
            }))
            log.warning("[INVALID][%s] %s", self.topic, error)


# ══════════════════════════════════════════════════════════════════════════════
# REQUIREMENT 1 + 2: Abnormal Heart Rate & Oxygen Level Monitoring
# Uses: stateful monitoring (ValueState + MapState), immediate + sustained logic
# ══════════════════════════════════════════════════════════════════════════════

class VitalsAlertDetector(KeyedProcessFunction):
    """
    Keyed by patient_id.

    Heart Rate detection
    ────────────────────
    - HR < 40 or > 150  → CRITICAL immediately (no state needed)
    - HR < 60 or > 100  → WARNING after HR_SUSTAINED_COUNT consecutive readings
                          (ValueState[int] hr_consecutive tracks count, resets on normal)

    SpO2 / Oxygen Level monitoring
    ───────────────────────────────
    - SpO2 < 90%  → CRITICAL immediately
    - SpO2 < 94%  → WARNING after SPO2_SUSTAINED_COUNT consecutive readings
                    (ValueState[int] spo2_consecutive)

    Composite / Real-time patient monitoring
    ─────────────────────────────────────────
    - Both HR and SpO2 abnormal in same reading → single COMPOSITE CRITICAL alert
      replacing the individual alerts (MapState[alert_type → last_ts] tracks which
      alert types are currently active per patient)
    """

    def open(self, runtime_context):
        self.hr_consecutive = runtime_context.get_state(
            ValueStateDescriptor("hr_abnormal_consecutive", Types.INT())
        )
        self.spo2_consecutive = runtime_context.get_state(
            ValueStateDescriptor("spo2_abnormal_consecutive", Types.INT())
        )
        # tracks alert types active in last 60s for composite detection
        self.active_alert_ts = runtime_context.get_map_state(
            MapStateDescriptor("active_alert_ts", Types.STRING(), Types.LONG())
        )

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        try:
            data = json.loads(value)
        except Exception:
            return

        patient_id = data.get("patient_id", "unknown")
        hr         = int(data.get("heart_rate", 70))
        spo2       = float(data.get("spo2", 98.0))
        now_ms     = _now_ms()
        emitted    = []

        # ── 1. Abnormal Heart Rate Detection ───────────────────────────────
        if hr < HR_CRITICAL_LOW or hr > HR_CRITICAL_HIGH:
            label = "Bradycardia" if hr < HR_CRITICAL_LOW else "Extreme Tachycardia"
            emitted.append(_make_alert(
                "HEART_RATE", "CRITICAL", patient_id,
                f"{label}: HR={hr} bpm — immediate intervention required",
                data, "patient_vitals"
            ))
            self.hr_consecutive.update(0)
            self.active_alert_ts.put("HEART_RATE", now_ms)

        elif hr < HR_WARNING_LOW or hr > HR_WARNING_HIGH:
            n = (self.hr_consecutive.value() or 0) + 1
            self.hr_consecutive.update(n)
            if n >= HR_SUSTAINED_COUNT:
                label = "Bradycardia" if hr < HR_WARNING_LOW else "Tachycardia"
                emitted.append(_make_alert(
                    "HEART_RATE", "WARNING", patient_id,
                    f"Sustained {label}: HR={hr} bpm ({n} consecutive abnormal readings)",
                    data, "patient_vitals"
                ))
                self.active_alert_ts.put("HEART_RATE", now_ms)
        else:
            self.hr_consecutive.update(0)
            self.active_alert_ts.remove("HEART_RATE")

        # ── 2. Oxygen Level (SpO2) Monitoring ──────────────────────────────
        if spo2 < SPO2_CRITICAL:
            emitted.append(_make_alert(
                "SPO2", "CRITICAL", patient_id,
                f"Severe Hypoxemia: SpO2={spo2}% — critical threshold <{SPO2_CRITICAL}%",
                data, "patient_vitals"
            ))
            self.spo2_consecutive.update(0)
            self.active_alert_ts.put("SPO2", now_ms)

        elif spo2 < SPO2_WARNING:
            n = (self.spo2_consecutive.value() or 0) + 1
            self.spo2_consecutive.update(n)
            if n >= SPO2_SUSTAINED_COUNT:
                emitted.append(_make_alert(
                    "SPO2", "WARNING", patient_id,
                    (f"Sustained low oxygen: SpO2={spo2}% "
                     f"({n} consecutive readings below {SPO2_WARNING}%)"),
                    data, "patient_vitals"
                ))
                self.active_alert_ts.put("SPO2", now_ms)
        else:
            self.spo2_consecutive.update(0)
            self.active_alert_ts.remove("SPO2")

        # ── 4. Real-time Composite Monitoring (HR + SpO2 both abnormal) ────
        # If both HR and SpO2 alerts fired in this same reading, replace with
        # a single escalated COMPOSITE CRITICAL for the doctor.
        hr_active   = self.active_alert_ts.contains("HEART_RATE")
        spo2_active = self.active_alert_ts.contains("SPO2")
        if hr_active and spo2_active and len(emitted) >= 2:
            emitted = [_make_alert(
                "COMPOSITE", "CRITICAL", patient_id,
                (f"MULTI-VITAL CRISIS: HR={hr} bpm + SpO2={spo2}% "
                 f"simultaneously critical — immediate multi-system response required"),
                data, "patient_vitals"
            )]
            log.warning("[COMPOSITE CRITICAL] patient=%s HR=%d SpO2=%.1f",
                        patient_id, hr, spo2)

        for alert in emitted:
            log.info("[ALERT][%s][%s] patient=%s — %s",
                     alert["alert_type"], alert["severity"],
                     patient_id, alert["alert_message"])
            yield json.dumps(alert)


# ══════════════════════════════════════════════════════════════════════════════
# WINDOW AGGREGATION 1: Heart Rate — TumblingEventTimeWindow(60s)
# Counts abnormal HR readings per patient per 1-minute window.
# Emits WARNING if > HR_WINDOW_ABNORMAL_COUNT readings in the window are abnormal.
# ══════════════════════════════════════════════════════════════════════════════

class HRWindowAlertFunction(ProcessWindowFunction):
    """
    Window: TumblingEventTimeWindows(60s), keyed by patient_id.
    Input : raw JSON strings from patient_vitals.
    Output: alert JSON string if abnormal HR count in window exceeds threshold.

    This catches patterns that single-event detection misses — e.g., many readings
    just above 100 bpm in a 60-second period, which individually look borderline
    but collectively indicate a sustained elevated state.
    """

    def process(self, key: str, context: "ProcessWindowFunction.Context",
                elements: Iterator):
        readings     = []
        abnormal_cnt = 0
        last_data    = {}

        for raw in elements:
            try:
                data = json.loads(raw)
                readings.append(data)
                hr = int(data.get("heart_rate", 70))
                if hr < HR_WARNING_LOW or hr > HR_WARNING_HIGH:
                    abnormal_cnt += 1
                last_data = data
            except Exception:
                continue

        if not readings or abnormal_cnt <= HR_WINDOW_ABNORMAL_COUNT:
            return

        window_start = datetime.utcfromtimestamp(context.window().start / 1000).strftime("%H:%M:%S")
        window_end   = datetime.utcfromtimestamp(context.window().end   / 1000).strftime("%H:%M:%S")
        hrs = [int(r.get("heart_rate", 0)) for r in readings]

        alert = _make_alert(
            "HEART_RATE", "WARNING", key,
            (f"Window aggregate [{window_start}→{window_end}]: "
             f"{abnormal_cnt}/{len(readings)} abnormal HR readings "
             f"(values: {hrs})"),
            last_data, "patient_vitals"
        )
        log.info("[WINDOW ALERT][HEART_RATE][WARNING] patient=%s — %d/%d abnormal in 60s window",
                 key, abnormal_cnt, len(readings))
        yield json.dumps(alert)


# ══════════════════════════════════════════════════════════════════════════════
# WINDOW AGGREGATION 2: SpO2 — SlidingEventTimeWindow(5min, 1min slide)
# Computes average SpO2 per patient over a 5-minute sliding window.
# Emits WARNING if the window average drops below SPO2_WINDOW_AVG_WARNING.
# ══════════════════════════════════════════════════════════════════════════════

class SpO2WindowAlertFunction(ProcessWindowFunction):
    """
    Window: SlidingEventTimeWindows(5min size, 1min slide), keyed by patient_id.
    Input : raw JSON strings from patient_vitals.
    Output: alert JSON string if avg SpO2 in window < SPO2_WINDOW_AVG_WARNING.

    Detects gradual oxygen desaturation that may not trigger single-event thresholds
    but represents a sustained low-oxygen trend requiring clinical attention.
    """

    def process(self, key: str, context: "ProcessWindowFunction.Context",
                elements: Iterator):
        readings  = []
        spo2_vals = []
        last_data = {}

        for raw in elements:
            try:
                data = json.loads(raw)
                readings.append(data)
                spo2_vals.append(float(data.get("spo2", 100.0)))
                last_data = data
            except Exception:
                continue

        if len(spo2_vals) < 2:
            return

        avg_spo2 = sum(spo2_vals) / len(spo2_vals)
        if avg_spo2 >= SPO2_WINDOW_AVG_WARNING:
            return

        window_start = datetime.utcfromtimestamp(context.window().start / 1000).strftime("%H:%M:%S")
        window_end   = datetime.utcfromtimestamp(context.window().end   / 1000).strftime("%H:%M:%S")

        alert = _make_alert(
            "SPO2", "WARNING", key,
            (f"5-min window avg SpO2={avg_spo2:.1f}% "
             f"[{window_start}→{window_end}] — "
             f"sustained low oxygen trend across {len(spo2_vals)} readings"),
            last_data, "patient_vitals"
        )
        log.info("[WINDOW ALERT][SPO2][WARNING] patient=%s — avg SpO2=%.1f%% over 5min window",
                 key, avg_spo2)
        yield json.dumps(alert)


# ══════════════════════════════════════════════════════════════════════════════
# REQUIREMENT 3: ICU Alert Generation
# Uses: KeyedProcessFunction + MapState (dedup per code_type per patient)
# ══════════════════════════════════════════════════════════════════════════════

class IcuAlertDetector(KeyedProcessFunction):
    """
    Keyed by patient_id.

    Generates an alert for every ICU code activation:
      - CRITICAL codes (Code_Blue, STEMI_Alert, Stroke_Alert, Trauma_Activation)
        → CRITICAL alert immediately
      - Rapid_Response → HIGH alert

    State: MapState[code_type → last_alert_epoch_ms]
    Purpose: deduplication — suppresses repeated alerts for the same code_type
    within ICU_DEDUP_MS (5 minutes) so the on-call doctor isn't flooded.
    """

    def open(self, runtime_context):
        self.last_alert_ts = runtime_context.get_map_state(
            MapStateDescriptor("icu_last_alert_ts", Types.STRING(), Types.LONG())
        )

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        try:
            data = json.loads(value)
        except Exception:
            return

        code_type  = data.get("code_type", "")
        severity   = data.get("severity", "")
        patient_id = data.get("patient_id", "unknown")

        if severity not in ("CRITICAL", "HIGH"):
            return

        now_ms  = _now_ms()
        last_ts = self.last_alert_ts.get(code_type) or 0
        if (now_ms - last_ts) < ICU_DEDUP_MS:
            log.debug("[ICU DEDUP] Suppressed %s for patient=%s (within 5 min window)",
                      code_type, patient_id)
            return

        self.last_alert_ts.put(code_type, now_ms)
        alert_severity = (
            "CRITICAL" if (code_type in ICU_CRITICAL_CODES or severity == "CRITICAL")
            else "HIGH"
        )

        alert = {
            "alert_id":    str(uuid.uuid4()),
            "alert_type":  "ICU",
            "severity":    alert_severity,
            "patient_id":  patient_id,
            "doctor_id":   None,
            "hospital":    data.get("hospital"),
            "ward":        data.get("ward"),
            "alert_message": (
                f"ICU CODE ACTIVATED — {code_type} in "
                f"{data.get('ward', 'unknown ward')} "
                f"[{alert_severity}]: immediate response required"
            ),
            "vitals_snapshot": {
                "code_type":     code_type,
                "department_id": data.get("department_id"),
                "status":        data.get("status"),
                "amount":        data.get("amount"),
            },
            "source_topic": "icu_codes",
            "ts":           data.get("ts"),
        }
        log.warning("[ICU ALERT][%s] patient=%s code=%s ward=%s",
                    alert_severity, patient_id, code_type, data.get("ward"))
        yield json.dumps(alert)


# ══════════════════════════════════════════════════════════════════════════════
# REQUIREMENT 4 (part 2): Lab Critical Detection
# Uses: KeyedProcessFunction + MapState (dedup per test_name per patient)
# ══════════════════════════════════════════════════════════════════════════════

class LabAlertDetector(KeyedProcessFunction):
    """
    Keyed by patient_id. Detects critical and high-flagged lab results.

    lab_reports events already carry doctor_id, so the alert can be routed
    directly to the correct doctor without a database lookup.

    State: MapState[test_name → last_alert_epoch_ms]
    Purpose: prevents alert spam for the same test result repeating within
    LAB_DEDUP_MS (10 minutes) — e.g., if a test is re-run quickly.
    """

    def open(self, runtime_context):
        self.last_critical_ts = runtime_context.get_map_state(
            MapStateDescriptor("lab_last_critical_ts", Types.STRING(), Types.LONG())
        )

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        try:
            data = json.loads(value)
        except Exception:
            return

        flag       = data.get("flag", "normal")
        test_name  = data.get("test_name", "unknown")
        patient_id = data.get("patient_id", "unknown")

        if flag not in ("critical", "high"):
            return

        now_ms  = _now_ms()
        last_ts = self.last_critical_ts.get(test_name) or 0
        if (now_ms - last_ts) < LAB_DEDUP_MS:
            return

        self.last_critical_ts.put(test_name, now_ms)
        severity = "CRITICAL" if flag == "critical" else "WARNING"

        alert = {
            "alert_id":    str(uuid.uuid4()),
            "alert_type":  "LAB_CRITICAL",
            "severity":    severity,
            "patient_id":  patient_id,
            "doctor_id":   data.get("doctor_id"),
            "hospital":    data.get("hospital"),
            "ward":        None,
            "alert_message": (
                f"Lab result {flag.upper()}: {test_name} = "
                f"{data.get('value')} {data.get('unit')} "
                f"(normal range: {data.get('normal_range')})"
            ),
            "vitals_snapshot": {
                "test_name":    test_name,
                "value":        data.get("value"),
                "unit":         data.get("unit"),
                "normal_range": data.get("normal_range"),
                "flag":         flag,
            },
            "source_topic": "lab_reports",
            "ts":           data.get("ts"),
        }
        log.info("[ALERT][LAB_CRITICAL][%s] patient=%s — %s",
                 severity, patient_id, alert["alert_message"])
        yield json.dumps(alert)


# ── Event-Time Watermark Strategy ─────────────────────────────────────────────

def _extract_event_ts(event: str, prev_ts: int) -> int:
    """Parse epoch-ms from the JSON 'ts' field (format: %Y-%m-%dT%H:%M:%S)."""
    try:
        ts_str = json.loads(event).get("ts", "")
        return int(datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S").timestamp() * 1000)
    except Exception:
        return prev_ts

def _event_time_strategy():
    """2-second bounded out-of-orderness; uses ts field from each event."""
    return (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(2))
        .with_timestamp_assigner(_extract_event_ts)
    )


# ── Builders ──────────────────────────────────────────────────────────────────

def build_kafka_source(topic: str) -> KafkaSource:
    return (
        KafkaSource.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP)
            .set_topics(topic)
            .set_group_id(f"flink-monitoring-{topic}")
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
    )

def build_jdbc_sink(sql: str, type_info) -> JdbcSink:
    return JdbcSink.sink(
        sql, type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url(MYSQL_URL)
            .with_driver_name("com.mysql.cj.jdbc.Driver")
            .with_user_name(MYSQL_USER)
            .with_password(MYSQL_PASSWORD)
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_interval_ms(1000)
            .with_batch_size(100)
            .with_max_retries(3)
            .build()
    )

def build_alerts_sink() -> KafkaSink:
    return (
        KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                    .set_topic(ALERTS_TOPIC)
                    .set_value_serialization_schema(SimpleStringSchema())
                    .build()
            )
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build()
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(CHECKPOINT_INTERVAL)

    # ── Standard pipelines: departments + hospital_events (validate → MySQL) ─
    for topic, validator, converter, type_info, sql in [
        ("departments",     validate_department,     convert_department,
         DEPARTMENT_TYPE,     DEPARTMENT_SQL),
        ("hospital_events", validate_hospital_event, convert_hospital_event,
         HOSPITAL_EVENT_TYPE, HOSPITAL_EVENT_SQL),
    ]:
        raw = env.from_source(
            build_kafka_source(topic),
            WatermarkStrategy.no_watermarks(),
            f"kafka-source-{topic}"
        )
        processed = raw.process(
            ValidateAndConvert(topic, validator, converter), output_type=type_info
        )
        processed.add_sink(build_jdbc_sink(sql, type_info))
        processed.get_side_output(INVALID_TAG).map(lambda x: f"[INVALID] {x}").print()
        log.info("Pipeline registered: %s", topic)

    # ═════════════════════════════════════════════════════════════════════════
    # patient_vitals pipeline
    #   → MySQL (validate + upsert)
    #   → VitalsAlertDetector  (HR abnormal + SpO2 monitoring + Composite)
    #   → HRWindowAlertFunction   (TumblingEventTimeWindows 60s — HR count)
    #   → SpO2WindowAlertFunction (SlidingEventTimeWindows 5min/1min — avg SpO2)
    # ═════════════════════════════════════════════════════════════════════════
    raw_vitals = env.from_source(
        build_kafka_source("patient_vitals"),
        _event_time_strategy(),                    # event-time processing
        "kafka-source-patient_vitals"
    )

    # MySQL sink
    processed_vitals = raw_vitals.process(
        ValidateAndConvert("patient_vitals", validate_vitals, convert_vitals),
        output_type=VITALS_TYPE
    )
    processed_vitals.add_sink(build_jdbc_sink(VITALS_SQL, VITALS_TYPE))
    processed_vitals.get_side_output(INVALID_TAG).map(lambda x: f"[INVALID] {x}").print()

    # Keyed stream for all alert detection
    keyed_vitals = raw_vitals.key_by(_get_patient_id)

    # Stateful HR + SpO2 + Composite detection (per-event)
    vitals_per_event_alerts = keyed_vitals.process(
        VitalsAlertDetector(), output_type=Types.STRING()
    )

    # Window: TumblingEventTimeWindows(60s) — HR abnormal count per patient
    vitals_hr_window_alerts = (
        keyed_vitals
        .window(TumblingEventTimeWindows.of(Time.seconds(60)))
        .process(HRWindowAlertFunction(), output_type=Types.STRING())
    )

    # Window: SlidingEventTimeWindows(5min, 1min) — avg SpO2 per patient
    vitals_spo2_window_alerts = (
        keyed_vitals
        .window(SlidingEventTimeWindows.of(
            Time.minutes(5), Time.minutes(1)
        ))
        .process(SpO2WindowAlertFunction(), output_type=Types.STRING())
    )

    log.info("Pipeline registered: patient_vitals "
             "(HR stateful + SpO2 stateful + Composite + HR 60s window + SpO2 5min window)")

    # ═════════════════════════════════════════════════════════════════════════
    # lab_reports pipeline
    #   → MySQL (validate + upsert)
    #   → LabAlertDetector (critical/high flags, doctor_id from event)
    # ═════════════════════════════════════════════════════════════════════════
    raw_labs = env.from_source(
        build_kafka_source("lab_reports"),
        _event_time_strategy(),
        "kafka-source-lab_reports"
    )
    processed_labs = raw_labs.process(
        ValidateAndConvert("lab_reports", validate_lab, convert_lab),
        output_type=LAB_TYPE
    )
    processed_labs.add_sink(build_jdbc_sink(LAB_SQL, LAB_TYPE))
    processed_labs.get_side_output(INVALID_TAG).map(lambda x: f"[INVALID] {x}").print()

    lab_alerts = (
        raw_labs
        .key_by(_get_patient_id)
        .process(LabAlertDetector(), output_type=Types.STRING())
    )
    log.info("Pipeline registered: lab_reports (+ critical lab alert detection)")

    # ═════════════════════════════════════════════════════════════════════════
    # icu_codes pipeline
    #   → MySQL (validate + upsert)
    #   → IcuAlertDetector (CRITICAL/HIGH codes, dedup per code_type)
    # ═════════════════════════════════════════════════════════════════════════
    raw_icu = env.from_source(
        build_kafka_source("icu_codes"),
        _event_time_strategy(),
        "kafka-source-icu_codes"
    )
    processed_icu = raw_icu.process(
        ValidateAndConvert("icu_codes", validate_icu_code, convert_icu_code),
        output_type=ICU_CODE_TYPE
    )
    processed_icu.add_sink(build_jdbc_sink(ICU_CODE_SQL, ICU_CODE_TYPE))
    processed_icu.get_side_output(INVALID_TAG).map(lambda x: f"[INVALID] {x}").print()

    icu_alerts = (
        raw_icu
        .key_by(_get_patient_id)
        .process(IcuAlertDetector(), output_type=Types.STRING())
    )
    log.info("Pipeline registered: icu_codes (+ ICU alert generation)")

    # ── Union ALL alert streams → single alerts Kafka topic ──────────────────
    all_alerts = (
        vitals_per_event_alerts
        .union(vitals_hr_window_alerts)
        .union(vitals_spo2_window_alerts)
        .union(lab_alerts)
        .union(icu_alerts)
    )
    all_alerts.sink_to(build_alerts_sink())
    log.info("Alert sink registered → Kafka topic: '%s'", ALERTS_TOPIC)

    log.info("Starting Monitoring DataStream Pipeline...")
    env.execute("Monitoring DataStream Pipeline")


if __name__ == "__main__":
    main()

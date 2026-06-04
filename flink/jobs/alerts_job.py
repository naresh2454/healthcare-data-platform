"""
Alerts DataStream Pipeline
  Kafka 'alerts' topic → doctor enrichment → rate-limit → Gmail email → MySQL

Architecture
────────────
Stage 1 — Doctor enrichment via Broadcast State
  appointments Kafka stream → BroadcastStream[patient_id → doctor_id]
  alerts stream.connect(broadcast) → DoctorEnrichFunction
    processBroadcastElement : update MapState[patient_id] = doctor_id
    processElement           : read state, attach doctor_id / doctor_email
  Doctor emails loaded from MySQL once at startup (10 doctors, rarely change).

Stage 2 — Rate-limit + Email sending
  Keyed by patient_id → EmailAlertFunction (KeyedProcessFunction)
    MapState[alert_type → last_email_epoch_ms] — max 1 email per patient
    per alert_type per EMAIL_RATE_LIMIT_MS (5 min), prevents alert flood.
  Gmail SMTP via smtplib (TLS, app password).

Stage 3 — Persist to MySQL patient_alerts table
  Emits Row after email decision → JdbcSink → patient_alerts

Streaming concepts used
────────────────────────
  Broadcast State   : BroadcastProcessFunction + MapStateDescriptor —
                      live patient → doctor mapping from appointments stream
  Stateful monitoring: KeyedProcessFunction + MapState — email rate limiting
  Alert generation  : structured email per severity with full clinical context
"""

import json
import logging
import smtplib
import uuid
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.jdbc import (
    JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions,
)
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import BroadcastProcessFunction, KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

MYSQL_HOST     = os.getenv("MYSQL_HOST",            "mysql")
MYSQL_PORT     = int(os.getenv("MYSQL_CONTAINER_PORT", "3306"))
MYSQL_DB       = os.getenv("MYSQL_DATABASE",         "healthcare")
MYSQL_USER     = os.getenv("MYSQL_USER",             "hc_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD",         "hc_pass")
MYSQL_URL      = os.getenv("MYSQL_URL",
                    "jdbc:mysql://mysql:3306/healthcare"
                    "?useSSL=false&allowPublicKeyRetrieval=true"
                    "&serverTimezone=UTC&sessionVariables=foreign_key_checks=0")

SMTP_HOST  = "smtp.gmail.com"
SMTP_PORT  = 587

EMAIL_RATE_LIMIT_MS = 5 * 60 * 1000   # 1 email per (patient, alert_type) per 5 min
CHECKPOINT_INTERVAL = int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "15000"))


# ── Broadcast State Descriptor ────────────────────────────────────────────────
# Maps patient_id → doctor_id; updated live from appointments stream.

PATIENT_DOCTOR_STATE = MapStateDescriptor(
    "patient_doctor_map", Types.STRING(), Types.STRING()
)


# ── MySQL Row Type + SQL for patient_alerts persistence ───────────────────────

ALERT_ROW_TYPE = Types.ROW_NAMED(
    ["alert_id", "alert_type", "severity", "patient_id", "doctor_id",
     "hospital", "ward", "alert_message", "source_topic", "is_email_sent", "ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.BOOLEAN(), Types.SQL_TIMESTAMP()]
)

ALERT_SQL = """
    INSERT INTO patient_alerts
        (alert_id, alert_type, severity, patient_id, doctor_id,
         hospital, ward, alert_message, source_topic, is_email_sent, ts)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        severity=VALUES(severity),
        alert_message=VALUES(alert_message),
        is_email_sent=VALUES(is_email_sent)
"""


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 1 — Doctor enrichment via Broadcast State
# ══════════════════════════════════════════════════════════════════════════════

class DoctorEnrichFunction(BroadcastProcessFunction):
    """
    Broadcast State pattern:
      - appointments stream (broadcast side) maintains MapState[patient_id → doctor_id]
      - alerts stream (regular side) reads the state to find the treating doctor

    Doctor details (name, email) are loaded from MySQL at startup so the
    broadcast stream does not need to carry email addresses — appointments
    only have doctor_id.
    """

    def open(self, runtime_context):
        import mysql.connector
        self._doctor_map = {}         # doctor_id  → {"name": str, "email": str}
        self._patient_doctor_map = {} # patient_id → doctor_id  (pre-seeded from MySQL)
        try:
            conn = mysql.connector.connect(
                host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DB,
                user=MYSQL_USER, password=MYSQL_PASSWORD, connection_timeout=10
            )
            cur = conn.cursor()

            # Load doctor details
            cur.execute(
                "SELECT doctor_id, first_name, last_name, email FROM doctors"
            )
            for doctor_id, fn, ln, email in cur.fetchall():
                self._doctor_map[doctor_id] = {
                    "name":  f"Dr. {fn} {ln}",
                    "email": email or "",
                }
            log.info("[DoctorEnrich] Loaded %d doctors from MySQL", len(self._doctor_map))

            # Pre-seed patient→doctor from most-recent appointment per patient
            cur.execute("""
                SELECT a.patient_id, a.doctor_id
                FROM appointments a
                INNER JOIN (
                    SELECT patient_id, MAX(appointment_date) AS max_date
                    FROM appointments
                    WHERE status IN ('Scheduled', 'Completed')
                    GROUP BY patient_id
                ) latest ON a.patient_id = latest.patient_id
                       AND a.appointment_date = latest.max_date
            """)
            for patient_id, doctor_id in cur.fetchall():
                self._patient_doctor_map[patient_id] = doctor_id
            log.info("[DoctorEnrich] Pre-seeded %d patient→doctor mappings from MySQL",
                     len(self._patient_doctor_map))

            cur.close()
            conn.close()
        except Exception as e:
            log.error("[DoctorEnrich] Could not load data from MySQL: %s", e)

    def process_broadcast_element(self, value: str,
                                  ctx: "BroadcastProcessFunction.Context"):
        """
        Receives appointments events. Updates broadcast state:
          patient_id → most-recent doctor_id
        Only 'Scheduled' and 'Completed' appointments are used so we always
        point to the actively treating physician.
        """
        try:
            appt = json.loads(value)
            status = appt.get("status", "")
            if status in ("Scheduled", "Completed"):
                patient_id = appt.get("patient_id")
                doctor_id  = appt.get("doctor_id")
                if patient_id and doctor_id:
                    ctx.get_broadcast_state(PATIENT_DOCTOR_STATE).put(
                        patient_id, doctor_id
                    )
        except Exception:
            pass

    def process_element(self, value: str,
                        ctx: "BroadcastProcessFunction.Context"):
        """
        Receives alert events. Enriches with doctor info:
          - For lab alerts: doctor_id is already in the alert (carried from event)
          - For vitals/ICU alerts: look up doctor_id from broadcast state
        Then attaches doctor_name and doctor_email from pre-loaded doctor map.
        """
        try:
            alert = json.loads(value)
        except Exception:
            return

        patient_id = alert.get("patient_id")
        doctor_id  = alert.get("doctor_id")

        # Lab alerts already carry doctor_id; use broadcast state for others,
        # falling back to the MySQL-seeded map for ICU/vitals alerts.
        if not doctor_id and patient_id:
            doctor_id = ctx.get_broadcast_state(PATIENT_DOCTOR_STATE).get(patient_id)
        if not doctor_id and patient_id:
            doctor_id = self._patient_doctor_map.get(patient_id)

        if doctor_id and doctor_id in self._doctor_map:
            alert["doctor_id"]    = doctor_id
            alert["doctor_name"]  = self._doctor_map[doctor_id]["name"]
            alert["doctor_email"] = self._doctor_map[doctor_id]["email"]
        else:
            alert["doctor_name"]  = None
            alert["doctor_email"] = None
            log.warning("[DoctorEnrich] No doctor found for patient=%s", patient_id)

        yield json.dumps(alert)


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 2 — Rate-limit + Send Gmail email
# ══════════════════════════════════════════════════════════════════════════════

class EmailAlertFunction(KeyedProcessFunction):
    """
    Keyed by patient_id.

    State: MapState[alert_type → last_email_epoch_ms]
    Ensures at most 1 email per patient per alert_type within EMAIL_RATE_LIMIT_MS.
    This prevents the on-call doctor from receiving hundreds of emails for a
    patient who is continuously in a critical state.

    After the rate-limit decision, emits the enriched alert JSON for Stage 3
    (MySQL persistence) regardless of whether an email was sent.
    """

    def open(self, runtime_context):
        self.last_email_ts = runtime_context.get_map_state(
            MapStateDescriptor("last_email_ts_per_type", Types.STRING(), Types.LONG())
        )
        self._smtp_user = os.getenv("SMTP_USER", "")
        self._smtp_pass = os.getenv("SMTP_PASSWORD", "")
        self._alert_from = os.getenv("ALERT_FROM_EMAIL", "") or self._smtp_user

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        try:
            alert = json.loads(value)
        except Exception:
            return

        alert_type   = alert.get("alert_type", "UNKNOWN")
        severity     = alert.get("severity", "UNKNOWN")
        doctor_email = alert.get("doctor_email")
        patient_id   = alert.get("patient_id", "unknown")
        now_ms       = int(datetime.utcnow().timestamp() * 1000)
        email_sent   = False

        # Rate-limit check
        last_ts = self.last_email_ts.get(alert_type) or 0
        within_limit = (now_ms - last_ts) < EMAIL_RATE_LIMIT_MS

        if within_limit:
            log.info("[EMAIL RATE-LIMITED] patient=%s type=%s — suppressed (within 5 min)",
                     patient_id, alert_type)
        elif not doctor_email:
            log.warning("[EMAIL SKIP] patient=%s type=%s — no doctor email available",
                        patient_id, alert_type)
        elif not self._smtp_user:
            log.warning("[EMAIL SKIP] SMTP_USER not configured — email not sent")
        else:
            try:
                _send_gmail(alert, self._smtp_user, self._smtp_pass, self._alert_from)
                email_sent = True
                self.last_email_ts.put(alert_type, now_ms)
                log.info("[EMAIL SENT][%s] %s → %s | patient=%s",
                         severity, alert_type, doctor_email, patient_id)
            except Exception as e:
                log.error("[EMAIL FAILED] patient=%s type=%s → %s: %s",
                          patient_id, alert_type, doctor_email, e)

        alert["is_email_sent"] = email_sent
        yield json.dumps(alert)


# ── Gmail SMTP ────────────────────────────────────────────────────────────────

def _send_gmail(alert: dict, smtp_user: str, smtp_pass: str, alert_from: str):
    severity    = alert.get("severity",    "UNKNOWN")
    alert_type  = alert.get("alert_type",  "ALERT")
    patient_id  = alert.get("patient_id",  "unknown")
    doctor_name = alert.get("doctor_name") or "Doctor"
    doctor_email= alert.get("doctor_email")
    ward        = alert.get("ward")     or "N/A"
    hospital    = alert.get("hospital") or "N/A"
    message     = alert.get("alert_message", "")
    ts          = alert.get("ts") or datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    vitals      = alert.get("vitals_snapshot") or {}

    vitals_lines = "\n".join(
        f"    {k.replace('_', ' ').title():30s}: {v}"
        for k, v in vitals.items()
        if v is not None
    ) or "    (no additional clinical data)"

    # Severity-based urgency note
    urgency_map = {
        "CRITICAL": "⚠ CRITICAL — Respond IMMEDIATELY",
        "HIGH":     "⚠ HIGH — Respond within 10 minutes",
        "WARNING":  "ℹ WARNING — Review at earliest opportunity",
    }
    urgency = urgency_map.get(severity, severity)

    body = f"""\
══════════════════════════════════════════════════════
  HEALTHCARE MONITORING SYSTEM — ALERT NOTIFICATION
══════════════════════════════════════════════════════

Dear {doctor_name},

An automated alert has been triggered for one of your patients.

  {urgency}

──────────────────────────────────────────────────────
  ALERT DETAILS
──────────────────────────────────────────────────────
  Alert Type   : {alert_type}
  Severity     : {severity}
  Patient ID   : {patient_id}
  Location     : {ward} — {hospital}
  Timestamp    : {ts} UTC

──────────────────────────────────────────────────────
  CLINICAL MESSAGE
──────────────────────────────────────────────────────
  {message}

──────────────────────────────────────────────────────
  CLINICAL DATA
──────────────────────────────────────────────────────
{vitals_lines}

══════════════════════════════════════════════════════
This is an automated message from the Healthcare
Real-time Monitoring System (Apache Flink pipeline).
Do NOT reply to this email.
Alert ID: {alert.get("alert_id", "N/A")}
══════════════════════════════════════════════════════
"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[{severity}] {alert_type} — Patient {patient_id} | {ward} | {hospital}"
    msg["From"]    = alert_from
    msg["To"]      = doctor_email
    msg["X-Priority"] = "1" if severity == "CRITICAL" else "3"
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(smtp_user, smtp_pass)
        server.sendmail(alert_from, [doctor_email], msg.as_string())


# ── Stage 3: alert JSON → MySQL Row ──────────────────────────────────────────

def _alert_to_row(value: str) -> Optional[Row]:
    try:
        d  = json.loads(value)
        ts_str = d.get("ts", "")
        ts = (datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
              if ts_str else datetime.utcnow())
        return Row(
            d.get("alert_id")      or str(uuid.uuid4()),
            d.get("alert_type",    "UNKNOWN"),
            d.get("severity",      "UNKNOWN"),
            d.get("patient_id",    "unknown"),
            d.get("doctor_id"),
            d.get("hospital"),
            d.get("ward"),
            d.get("alert_message", ""),
            d.get("source_topic"),
            bool(d.get("is_email_sent", False)),
            ts,
        )
    except Exception as e:
        log.error("[RowConvert] Failed: %s", e)
        return None


# ── Builders ──────────────────────────────────────────────────────────────────

def _kafka_source(topic: str, group_id: str) -> KafkaSource:
    return (
        KafkaSource.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP)
            .set_topics(topic)
            .set_group_id(group_id)
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
    )

def _jdbc_sink() -> JdbcSink:
    return JdbcSink.sink(
        ALERT_SQL, ALERT_ROW_TYPE,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url(MYSQL_URL)
            .with_driver_name("com.mysql.cj.jdbc.Driver")
            .with_user_name(MYSQL_USER)
            .with_password(MYSQL_PASSWORD)
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_interval_ms(2000)
            .with_batch_size(50)
            .with_max_retries(3)
            .build()
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(CHECKPOINT_INTERVAL)

    # ── Appointments → BroadcastStream (patient_id → doctor_id mapping) ──────
    appointments_stream = env.from_source(
        _kafka_source("appointments", "flink-alerts-appointments"),
        WatermarkStrategy.no_watermarks(),
        "kafka-source-appointments"
    )
    broadcast_appts = appointments_stream.broadcast(PATIENT_DOCTOR_STATE)

    # ── Alerts input stream ───────────────────────────────────────────────────
    alerts_stream = env.from_source(
        _kafka_source("alerts", "flink-alerts-consumer"),
        WatermarkStrategy.no_watermarks(),
        "kafka-source-alerts"
    )

    # ── Stage 1: Enrich alert with doctor info via Broadcast State ───────────
    enriched = (
        alerts_stream
        .connect(broadcast_appts)
        .process(DoctorEnrichFunction(), output_type=Types.STRING())
    )

    # ── Stage 2: Rate-limit per (patient, alert_type) + send Gmail email ─────
    processed = (
        enriched
        .key_by(lambda x: json.loads(x).get("patient_id", "unknown"))
        .process(EmailAlertFunction(), output_type=Types.STRING())
    )

    # ── Stage 3: Convert to Row → persist to MySQL patient_alerts table ──────
    processed \
        .map(_alert_to_row, output_type=ALERT_ROW_TYPE) \
        .filter(lambda r: r is not None) \
        .add_sink(_jdbc_sink())

    log.info("Starting Alerts Pipeline "
             "(Broadcast State enrichment → Gmail SMTP → MySQL patient_alerts)...")
    env.execute("Healthcare Alerts Pipeline")


if __name__ == "__main__":
    main()

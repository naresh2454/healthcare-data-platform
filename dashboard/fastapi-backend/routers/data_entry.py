import json
import uuid
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
from kafka import KafkaProducer
from kafka.errors import KafkaError
import config

router = APIRouter()


def _producer():
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        request_timeout_ms=5000,
    )


def _send(topic: str, key: str, payload: dict):
    try:
        prod = _producer()
        future = prod.send(topic, key=key.encode(), value=payload)
        record = future.get(timeout=10)
        prod.flush()
        prod.close()
        return {"success": True, "topic": topic, "partition": record.partition, "offset": record.offset}
    except KafkaError as e:
        raise HTTPException(status_code=502, detail=f"Kafka error: {e}")


# ── Patient ───────────────────────────────────────────────────────────────────
class PatientIn(BaseModel):
    first_name: str
    last_name: str
    gender: str
    date_of_birth: date
    contact_number: str
    address: Optional[str] = None
    insurance_provider: Optional[str] = None
    insurance_number: Optional[str] = None
    email: Optional[str] = None


@router.post("/patient")
def register_patient(body: PatientIn):
    patient_id = "P-" + uuid.uuid4().hex[:6].upper()
    payload = {"patient_id": patient_id, "registration_date": str(date.today()), **body.model_dump()}
    return _send("patients", patient_id, payload)


# ── Doctor ────────────────────────────────────────────────────────────────────
class DoctorIn(BaseModel):
    first_name: str
    last_name: str
    specialization: str
    phone_number: str
    years_experience: int
    hospital_branch: Optional[str] = None
    email: Optional[str] = None


@router.post("/doctor")
def add_doctor(body: DoctorIn):
    doctor_id = "D-" + uuid.uuid4().hex[:6].upper()
    payload = {"doctor_id": doctor_id, **body.model_dump()}
    return _send("doctors", doctor_id, payload)


# ── Appointment ───────────────────────────────────────────────────────────────
class AppointmentIn(BaseModel):
    patient_id: str
    doctor_id: str
    appointment_date: date
    appointment_time: str
    reason_for_visit: Optional[str] = None
    status: str = "Scheduled"


@router.post("/appointment")
def schedule_appointment(body: AppointmentIn):
    appt_id = "A-" + uuid.uuid4().hex[:6].upper()
    payload = {"appointment_id": appt_id, **body.model_dump()}
    return _send("appointments", appt_id, payload)


# ── Treatment ─────────────────────────────────────────────────────────────────
class TreatmentIn(BaseModel):
    appointment_id: str
    treatment_type: str
    description: Optional[str] = None
    cost: float
    treatment_date: date


@router.post("/treatment")
def record_treatment(body: TreatmentIn):
    treatment_id = "T-" + uuid.uuid4().hex[:6].upper()
    payload = {"treatment_id": treatment_id, **body.model_dump()}
    return _send("treatments", treatment_id, payload)


# ── Billing ───────────────────────────────────────────────────────────────────
class BillingIn(BaseModel):
    patient_id: str
    treatment_id: str
    bill_date: date
    amount: float
    payment_method: Optional[str] = None
    payment_status: str = "Pending"


@router.post("/billing")
def generate_bill(body: BillingIn):
    bill_id = "B-" + uuid.uuid4().hex[:6].upper()
    payload = {"bill_id": bill_id, **body.model_dump()}
    return _send("billing", bill_id, payload)


# ── Manual Alert (direct to alerts Kafka topic → Flink → Gmail) ──────────────
class AlertIn(BaseModel):
    patient_id: str
    alert_type: str
    severity: str
    alert_message: str
    hospital: Optional[str] = "City General Hospital"
    ward: Optional[str] = None


@router.post("/alert")
def send_alert(body: AlertIn):
    from datetime import datetime
    alert_id = str(uuid.uuid4())
    payload = {
        "alert_id":      alert_id,
        "alert_type":    body.alert_type,
        "severity":      body.severity,
        "patient_id":    body.patient_id,
        "hospital":      body.hospital,
        "ward":          body.ward,
        "alert_message": body.alert_message,
        "source_topic":  "manual",
        "ts":            datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
    }
    return _send("alerts", alert_id, payload)


# ── Lab Report ───────────────────────────────────────────────────────────────
class LabReportIn(BaseModel):
    patient_id: str
    doctor_id: str
    hospital: Optional[str] = "City General Hospital"
    test_name: str
    value: float
    unit: str
    normal_range: str
    flag: str
    amount: float


@router.post("/lab-report")
def add_lab_report(body: LabReportIn):
    from datetime import datetime
    report_id = str(uuid.uuid4())
    payload = {
        "report_id": report_id,
        "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        **body.model_dump(),
    }
    return _send("lab_reports", report_id, payload)


# ── Hospital Event ────────────────────────────────────────────────────────────
class HospitalEventIn(BaseModel):
    patient_id: str
    department_id: str
    hospital: Optional[str] = "City General Hospital"
    ward: Optional[str] = None
    event_type: str
    amount: float


@router.post("/hospital-event")
def add_hospital_event(body: HospitalEventIn):
    from datetime import datetime
    event_id = str(uuid.uuid4())
    payload = {
        "event_id": event_id,
        "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        **body.model_dump(),
    }
    return _send("hospital_events", event_id, payload)


# ── ICU Code ──────────────────────────────────────────────────────────────────
class IcuCodeIn(BaseModel):
    patient_id: str
    department_id: str
    hospital: Optional[str] = "City General Hospital"
    ward: Optional[str] = None
    code_type: str
    severity: str
    amount: float
    status: str = "Activated"


@router.post("/icu-code")
def add_icu_code(body: IcuCodeIn):
    from datetime import datetime
    code_id = str(uuid.uuid4())
    payload = {
        "code_id": code_id,
        "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        **body.model_dump(),
    }
    return _send("icu_codes", code_id, payload)


# ── Department ────────────────────────────────────────────────────────────────
class DepartmentIn(BaseModel):
    department_id: str
    department_name: str
    hospital_branch: str


@router.post("/department")
def add_department(body: DepartmentIn):
    payload = body.model_dump()
    return _send("departments", body.department_id, payload)


# ── Patient Vitals ────────────────────────────────────────────────────────────
class VitalsIn(BaseModel):
    patient_id: str
    hospital: Optional[str] = None
    ward: Optional[str] = None
    heart_rate: int
    spo2: float
    systolic: int
    diastolic: int
    temperature_celsius: float
    respiratory_rate: int
    is_anomaly: bool = False


@router.post("/vitals")
def record_vitals(body: VitalsIn):
    from datetime import datetime
    event_id = "V-" + uuid.uuid4().hex[:6].upper()
    payload = {
        "event_id": event_id,
        "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        **body.model_dump(),
    }
    return _send("patient_vitals", event_id, payload)

import { useState } from 'react'
import { dataEntry } from '../api/client'
import toast from 'react-hot-toast'

const TABS = [
  { id: 'patient',     label: 'Register Patient'    },
  { id: 'doctor',      label: 'Add Doctor'          },
  { id: 'appointment', label: 'Schedule Appointment'},
  { id: 'treatment',   label: 'Record Treatment'    },
  { id: 'billing',     label: 'Generate Bill'       },
  { id: 'vitals',      label: 'Patient Vitals'      },
  { id: 'alert',       label: 'Send Alert'          },
  { id: 'lab',         label: 'Lab Report'          },
  { id: 'hospital',    label: 'Hospital Event'      },
  { id: 'icu',         label: 'ICU Code'            },
  { id: 'department',  label: 'Department'          },
]

const LAB_TESTS = [
  { name:'Glucose',    unit:'mg/dL',  lo:70,   hi:100,  cost:50,  },
  { name:'Hemoglobin', unit:'g/dL',   lo:12,   hi:17.5, cost:75,  },
  { name:'WBC',        unit:'K/uL',   lo:4.5,  hi:11.0, cost:80,  },
  { name:'Creatinine', unit:'mg/dL',  lo:0.6,  hi:1.2,  cost:90,  },
  { name:'Troponin',   unit:'ng/mL',  lo:0.0,  hi:0.04, cost:200, },
  { name:'Sodium',     unit:'mEq/L',  lo:136,  hi:145,  cost:60,  },
]
const HOSPITAL_EVENT_TYPES = ['Admission','Discharge','Transfer','Emergency_Arrival','Surgery_Start','Surgery_End','ICU_Transfer']
const HOSPITAL_EVENT_AMOUNTS = { Admission:1500, Discharge:250, Transfer:300, Emergency_Arrival:2500, Surgery_Start:8000, Surgery_End:0, ICU_Transfer:1200 }
const ICU_CODE_TYPES = ['Code_Blue','STEMI_Alert','Stroke_Alert','Rapid_Response','Trauma_Activation']
const ICU_SEVERITY   = { Code_Blue:'CRITICAL', STEMI_Alert:'CRITICAL', Stroke_Alert:'CRITICAL', Rapid_Response:'HIGH', Trauma_Activation:'CRITICAL' }
const ICU_AMOUNTS    = { Code_Blue:5000, STEMI_Alert:12000, Stroke_Alert:10000, Rapid_Response:3000, Trauma_Activation:15000 }
const DEPARTMENTS    = [
  { id:'DEPT01', name:'ICU',        branch:'Central Hospital' },
  { id:'DEPT02', name:'Cardiology', branch:'Westside Clinic'  },
  { id:'DEPT03', name:'Emergency',  branch:'Central Hospital' },
  { id:'DEPT04', name:'Nephrology', branch:'North Wing'       },
  { id:'DEPT05', name:'Neurology',  branch:'Eastside Clinic'  },
]
const WARDS = ['Ward-A','Ward-B','ICU-1','ICU-2','ER-1']

const SPECIALIZATIONS = ['Cardiology','Orthopedics','Neurology','Dermatology','Pediatrics','Oncology','General Medicine','Gynecology']
const BRANCHES         = ['Main Hospital','North Branch','South Branch','East Branch','West Branch']
const TREATMENT_TYPES  = ['Consultation','Surgery','Physiotherapy','Laboratory Test','Radiology','Vaccination','Dental','Emergency']
const PAYMENT_METHODS  = ['Cash','Credit Card','Debit Card','Insurance','UPI','Net Banking']
const PAYMENT_STATUSES = ['Paid','Pending','Failed','Refunded']
const APPT_STATUSES    = ['Scheduled','Completed','Cancelled','No-show']

function Field({ label, required, children }) {
  return (
    <div>
      <label className="block text-xs font-medium text-slate-400 mb-1">
        {label}{required && <span className="text-red-400 ml-0.5">*</span>}
      </label>
      {children}
    </div>
  )
}

const inp = 'w-full bg-navy-900 border border-navy-700 rounded-lg px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500 transition'
const sel = `${inp} cursor-pointer`

function PatientForm() {
  const [f, setF] = useState({ first_name:'', last_name:'', gender:'', date_of_birth:'', contact_number:'', address:'', insurance_provider:'', insurance_number:'', email:'' })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.patient(f)
      toast.success('Patient registered successfully')
      setF({ first_name:'', last_name:'', gender:'', date_of_birth:'', contact_number:'', address:'', insurance_provider:'', insurance_number:'', email:'' })
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Registration failed')
    }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="First Name" required>
          <input className={inp} value={f.first_name} onChange={set('first_name')} placeholder="Enter first name" required />
        </Field>
        <Field label="Last Name" required>
          <input className={inp} value={f.last_name} onChange={set('last_name')} placeholder="Enter last name" required />
        </Field>
        <Field label="Date of Birth" required>
          <input type="date" className={inp} value={f.date_of_birth} onChange={set('date_of_birth')} required />
        </Field>
        <Field label="Gender" required>
          <select className={sel} value={f.gender} onChange={set('gender')} required>
            <option value="">Select gender</option>
            <option value="M">Male</option>
            <option value="F">Female</option>
            <option value="O">Other</option>
          </select>
        </Field>
        <Field label="Contact Number" required>
          <input className={inp} value={f.contact_number} onChange={set('contact_number')} placeholder="+91 XXXXX XXXXX" required />
        </Field>
        <Field label="Email">
          <input type="email" className={inp} value={f.email} onChange={set('email')} placeholder="patient@email.com" />
        </Field>
        <Field label="Address">
          <input className={inp} value={f.address} onChange={set('address')} placeholder="Street address" />
        </Field>
        <Field label="Insurance Provider">
          <input className={inp} value={f.insurance_provider} onChange={set('insurance_provider')} placeholder="e.g. Star Health" />
        </Field>
        <Field label="Insurance Number">
          <input className={inp} value={f.insurance_number} onChange={set('insurance_number')} placeholder="Policy number" />
        </Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">
          Register Patient
        </button>
      </div>
    </form>
  )
}

function DoctorForm() {
  const [f, setF] = useState({ first_name:'', last_name:'', specialization:'', phone_number:'', years_experience:0, hospital_branch:'', email:'' })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.doctor({ ...f, years_experience: Number(f.years_experience) })
      toast.success('Doctor added successfully')
      setF({ first_name:'', last_name:'', specialization:'', phone_number:'', years_experience:0, hospital_branch:'', email:'' })
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Failed to add doctor')
    }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="First Name" required>
          <input className={inp} value={f.first_name} onChange={set('first_name')} placeholder="Dr. first name" required />
        </Field>
        <Field label="Last Name" required>
          <input className={inp} value={f.last_name} onChange={set('last_name')} placeholder="Dr. last name" required />
        </Field>
        <Field label="Specialization" required>
          <select className={sel} value={f.specialization} onChange={set('specialization')} required>
            <option value="">Select specialization</option>
            {SPECIALIZATIONS.map(s => <option key={s}>{s}</option>)}
          </select>
        </Field>
        <Field label="Hospital Branch">
          <select className={sel} value={f.hospital_branch} onChange={set('hospital_branch')}>
            <option value="">Select branch</option>
            {BRANCHES.map(b => <option key={b}>{b}</option>)}
          </select>
        </Field>
        <Field label="Phone Number" required>
          <input className={inp} value={f.phone_number} onChange={set('phone_number')} placeholder="+91 XXXXX XXXXX" required />
        </Field>
        <Field label="Years of Experience" required>
          <input type="number" min={0} max={60} className={inp} value={f.years_experience} onChange={set('years_experience')} required />
        </Field>
        <Field label="Email">
          <input type="email" className={inp} value={f.email} onChange={set('email')} placeholder="doctor@hospital.com" />
        </Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">
          Add Doctor
        </button>
      </div>
    </form>
  )
}

function AppointmentForm() {
  const [f, setF] = useState({ patient_id:'', doctor_id:'', appointment_date:'', appointment_time:'', reason_for_visit:'', status:'Scheduled' })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.appointment(f)
      toast.success('Appointment scheduled')
      setF({ patient_id:'', doctor_id:'', appointment_date:'', appointment_time:'', reason_for_visit:'', status:'Scheduled' })
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Failed to schedule')
    }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required>
          <input className={inp} value={f.patient_id} onChange={set('patient_id')} placeholder="e.g. P-3A1B2C" required />
        </Field>
        <Field label="Doctor ID" required>
          <input className={inp} value={f.doctor_id} onChange={set('doctor_id')} placeholder="e.g. D-4F5E6D" required />
        </Field>
        <Field label="Appointment Date" required>
          <input type="date" className={inp} value={f.appointment_date} onChange={set('appointment_date')} required />
        </Field>
        <Field label="Appointment Time" required>
          <input type="time" className={inp} value={f.appointment_time} onChange={set('appointment_time')} required />
        </Field>
        <Field label="Status">
          <select className={sel} value={f.status} onChange={set('status')}>
            {APPT_STATUSES.map(s => <option key={s}>{s}</option>)}
          </select>
        </Field>
        <Field label="Reason for Visit">
          <input className={inp} value={f.reason_for_visit} onChange={set('reason_for_visit')} placeholder="Chief complaint" />
        </Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">
          Schedule Appointment
        </button>
      </div>
    </form>
  )
}

function TreatmentForm() {
  const [f, setF] = useState({ appointment_id:'', treatment_type:'', description:'', cost:'', treatment_date:'' })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.treatment({ ...f, cost: Number(f.cost) })
      toast.success('Treatment recorded')
      setF({ appointment_id:'', treatment_type:'', description:'', cost:'', treatment_date:'' })
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Failed to record treatment')
    }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Appointment ID" required>
          <input className={inp} value={f.appointment_id} onChange={set('appointment_id')} placeholder="e.g. A-7G8H9I" required />
        </Field>
        <Field label="Treatment Type" required>
          <select className={sel} value={f.treatment_type} onChange={set('treatment_type')} required>
            <option value="">Select type</option>
            {TREATMENT_TYPES.map(t => <option key={t}>{t}</option>)}
          </select>
        </Field>
        <Field label="Treatment Date" required>
          <input type="date" className={inp} value={f.treatment_date} onChange={set('treatment_date')} required />
        </Field>
        <Field label="Cost (₹)" required>
          <input type="number" min={0} step="0.01" className={inp} value={f.cost} onChange={set('cost')} placeholder="0.00" required />
        </Field>
        <Field label="Description">
          <input className={inp} value={f.description} onChange={set('description')} placeholder="Treatment notes" />
        </Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">
          Record Treatment
        </button>
      </div>
    </form>
  )
}

function BillingForm() {
  const [f, setF] = useState({ patient_id:'', treatment_id:'', bill_date:'', amount:'', payment_method:'', payment_status:'Pending' })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.billing({ ...f, amount: Number(f.amount) })
      toast.success('Bill generated successfully')
      setF({ patient_id:'', treatment_id:'', bill_date:'', amount:'', payment_method:'', payment_status:'Pending' })
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Failed to generate bill')
    }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required>
          <input className={inp} value={f.patient_id} onChange={set('patient_id')} placeholder="e.g. P-3A1B2C" required />
        </Field>
        <Field label="Treatment ID" required>
          <input className={inp} value={f.treatment_id} onChange={set('treatment_id')} placeholder="e.g. T-1J2K3L" required />
        </Field>
        <Field label="Bill Date" required>
          <input type="date" className={inp} value={f.bill_date} onChange={set('bill_date')} required />
        </Field>
        <Field label="Amount (₹)" required>
          <input type="number" min={0} step="0.01" className={inp} value={f.amount} onChange={set('amount')} placeholder="0.00" required />
        </Field>
        <Field label="Payment Method">
          <select className={sel} value={f.payment_method} onChange={set('payment_method')}>
            <option value="">Select method</option>
            {PAYMENT_METHODS.map(m => <option key={m}>{m}</option>)}
          </select>
        </Field>
        <Field label="Payment Status">
          <select className={sel} value={f.payment_status} onChange={set('payment_status')}>
            {PAYMENT_STATUSES.map(s => <option key={s}>{s}</option>)}
          </select>
        </Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">
          Generate Bill
        </button>
      </div>
    </form>
  )
}

function VitalsForm() {
  const [f, setF] = useState({
    patient_id: '', hospital: '', ward: '',
    heart_rate: '', spo2: '', systolic: '', diastolic: '',
    temperature_celsius: '', respiratory_rate: '',
  })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.vitals({
        ...f,
        heart_rate:          Number(f.heart_rate),
        spo2:                Number(f.spo2),
        systolic:            Number(f.systolic),
        diastolic:           Number(f.diastolic),
        temperature_celsius: Number(f.temperature_celsius),
        respiratory_rate:    Number(f.respiratory_rate),
      })
      toast.success('Vitals recorded successfully')
      setF({ patient_id:'', hospital:'', ward:'', heart_rate:'', spo2:'', systolic:'', diastolic:'', temperature_celsius:'', respiratory_rate:'' })
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Failed to record vitals')
    }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="bg-navy-900 border border-yellow-600/30 rounded-lg px-4 py-2 text-xs text-yellow-400 mb-2">
        Alert thresholds — HR &lt; 40 or &gt; 150 bpm = CRITICAL &nbsp;|&nbsp; SpO₂ &lt; 90% = CRITICAL &nbsp;|&nbsp; Temp &gt; 39°C or &lt; 35°C = warning
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required>
          <input className={inp} value={f.patient_id} onChange={set('patient_id')} placeholder="e.g. P001" required />
        </Field>
        <Field label="Hospital">
          <input className={inp} value={f.hospital} onChange={set('hospital')} placeholder="e.g. City General" />
        </Field>
        <Field label="Ward">
          <input className={inp} value={f.ward} onChange={set('ward')} placeholder="e.g. ICU, General" />
        </Field>
        <Field label="Heart Rate (bpm)" required>
          <input type="number" min={0} max={300} className={inp} value={f.heart_rate} onChange={set('heart_rate')} placeholder="60–100 normal" required />
        </Field>
        <Field label="SpO₂ (%)" required>
          <input type="number" min={0} max={100} step="0.1" className={inp} value={f.spo2} onChange={set('spo2')} placeholder="95–100 normal" required />
        </Field>
        <Field label="Systolic BP (mmHg)" required>
          <input type="number" min={0} max={300} className={inp} value={f.systolic} onChange={set('systolic')} placeholder="120 normal" required />
        </Field>
        <Field label="Diastolic BP (mmHg)" required>
          <input type="number" min={0} max={200} className={inp} value={f.diastolic} onChange={set('diastolic')} placeholder="80 normal" required />
        </Field>
        <Field label="Temperature (°C)" required>
          <input type="number" min={30} max={45} step="0.1" className={inp} value={f.temperature_celsius} onChange={set('temperature_celsius')} placeholder="36.5–37.5 normal" required />
        </Field>
        <Field label="Respiratory Rate (breaths/min)" required>
          <input type="number" min={0} max={60} className={inp} value={f.respiratory_rate} onChange={set('respiratory_rate')} placeholder="12–20 normal" required />
        </Field>

      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">
          Record Vitals
        </button>
      </div>
    </form>
  )
}

function AlertForm() {
  const ALERT_TYPES = ['ICU','LAB_CRITICAL','SPO2','COMPOSITE','HR']
  const SEVERITIES  = ['CRITICAL','HIGH','WARNING']
  const [f, setF] = useState({ patient_id:'', alert_type:'ICU', severity:'CRITICAL', alert_message:'', hospital:'City General Hospital', ward:'' })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      const res = await dataEntry.alert(f)
      toast.success(`Alert sent → Flink → doctor email (offset ${res.data.offset})`)
      setF(p => ({ ...p, patient_id:'', alert_message:'', ward:'' }))
    } catch (err) { toast.error(err.response?.data?.detail || 'Failed to send alert') }
  }

  const sevColor = { CRITICAL:'border-red-500/50 bg-red-500/5', HIGH:'border-orange-500/50 bg-orange-500/5', WARNING:'border-yellow-500/50 bg-yellow-500/5' }
  return (
    <form onSubmit={submit} className="space-y-4">
      <div className={`border rounded-lg px-4 py-2 text-xs mb-2 ${sevColor[f.severity] || ''}`}>
        <span className="text-slate-300">Flow: </span>
        <span className="text-white font-mono">Form → Kafka alerts → Flink enrichment → doctor lookup → Gmail email → MySQL</span>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required>
          <input className={inp} value={f.patient_id} onChange={set('patient_id')} placeholder="e.g. P006" required />
        </Field>
        <Field label="Alert Type" required>
          <select className={sel} value={f.alert_type} onChange={set('alert_type')} required>
            {ALERT_TYPES.map(t => <option key={t}>{t}</option>)}
          </select>
        </Field>
        <Field label="Severity" required>
          <select className={sel} value={f.severity} onChange={set('severity')} required>
            {SEVERITIES.map(s => <option key={s}>{s}</option>)}
          </select>
        </Field>
        <Field label="Ward">
          <select className={sel} value={f.ward} onChange={set('ward')}>
            <option value="">Select ward</option>
            {WARDS.map(w => <option key={w}>{w}</option>)}
          </select>
        </Field>
        <Field label="Hospital">
          <input className={inp} value={f.hospital} onChange={set('hospital')} />
        </Field>
        <Field label="Alert Message" required>
          <input className={inp} value={f.alert_message} onChange={set('alert_message')} placeholder="Describe the clinical situation" required />
        </Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-red-600 hover:bg-red-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">
          Send Alert via Flink
        </button>
      </div>
    </form>
  )
}

function LabReportForm() {
  const defaultTest = LAB_TESTS[0]
  const [f, setF] = useState({ patient_id:'', doctor_id:'', hospital:'City General Hospital', test_name: defaultTest.name, value:'', unit: defaultTest.unit, normal_range:`${defaultTest.lo}-${defaultTest.hi}`, flag:'normal', amount: defaultTest.cost })

  const onTestChange = (e) => {
    const t = LAB_TESTS.find(x => x.name === e.target.value) || LAB_TESTS[0]
    setF(p => ({ ...p, test_name: t.name, unit: t.unit, normal_range:`${t.lo}-${t.hi}`, amount: t.cost, flag: computeFlag(Number(p.value), t.lo, t.hi) }))
  }
  const onValueChange = (e) => {
    const t = LAB_TESTS.find(x => x.name === f.test_name) || LAB_TESTS[0]
    const v = Number(e.target.value)
    setF(p => ({ ...p, value: e.target.value, flag: computeFlag(v, t.lo, t.hi) }))
  }
  function computeFlag(v, lo, hi) {
    if (!v) return 'normal'
    if (v < lo * 0.7 || v > hi * 3) return 'critical'
    if (v < lo) return 'low'
    if (v > hi) return 'high'
    return 'normal'
  }

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.labReport({ ...f, value: Number(f.value), amount: Number(f.amount) })
      toast.success('Lab report submitted')
      setF(p => ({ ...p, patient_id:'', doctor_id:'', value:'' }))
    } catch (err) { toast.error(err.response?.data?.detail || 'Failed') }
  }

  const flagColor = { normal:'text-green-400', low:'text-yellow-400', high:'text-orange-400', critical:'text-red-400' }
  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required><input className={inp} value={f.patient_id} onChange={e=>setF(p=>({...p,patient_id:e.target.value}))} placeholder="e.g. P001" required /></Field>
        <Field label="Doctor ID" required><input className={inp} value={f.doctor_id} onChange={e=>setF(p=>({...p,doctor_id:e.target.value}))} placeholder="e.g. D001" required /></Field>
        <Field label="Test Name" required>
          <select className={sel} value={f.test_name} onChange={onTestChange} required>
            {LAB_TESTS.map(t => <option key={t.name}>{t.name}</option>)}
          </select>
        </Field>
        <Field label="Value" required>
          <input type="number" step="0.001" className={inp} value={f.value} onChange={onValueChange} placeholder={`Normal: ${f.normal_range} ${f.unit}`} required />
        </Field>
        <Field label="Unit"><input className={inp} value={f.unit} readOnly /></Field>
        <Field label="Normal Range"><input className={inp} value={f.normal_range} readOnly /></Field>
        <Field label="Flag (auto-computed)">
          <div className={`${inp} ${flagColor[f.flag] || 'text-white'} font-semibold`}>{f.flag.toUpperCase()}</div>
        </Field>
        <Field label="Amount (₹)"><input type="number" min={0} step="0.01" className={inp} value={f.amount} onChange={e=>setF(p=>({...p,amount:e.target.value}))} /></Field>
        <Field label="Hospital"><input className={inp} value={f.hospital} onChange={e=>setF(p=>({...p,hospital:e.target.value}))} /></Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">Submit Lab Report</button>
      </div>
    </form>
  )
}

function HospitalEventForm() {
  const [f, setF] = useState({ patient_id:'', department_id:'DEPT01', hospital:'City General Hospital', ward:'', event_type:'Admission', amount: HOSPITAL_EVENT_AMOUNTS['Admission'] })

  const onEventChange = (e) => {
    setF(p => ({ ...p, event_type: e.target.value, amount: HOSPITAL_EVENT_AMOUNTS[e.target.value] ?? 500 }))
  }

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.hospitalEvent({ ...f, amount: Number(f.amount) })
      toast.success('Hospital event recorded')
      setF(p => ({ ...p, patient_id:'' }))
    } catch (err) { toast.error(err.response?.data?.detail || 'Failed') }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required><input className={inp} value={f.patient_id} onChange={e=>setF(p=>({...p,patient_id:e.target.value}))} placeholder="e.g. P001" required /></Field>
        <Field label="Department" required>
          <select className={sel} value={f.department_id} onChange={e=>setF(p=>({...p,department_id:e.target.value}))} required>
            {DEPARTMENTS.map(d => <option key={d.id} value={d.id}>{d.id} — {d.name}</option>)}
          </select>
        </Field>
        <Field label="Event Type" required>
          <select className={sel} value={f.event_type} onChange={onEventChange} required>
            {HOSPITAL_EVENT_TYPES.map(t => <option key={t}>{t}</option>)}
          </select>
        </Field>
        <Field label="Ward">
          <select className={sel} value={f.ward} onChange={e=>setF(p=>({...p,ward:e.target.value}))}>
            <option value="">Select ward</option>
            {WARDS.map(w => <option key={w}>{w}</option>)}
          </select>
        </Field>
        <Field label="Amount (₹) (auto)"><input type="number" min={0} step="0.01" className={inp} value={f.amount} onChange={e=>setF(p=>({...p,amount:e.target.value}))} /></Field>
        <Field label="Hospital"><input className={inp} value={f.hospital} onChange={e=>setF(p=>({...p,hospital:e.target.value}))} /></Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">Record Event</button>
      </div>
    </form>
  )
}

function IcuCodeForm() {
  const [f, setF] = useState({ patient_id:'', department_id:'DEPT01', hospital:'City General Hospital', ward:'ICU-1', code_type:'Code_Blue', severity:'CRITICAL', amount: ICU_AMOUNTS['Code_Blue'], status:'Activated' })

  const onCodeChange = (e) => {
    const code = e.target.value
    setF(p => ({ ...p, code_type: code, severity: ICU_SEVERITY[code] || 'CRITICAL', amount: ICU_AMOUNTS[code] ?? 1000 }))
  }

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.icuCode({ ...f, amount: Number(f.amount) })
      toast.success('ICU code activated')
      setF(p => ({ ...p, patient_id:'' }))
    } catch (err) { toast.error(err.response?.data?.detail || 'Failed') }
  }

  const sevColor = { CRITICAL:'text-red-400', HIGH:'text-orange-400' }
  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="bg-navy-900 border border-red-600/30 rounded-lg px-4 py-2 text-xs text-red-400 mb-2">
        Warning — submitting this form triggers an ICU alert and emails the assigned doctor.
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required><input className={inp} value={f.patient_id} onChange={e=>setF(p=>({...p,patient_id:e.target.value}))} placeholder="e.g. P001" required /></Field>
        <Field label="Department" required>
          <select className={sel} value={f.department_id} onChange={e=>setF(p=>({...p,department_id:e.target.value}))} required>
            {DEPARTMENTS.map(d => <option key={d.id} value={d.id}>{d.id} — {d.name}</option>)}
          </select>
        </Field>
        <Field label="Code Type" required>
          <select className={sel} value={f.code_type} onChange={onCodeChange} required>
            {ICU_CODE_TYPES.map(c => <option key={c}>{c}</option>)}
          </select>
        </Field>
        <Field label="Ward" required>
          <select className={sel} value={f.ward} onChange={e=>setF(p=>({...p,ward:e.target.value}))} required>
            {WARDS.map(w => <option key={w}>{w}</option>)}
          </select>
        </Field>
        <Field label="Severity (auto)">
          <div className={`${inp} font-semibold ${sevColor[f.severity] || 'text-white'}`}>{f.severity}</div>
        </Field>
        <Field label="Amount (₹) (auto)"><input type="number" min={0} step="0.01" className={inp} value={f.amount} onChange={e=>setF(p=>({...p,amount:e.target.value}))} /></Field>
        <Field label="Status">
          <select className={sel} value={f.status} onChange={e=>setF(p=>({...p,status:e.target.value}))}>
            <option>Activated</option><option>Resolved</option><option>Ongoing</option>
          </select>
        </Field>
        <Field label="Hospital"><input className={inp} value={f.hospital} onChange={e=>setF(p=>({...p,hospital:e.target.value}))} /></Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-red-600 hover:bg-red-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">Activate ICU Code</button>
      </div>
    </form>
  )
}

function DepartmentForm() {
  const [f, setF] = useState({ department_id:'', department_name:'', hospital_branch:'' })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.department(f)
      toast.success('Department added')
      setF({ department_id:'', department_name:'', hospital_branch:'' })
    } catch (err) { toast.error(err.response?.data?.detail || 'Failed') }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Department ID" required><input className={inp} value={f.department_id} onChange={set('department_id')} placeholder="e.g. DEPT06" required /></Field>
        <Field label="Department Name" required><input className={inp} value={f.department_name} onChange={set('department_name')} placeholder="e.g. Radiology" required /></Field>
        <Field label="Hospital Branch" required>
          <select className={sel} value={f.hospital_branch} onChange={set('hospital_branch')} required>
            <option value="">Select branch</option>
            {BRANCHES.map(b => <option key={b}>{b}</option>)}
          </select>
        </Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">Add Department</button>
      </div>
    </form>
  )
}

const FORMS = { patient: PatientForm, doctor: DoctorForm, appointment: AppointmentForm, treatment: TreatmentForm, billing: BillingForm, vitals: VitalsForm, alert: AlertForm, lab: LabReportForm, hospital: HospitalEventForm, icu: IcuCodeForm, department: DepartmentForm }

export default function DataEntry() {
  const [active, setActive] = useState('patient')
  const ActiveForm = FORMS[active]

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-bold text-white">Data Entry</h1>

      {/* Tabs */}
      <div className="flex gap-1 bg-navy-800 p-1 rounded-xl w-fit flex-wrap">
        {TABS.map(({ id, label }) => (
          <button
            key={id}
            onClick={() => setActive(id)}
            className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors ${
              active === id ? 'bg-brand-600 text-white' : 'text-slate-400 hover:text-white hover:bg-navy-700'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Form card */}
      <div className="bg-navy-800 rounded-xl border border-navy-700 p-6">
        <h2 className="text-base font-semibold text-white mb-5">
          {TABS.find(t => t.id === active)?.label}
        </h2>
        <ActiveForm />
      </div>
    </div>
  )
}

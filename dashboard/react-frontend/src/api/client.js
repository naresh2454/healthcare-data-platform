import axios from 'axios'

const api = axios.create({ baseURL: '/api' })

export const financial = {
  summary:               () => api.get('/financial/summary'),
  revenueByDoctor:       () => api.get('/financial/revenue-by-doctor'),
  revenueBySpec:         () => api.get('/financial/revenue-by-specialization'),
  revenueByBranch:       () => api.get('/financial/revenue-by-branch'),
  monthlyRevenue:        () => api.get('/financial/monthly-revenue'),
  billingPayment:        () => api.get('/financial/billing-payment'),
  treatmentCost:         () => api.get('/financial/treatment-cost'),
  outstandingPayments:   () => api.get('/financial/outstanding-payments'),
}

export const operational = {
  summary:           () => api.get('/operational/summary'),
  doctorWorkload:    () => api.get('/operational/doctor-workload'),
  appointmentStatus: () => api.get('/operational/appointment-status'),
  peakHours:         () => api.get('/operational/peak-hours'),
  topDoctors:        () => api.get('/operational/top-doctors-scorecard'),
}

export const patients = {
  summary:         () => api.get('/patients/summary'),
  ageGroups:       () => api.get('/patients/age-groups'),
  retention:       () => api.get('/patients/retention'),
  newTrend:        () => api.get('/patients/new-patient-trend'),
  spending:        () => api.get('/patients/spending'),
}

export const pipeline = {
  lastRun:  () => api.get('/pipeline/last-run'),
  runs:     () => api.get('/pipeline/runs'),
  dagInfo:  () => api.get('/pipeline/dag-info'),
}

export const infrastructure = {
  status: () => api.get('/infrastructure/status'),
}

export const chat = {
  message: (body) => fetch('/api/chat/message', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }),
  insights: () => api.post('/chat/insights', {}),
}

export const monitoring = {
  summary:            () => api.get('/monitoring/summary'),
  alertsByType:       () => api.get('/monitoring/alerts-by-type'),
  recentAlerts:       () => api.get('/monitoring/recent-alerts'),
  vitalsSummary:      () => api.get('/monitoring/vitals-summary'),
  labSummary:         () => api.get('/monitoring/lab-summary'),
  icuSummary:         () => api.get('/monitoring/icu-summary'),
  patientAlertSummary:() => api.get('/monitoring/patient-alert-summary'),
}

export const dataEntry = {
  patient:       (d) => api.post('/data-entry/patient',        d),
  doctor:        (d) => api.post('/data-entry/doctor',         d),
  appointment:   (d) => api.post('/data-entry/appointment',    d),
  treatment:     (d) => api.post('/data-entry/treatment',      d),
  billing:       (d) => api.post('/data-entry/billing',        d),
  vitals:        (d) => api.post('/data-entry/vitals',         d),
  alert:         (d) => api.post('/data-entry/alert',          d),
  labReport:     (d) => api.post('/data-entry/lab-report',     d),
  hospitalEvent: (d) => api.post('/data-entry/hospital-event', d),
  icuCode:       (d) => api.post('/data-entry/icu-code',       d),
  department:    (d) => api.post('/data-entry/department',     d),
}

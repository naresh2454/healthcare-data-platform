import { useEffect, useState } from 'react'
import { monitoring } from '../api/client'

function StatBox({ label, value, sub, color = 'text-white' }) {
  return (
    <div className="bg-navy-800 border border-navy-700 rounded-xl p-4">
      <p className="text-xs text-slate-400 mb-1">{label}</p>
      <p className={`text-2xl font-bold ${color}`}>{value ?? '—'}</p>
      {sub && <p className="text-xs text-slate-500 mt-1">{sub}</p>}
    </div>
  )
}

const SEVERITY_COLOR = {
  CRITICAL: 'text-red-400 bg-red-400/10 border-red-500/30',
  HIGH:     'text-orange-400 bg-orange-400/10 border-orange-500/30',
  WARNING:  'text-yellow-400 bg-yellow-400/10 border-yellow-500/30',
}
const SEVERITY_DOT = {
  CRITICAL: 'bg-red-500',
  HIGH:     'bg-orange-400',
  WARNING:  'bg-yellow-400',
}

export default function Monitoring() {
  const [summary, setSummary]     = useState(null)
  const [byType, setByType]       = useState([])
  const [recent, setRecent]       = useState([])
  const [vitals, setVitals]       = useState([])
  const [labs, setLabs]           = useState([])
  const [icu, setIcu]             = useState([])
  const [tab, setTab]             = useState('alerts')
  const [loading, setLoading]     = useState(true)

  useEffect(() => {
    Promise.all([
      monitoring.summary(),
      monitoring.alertsByType(),
      monitoring.recentAlerts(),
      monitoring.vitalsSummary(),
      monitoring.labSummary(),
      monitoring.icuSummary(),
    ]).then(([s, bt, ra, v, l, i]) => {
      setSummary(s.data)
      setByType(bt.data)
      setRecent(ra.data)
      setVitals(v.data)
      setLabs(l.data)
      setIcu(i.data)
    }).finally(() => setLoading(false))
  }, [])

  if (loading) return <p className="text-slate-400 text-sm">Loading monitoring data…</p>

  const TABS = [
    { id: 'alerts',  label: 'Recent Alerts' },
    { id: 'vitals',  label: 'Vitals Anomalies' },
    { id: 'labs',    label: 'Lab Results' },
    { id: 'icu',     label: 'ICU Codes' },
  ]

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-bold text-white">Flink Monitoring</h1>

      {/* Summary stats */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <StatBox label="Total Alerts" value={summary?.total_alerts?.toLocaleString()} />
        <StatBox label="Critical" value={summary?.critical_count?.toLocaleString()} color="text-red-400" />
        <StatBox label="Last 24h" value={summary?.last_24h?.toLocaleString()} color="text-orange-400" />
        <StatBox label="Emails Sent" value={summary?.emails_sent?.toLocaleString()} color="text-green-400" sub="to doctors" />
        <StatBox label="ICU Activations" value={summary?.icu_activations?.toLocaleString()} color="text-purple-400" sub="Spark aggregated" />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <StatBox label="Vitals Anomalies" value={summary?.vitals_anomalies?.toLocaleString()} color="text-yellow-400" sub="Spark aggregated" />
        <StatBox label="Critical Lab Tests" value={summary?.critical_labs?.toLocaleString()} color="text-red-400" sub="Spark aggregated" />
        <StatBox label="Last Hour Alerts" value={summary?.last_1h?.toLocaleString()} color="text-brand-400" sub="live from Flink" />
      </div>

      {/* Alert type breakdown */}
      <div className="bg-navy-800 border border-navy-700 rounded-xl p-5">
        <h2 className="text-sm font-semibold text-white mb-4">Alerts by Type (Flink detected)</h2>
        <div className="flex flex-wrap gap-2">
          {byType.map((r, i) => (
            <span key={i} className={`px-3 py-1.5 rounded-lg border text-xs font-medium ${SEVERITY_COLOR[r.severity] || 'text-slate-400 bg-navy-700 border-navy-600'}`}>
              {r.alert_type} / {r.severity} — {r.count.toLocaleString()}
            </span>
          ))}
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 bg-navy-800 p-1 rounded-xl w-fit flex-wrap">
        {TABS.map(({ id, label }) => (
          <button key={id} onClick={() => setTab(id)}
            className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors ${tab === id ? 'bg-brand-600 text-white' : 'text-slate-400 hover:text-white hover:bg-navy-700'}`}>
            {label}
          </button>
        ))}
      </div>

      {/* Recent Alerts */}
      {tab === 'alerts' && (
        <div className="bg-navy-800 border border-navy-700 rounded-xl overflow-hidden">
          <div className="px-5 py-3 border-b border-navy-700">
            <h2 className="text-sm font-semibold text-white">Last 50 Flink-Generated Alerts</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-navy-700 text-slate-400">
                  <th className="px-4 py-2 text-left">Time</th>
                  <th className="px-4 py-2 text-left">Severity</th>
                  <th className="px-4 py-2 text-left">Type</th>
                  <th className="px-4 py-2 text-left">Patient</th>
                  <th className="px-4 py-2 text-left">Doctor</th>
                  <th className="px-4 py-2 text-left">Message</th>
                  <th className="px-4 py-2 text-left">Emailed</th>
                </tr>
              </thead>
              <tbody>
                {recent.map((r) => (
                  <tr key={r.alert_id} className="border-b border-navy-700/50 hover:bg-navy-700/30">
                    <td className="px-4 py-2 text-slate-400 whitespace-nowrap">{r.ts?.slice(0, 16).replace('T', ' ')}</td>
                    <td className="px-4 py-2">
                      <span className={`px-2 py-0.5 rounded text-xs font-semibold flex items-center gap-1 w-fit ${SEVERITY_COLOR[r.severity] || ''}`}>
                        <span className={`w-1.5 h-1.5 rounded-full ${SEVERITY_DOT[r.severity] || 'bg-slate-400'}`} />
                        {r.severity}
                      </span>
                    </td>
                    <td className="px-4 py-2 text-slate-300">{r.alert_type}</td>
                    <td className="px-4 py-2 text-white font-mono">{r.patient_id}</td>
                    <td className="px-4 py-2 text-slate-300 font-mono">{r.doctor_id || '—'}</td>
                    <td className="px-4 py-2 text-slate-400 max-w-xs truncate">{r.alert_message}</td>
                    <td className="px-4 py-2">
                      <span className={r.is_email_sent ? 'text-green-400' : 'text-slate-600'}>
                        {r.is_email_sent ? 'Yes' : 'No'}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Vitals Anomalies */}
      {tab === 'vitals' && (
        <div className="bg-navy-800 border border-navy-700 rounded-xl overflow-hidden">
          <div className="px-5 py-3 border-b border-navy-700">
            <h2 className="text-sm font-semibold text-white">Vitals Anomaly Summary per Patient (Spark aggregated)</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-navy-700 text-slate-400">
                  <th className="px-4 py-2 text-left">Patient</th>
                  <th className="px-4 py-2 text-right">Readings</th>
                  <th className="px-4 py-2 text-right">Anomalies</th>
                  <th className="px-4 py-2 text-right">Anomaly %</th>
                  <th className="px-4 py-2 text-right">Avg HR</th>
                  <th className="px-4 py-2 text-right">Avg SpO₂</th>
                  <th className="px-4 py-2 text-right">Avg Sys/Dia</th>
                  <th className="px-4 py-2 text-right">Avg Temp °C</th>
                  <th className="px-4 py-2 text-right">Avg RR</th>
                </tr>
              </thead>
              <tbody>
                {vitals.map((r) => (
                  <tr key={r.patient_id} className="border-b border-navy-700/50 hover:bg-navy-700/30">
                    <td className="px-4 py-2 text-white font-mono">{r.patient_id}</td>
                    <td className="px-4 py-2 text-right text-slate-300">{r.total_readings}</td>
                    <td className="px-4 py-2 text-right text-orange-400">{r.anomaly_count}</td>
                    <td className="px-4 py-2 text-right">
                      <span className={r.anomaly_rate_pct > 20 ? 'text-red-400' : r.anomaly_rate_pct > 10 ? 'text-yellow-400' : 'text-green-400'}>
                        {r.anomaly_rate_pct}%
                      </span>
                    </td>
                    <td className="px-4 py-2 text-right text-slate-300">{r.avg_heart_rate}</td>
                    <td className="px-4 py-2 text-right text-slate-300">{r.avg_spo2}%</td>
                    <td className="px-4 py-2 text-right text-slate-300">{r.avg_systolic}/{r.avg_diastolic}</td>
                    <td className="px-4 py-2 text-right text-slate-300">{r.avg_temperature}</td>
                    <td className="px-4 py-2 text-right text-slate-300">{r.avg_respiratory_rate}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Lab Results */}
      {tab === 'labs' && (
        <div className="bg-navy-800 border border-navy-700 rounded-xl overflow-hidden">
          <div className="px-5 py-3 border-b border-navy-700">
            <h2 className="text-sm font-semibold text-white">Lab Test Summary (Spark aggregated from Flink data)</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-navy-700 text-slate-400">
                  <th className="px-4 py-2 text-left">Test</th>
                  <th className="px-4 py-2 text-right">Total</th>
                  <th className="px-4 py-2 text-right">Normal</th>
                  <th className="px-4 py-2 text-right">Low</th>
                  <th className="px-4 py-2 text-right">High</th>
                  <th className="px-4 py-2 text-right">Critical</th>
                  <th className="px-4 py-2 text-right">Critical %</th>
                  <th className="px-4 py-2 text-right">Revenue ₹</th>
                </tr>
              </thead>
              <tbody>
                {labs.map((r) => (
                  <tr key={r.test_name} className="border-b border-navy-700/50 hover:bg-navy-700/30">
                    <td className="px-4 py-2 text-white">{r.test_name}</td>
                    <td className="px-4 py-2 text-right text-slate-300">{r.total_tests}</td>
                    <td className="px-4 py-2 text-right text-green-400">{r.normal_count}</td>
                    <td className="px-4 py-2 text-right text-yellow-400">{r.low_count}</td>
                    <td className="px-4 py-2 text-right text-orange-400">{r.high_count}</td>
                    <td className="px-4 py-2 text-right text-red-400">{r.critical_count}</td>
                    <td className="px-4 py-2 text-right">
                      <span className={r.critical_rate_pct > 15 ? 'text-red-400' : 'text-slate-300'}>
                        {r.critical_rate_pct}%
                      </span>
                    </td>
                    <td className="px-4 py-2 text-right text-slate-300">₹{Number(r.total_revenue).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ICU Codes */}
      {tab === 'icu' && (
        <div className="bg-navy-800 border border-navy-700 rounded-xl overflow-hidden">
          <div className="px-5 py-3 border-b border-navy-700">
            <h2 className="text-sm font-semibold text-white">ICU Code Activations (Flink → Spark)</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-navy-700 text-slate-400">
                  <th className="px-4 py-2 text-left">Code Type</th>
                  <th className="px-4 py-2 text-left">Severity</th>
                  <th className="px-4 py-2 text-right">Activations</th>
                  <th className="px-4 py-2 text-right">Total Cost ₹</th>
                  <th className="px-4 py-2 text-right">Avg Cost ₹</th>
                </tr>
              </thead>
              <tbody>
                {icu.map((r, i) => (
                  <tr key={i} className="border-b border-navy-700/50 hover:bg-navy-700/30">
                    <td className="px-4 py-2 text-white">{r.code_type}</td>
                    <td className="px-4 py-2">
                      <span className={`px-2 py-0.5 rounded text-xs font-semibold ${SEVERITY_COLOR[r.severity] || 'text-slate-400'}`}>
                        {r.severity}
                      </span>
                    </td>
                    <td className="px-4 py-2 text-right text-orange-400 font-semibold">{r.code_count.toLocaleString()}</td>
                    <td className="px-4 py-2 text-right text-slate-300">₹{Number(r.total_amount).toLocaleString()}</td>
                    <td className="px-4 py-2 text-right text-slate-300">₹{Number(r.avg_amount).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

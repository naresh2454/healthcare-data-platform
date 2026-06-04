import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import Financial from './pages/Financial'
import Operational from './pages/Operational'
import Patients from './pages/Patients'
import Pipeline from './pages/Pipeline'
import DataEntry from './pages/DataEntry'
import Infrastructure from './pages/Infrastructure'
import Chat from './pages/Chat'
import Monitoring from './pages/Monitoring'

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Navigate to="/financial" replace />} />
        <Route path="/financial"     element={<Financial />} />
        <Route path="/operational"   element={<Operational />} />
        <Route path="/patients"      element={<Patients />} />
        <Route path="/pipeline"      element={<Pipeline />} />
        <Route path="/data-entry"    element={<DataEntry />} />
        <Route path="/infrastructure" element={<Infrastructure />} />
        <Route path="/chat"          element={<Chat />} />
        <Route path="/monitoring"    element={<Monitoring />} />
      </Routes>
    </Layout>
  )
}

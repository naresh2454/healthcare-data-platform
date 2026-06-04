import { NavLink } from 'react-router-dom'

const NAV = [
  { to: '/financial',      icon: '📊', label: 'Financial'       },
  { to: '/operational',    icon: '⚙️',  label: 'Operational'     },
  { to: '/patients',       icon: '👥', label: 'Patients'        },
  { to: '/pipeline',       icon: '🔄', label: 'Pipeline'        },
  { to: '/data-entry',     icon: '📋', label: 'Data Entry'      },
  { to: '/infrastructure', icon: '🖥️', label: 'Infrastructure'  },
  { to: '/monitoring',     icon: '🚨', label: 'Monitoring'      },
  { to: '/chat',           icon: '💬', label: 'Ask AI'          },
]

export default function Layout({ children }) {
  return (
    <div className="flex h-screen overflow-hidden">
      {/* Sidebar */}
      <aside className="w-52 bg-navy-800 border-r border-navy-700 flex flex-col shrink-0">
        <div className="px-5 py-5 border-b border-navy-700">
          <span className="text-lg font-bold text-white">🏥 HealthCare</span>
          <p className="text-xs text-slate-400 mt-0.5">Analytics Platform</p>
        </div>
        <nav className="flex-1 py-4 space-y-1 px-2">
          {NAV.map(({ to, icon, label }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) =>
                `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-brand-600 text-white'
                    : 'text-slate-400 hover:bg-navy-700 hover:text-white'
                }`
              }
            >
              <span className="text-base">{icon}</span>
              {label}
            </NavLink>
          ))}
        </nav>
        <div className="px-4 py-3 border-t border-navy-700 text-xs text-slate-500">
          EC22 · Analytics v1.0
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto bg-navy-900 p-6">
        {children}
      </main>
    </div>
  )
}

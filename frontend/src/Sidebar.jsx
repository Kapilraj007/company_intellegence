import { T } from "./theme";
import { Badge, Dot, Logo } from "./components";

const NAV = [
  { id: "dashboard", icon: "⬡", label: "Dashboard" },
  { id: "pipeline",  icon: "◈", label: "Pipeline"  },
  { id: "explorer",  icon: "✦", label: "Search"    },
  { id: "clusters",  icon: "◎", label: "Clusters"  },
  { id: "analytics", icon: "◌", label: "Analytics" },
  { id: "outputs",   icon: "◉", label: "Outputs"   },
  { id: "profile",   icon: "◍", label: "Profile"   },
];

export default function Sidebar({ page, setPage, user, apiInfo }) {
  return (
    <>
      <style>{`
        .sidebar {
          width: 220px;
          min-width: 220px;
          background: ${T.navy2};
          border-right: 1px solid ${T.border};
          display: flex;
          flex-direction: column;
          padding: 24px 0;
          position: sticky;
          top: 0;
          height: 100vh;
          overflow-y: auto;
          flex-shrink: 0;
        }

        .sidebar-top {
          padding: 0 22px 28px;
        }

        .sidebar-nav {
          flex: 1;
          padding: 0 12px;
          display: flex;
          flex-direction: column;
          gap: 4px;
        }

        .sidebar-active-dot {
          margin-left: auto;
          width: 3px;
          height: 3px;
          border-radius: 50%;
          background: ${T.amber};
        }

        .sidebar-user {
          padding: 16px 22px;
          border-top: 1px solid ${T.border};
          display: flex;
          align-items: center;
          gap: 10px;
        }

        .sidebar-status {
          padding: 0 22px 18px;
        }

        @media (max-width: 900px) {
          .sidebar {
            width: 100%;
            min-width: 0;
            height: auto;
            border-right: none;
            border-bottom: 1px solid ${T.border};
            padding: 12px;
            position: sticky;
            z-index: 20;
            overflow: visible;
          }

          .sidebar-top {
            padding: 0;
            margin-bottom: 10px;
          }

          .sidebar-nav {
            padding: 0;
            flex-direction: row;
            overflow-x: auto;
            gap: 8px;
          }

          .sidebar-user {
            display: none;
          }

          .sidebar-active-dot {
            display: none;
          }

          .sidebar-status {
            padding: 0 0 10px;
          }
        }

        @media (max-width: 560px) {
          .sidebar-label { display: none; }
          .sidebar-nav button {
            padding: 8px 10px !important;
          }
        }
      `}</style>

      <aside className="sidebar">
        <div className="sidebar-top">
          <Logo size={18} />
        </div>

        <div className="sidebar-status">
          <Badge color={apiInfo?.online ? T.green : T.red}>
            <Dot color={apiInfo?.online ? T.green : T.red} pulse={apiInfo?.online}/>
            {apiInfo?.online ? "API Online" : "API Offline"}
          </Badge>
          <div style={{ fontSize: 11, color: T.muted, marginTop: 8, lineHeight: 1.5 }}>
            {apiInfo?.online
              ? `${apiInfo.tasks_in_memory} task${apiInfo.tasks_in_memory === 1 ? "" : "s"} currently tracked in memory`
              : "Start the FastAPI server to enable live jobs, search, and outputs."}
          </div>
        </div>

        <nav className="sidebar-nav">
          {NAV.map(n => {
            const active = page === n.id || (n.id === "explorer" && page === "search-detail");
            return (
              <button key={n.id} onClick={() => setPage(n.id)} style={{
                display: "flex", alignItems: "center", gap: 12,
                padding: "10px 14px", borderRadius: 8, border: "none",
                cursor: "pointer", width: "100%", textAlign: "left", minWidth: "max-content",
                background: active ? T.amber + "18" : "transparent",
                color: active ? T.amber : T.muted,
                fontFamily: "'Syne', sans-serif", fontWeight: active ? 700 : 500,
                fontSize: 13, transition: "all 0.15s",
              }}>
                <span style={{ fontSize: 17, flexShrink: 0 }}>{n.icon}</span>
                <span className="sidebar-label">{n.label}</span>
                {active && <span className="sidebar-active-dot" />}
              </button>
            );
          })}
        </nav>

        <div className="sidebar-user">
          <div style={{
            width: 34, height: 34, borderRadius: 8, flexShrink: 0,
            background: T.amber + "20", border: `1px solid ${T.amber}40`,
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 14, color: T.amber, fontWeight: 700, fontFamily: "'Syne', sans-serif",
          }}>
            {user.name[0].toUpperCase()}
          </div>
          <div className="sidebar-user-info">
            <div style={{ fontSize: 13, fontWeight: 600, color: T.white, fontFamily: "'Syne', sans-serif" }}>{user.name}</div>
            <div style={{ fontSize: 11, color: T.muted }}>{user.role}</div>
          </div>
        </div>
      </aside>
    </>
  );
}

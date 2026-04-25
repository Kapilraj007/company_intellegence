import { useCallback, useEffect, useMemo, useState } from "react";
import {
  approveUser,
  getAdminActivityLogs,
  getAdminDashboard,
  getAdminDataVersions,
  getAdminErrorLogs,
  getAdminPipelineRuns,
  getAdminUsers,
  rejectUser,
  updateUserRole,
  verifyUser,
} from "../api";
import "./admin-panel.css";

const NAV_ITEMS = [
  { id: "overview", label: "Overview" },
  { id: "users", label: "User Approval" },
  { id: "pipelines", label: "Pipelines" },
  { id: "versions", label: "Data Versions" },
  { id: "activity", label: "Activity Logs" },
  { id: "errors", label: "Error Monitor" },
];

function formatDateTime(value) {
  const raw = String(value || "").trim();
  if (!raw) return "n/a";
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return raw;
  return date.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function compactNumber(value) {
  const numeric = Number(value || 0);
  return Number.isFinite(numeric) ? numeric.toLocaleString() : "0";
}

function statusPillClass(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized === "approved" || normalized === "completed" || normalized === "verified" || normalized === "success") {
    return "admin-pill ok";
  }
  if (normalized === "pending" || normalized === "running") {
    return "admin-pill warn";
  }
  if (normalized === "rejected" || normalized === "failed" || normalized === "error") {
    return "admin-pill bad";
  }
  return "admin-pill";
}

export default function AdminPanelApp({ user, onLogout, onOpenWorkspace = () => {} }) {
  const [activeNav, setActiveNav] = useState("overview");
  const [userFilter, setUserFilter] = useState("pending");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [actionBusyId, setActionBusyId] = useState("");
  const [dashboard, setDashboard] = useState(null);
  const [users, setUsers] = useState([]);
  const [runs, setRuns] = useState([]);
  const [activity, setActivity] = useState([]);
  const [errors, setErrors] = useState([]);
  const [versions, setVersions] = useState([]);

  const refreshData = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [dashboardRes, usersRes, runsRes, activityRes, errorsRes, versionsRes] = await Promise.all([
        getAdminDashboard(),
        getAdminUsers({ approval_status: userFilter, limit: 800 }),
        getAdminPipelineRuns(400),
        getAdminActivityLogs(500),
        getAdminErrorLogs(300),
        getAdminDataVersions(500),
      ]);
      setDashboard(dashboardRes || null);
      setUsers(Array.isArray(usersRes?.users) ? usersRes.users : []);
      setRuns(Array.isArray(runsRes?.runs) ? runsRes.runs : []);
      setActivity(Array.isArray(activityRes?.logs) ? activityRes.logs : []);
      setErrors(Array.isArray(errorsRes?.errors) ? errorsRes.errors : []);
      setVersions(Array.isArray(versionsRes?.versions) ? versionsRes.versions : []);
    } catch (requestError) {
      setError(requestError?.message || "Unable to load admin panel data.");
    } finally {
      setLoading(false);
    }
  }, [userFilter]);

  useEffect(() => {
    refreshData();
  }, [refreshData]);

  const stats = useMemo(() => {
    const usersPayload = dashboard?.users || {};
    const pipelinePayload = dashboard?.pipelines || {};
    const storagePayload = dashboard?.storage || {};
    const activityPayload = dashboard?.activity || {};
    const errorsPayload = dashboard?.errors || {};
    return {
      usersTotal: compactNumber(usersPayload.total),
      usersPending: compactNumber(usersPayload.pending),
      pipelineTotal: compactNumber(pipelinePayload.total_runs),
      pipelineRunning: compactNumber(pipelinePayload.running),
      storageBytes: compactNumber(storagePayload?.bytes?.total),
      storageVersions: compactNumber(storagePayload.version_events || storagePayload.versions_count),
      activityTotal: compactNumber(activityPayload.total_events),
      errorsTotal: compactNumber(errorsPayload.total),
    };
  }, [dashboard]);

  const handleUserAction = useCallback(
    async (targetUserId, action) => {
      setActionBusyId(targetUserId);
      setError("");
      try {
        const target = users.find((candidate) => candidate.user_id === targetUserId);
        if (action === "approve") {
          await approveUser(targetUserId);
        } else if (action === "reject") {
          await rejectUser(targetUserId);
        } else if (action === "verify") {
          const isVerified = String(target?.verification_status || "").toLowerCase() === "verified";
          await verifyUser(targetUserId, { verified: !isVerified });
        } else if (action === "toggle-role") {
          const nextRole = String(target?.role || "").toLowerCase() === "admin" ? "user" : "admin";
          await updateUserRole(targetUserId, nextRole);
        }
        await refreshData();
      } catch (requestError) {
        setError(requestError?.message || "User action failed.");
      } finally {
        setActionBusyId("");
      }
    },
    [refreshData, users]
  );

  return (
    <div className="admin-root">
      <div className="admin-layout">
        <aside className="admin-sidebar">
          <div className="admin-brand">
            <div className="admin-brand-mark">AP</div>
            <div>
              <div className="admin-brand-title">Admin Platform</div>
              <div className="admin-brand-sub">Governance and Operations</div>
            </div>
          </div>

          <nav className="admin-nav">
            {NAV_ITEMS.map((item) => (
              <button
                key={item.id}
                className={`admin-nav-btn ${activeNav === item.id ? "active" : ""}`}
                onClick={() => setActiveNav(item.id)}
              >
                {item.label}
              </button>
            ))}
          </nav>

          <div className="admin-sidebar-footer">
            <div className="admin-user-chip">
              <span className="admin-user-dot" />
              <span>{user?.name || "Admin"}</span>
            </div>
            <div className="admin-actions">
              <button className="admin-plain-btn" onClick={refreshData}>Refresh</button>
              <button className="admin-plain-btn" onClick={onOpenWorkspace}>Workspace</button>
              <button className="admin-plain-btn danger" onClick={onLogout}>Logout</button>
            </div>
          </div>
        </aside>

        <main className="admin-main">
          <header className="admin-header">
            <h1>Operations Console</h1>
            <p>Approval workflow, auditability, pipeline monitoring, and version oversight.</p>
          </header>

          {error && <div className="admin-alert">{error}</div>}
          {loading ? <div className="admin-loading">Loading admin data…</div> : null}

          <section className="admin-stat-grid">
            <article className="admin-stat-card">
              <h2>Users</h2>
              <strong>{stats.usersTotal}</strong>
              <span>{stats.usersPending} pending approval</span>
            </article>
            <article className="admin-stat-card">
              <h2>Pipelines</h2>
              <strong>{stats.pipelineTotal}</strong>
              <span>{stats.pipelineRunning} active</span>
            </article>
            <article className="admin-stat-card">
              <h2>Storage</h2>
              <strong>{stats.storageBytes} bytes</strong>
              <span>{stats.storageVersions} versions tracked</span>
            </article>
            <article className="admin-stat-card">
              <h2>Telemetry</h2>
              <strong>{stats.activityTotal}</strong>
              <span>{stats.errorsTotal} errors recorded</span>
            </article>
          </section>

          {activeNav === "overview" ? (
            <section className="admin-section">
              <h3>System Snapshot</h3>
              <div className="admin-grid-2">
                <div className="admin-panel">
                  <h4>Usage</h4>
                  <div className="admin-kv"><span>Approved users</span><b>{compactNumber(dashboard?.users?.approved)}</b></div>
                  <div className="admin-kv"><span>Rejected users</span><b>{compactNumber(dashboard?.users?.rejected)}</b></div>
                  <div className="admin-kv"><span>Last 24h signups</span><b>{compactNumber(dashboard?.users?.last_24h_signups)}</b></div>
                </div>
                <div className="admin-panel">
                  <h4>Pipelines</h4>
                  <div className="admin-kv"><span>Completed</span><b>{compactNumber(dashboard?.pipelines?.completed)}</b></div>
                  <div className="admin-kv"><span>Failed</span><b>{compactNumber(dashboard?.pipelines?.failed)}</b></div>
                  <div className="admin-kv"><span>Last 24h runs</span><b>{compactNumber(dashboard?.pipelines?.last_24h_runs)}</b></div>
                </div>
              </div>
            </section>
          ) : null}

          {activeNav === "users" ? (
            <section className="admin-section">
              <div className="admin-row between">
                <h3>User Approval Queue</h3>
                <select value={userFilter} onChange={(event) => setUserFilter(event.target.value)} className="admin-select">
                  <option value="pending">Pending</option>
                  <option value="approved">Approved</option>
                  <option value="rejected">Rejected</option>
                  <option value="">All</option>
                </select>
              </div>
              <div className="admin-table-wrap">
                <table className="admin-table">
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Email</th>
                      <th>Role</th>
                      <th>Verification</th>
                      <th>Approval</th>
                      <th>Created</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {users.length === 0 ? (
                      <tr>
                        <td colSpan={7} className="admin-empty-cell">No users in this filter.</td>
                      </tr>
                    ) : users.map((entry) => (
                      <tr key={entry.user_id}>
                        <td>{entry.name || "n/a"}</td>
                        <td>{entry.email || "n/a"}</td>
                        <td><span className={statusPillClass(entry.role)}>{entry.role || "user"}</span></td>
                        <td><span className={statusPillClass(entry.verification_status)}>{entry.verification_status || "unverified"}</span></td>
                        <td><span className={statusPillClass(entry.approval_status)}>{entry.approval_status || "pending"}</span></td>
                        <td>{formatDateTime(entry.created_at)}</td>
                        <td>
                          <div className="admin-cell-actions">
                            <button
                              className="admin-tiny-btn"
                              disabled={actionBusyId === entry.user_id}
                              onClick={() => handleUserAction(entry.user_id, "verify")}
                            >
                              {String(entry.verification_status || "").toLowerCase() === "verified" ? "Unverify" : "Verify"}
                            </button>
                            <button
                              className="admin-tiny-btn"
                              disabled={actionBusyId === entry.user_id}
                              onClick={() => handleUserAction(entry.user_id, "approve")}
                            >
                              Approve
                            </button>
                            <button
                              className="admin-tiny-btn"
                              disabled={actionBusyId === entry.user_id}
                              onClick={() => handleUserAction(entry.user_id, "reject")}
                            >
                              Reject
                            </button>
                            <button
                              className="admin-tiny-btn"
                              disabled={actionBusyId === entry.user_id}
                              onClick={() => handleUserAction(entry.user_id, "toggle-role")}
                            >
                              {String(entry.role || "").toLowerCase() === "admin" ? "Make User" : "Make Admin"}
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          ) : null}

          {activeNav === "pipelines" ? (
            <section className="admin-section">
              <h3>Pipeline Execution Monitor</h3>
              <div className="admin-table-wrap">
                <table className="admin-table">
                  <thead>
                    <tr>
                      <th>Run ID</th>
                      <th>User</th>
                      <th>Company</th>
                      <th>Status</th>
                      <th>Started</th>
                      <th>Completed</th>
                    </tr>
                  </thead>
                  <tbody>
                    {runs.length === 0 ? (
                      <tr><td colSpan={6} className="admin-empty-cell">No pipeline runs available.</td></tr>
                    ) : runs.slice(0, 250).map((run) => (
                      <tr key={run.run_id || `${run.company_name}-${run.started_at}`}>
                        <td>{run.run_id || "n/a"}</td>
                        <td>{run.user_name || run.user_id || "n/a"}</td>
                        <td>{run.company_name || run.company_id || "n/a"}</td>
                        <td><span className={statusPillClass(run.status)}>{run.status || "unknown"}</span></td>
                        <td>{formatDateTime(run.started_at)}</td>
                        <td>{formatDateTime(run.completed_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          ) : null}

          {activeNav === "versions" ? (
            <section className="admin-section">
              <h3>Data Version Tracking (Reruns Only)</h3>
              <div className="admin-table-wrap">
                <table className="admin-table">
                  <thead>
                    <tr>
                      <th>Version</th>
                      <th>User</th>
                      <th>Company</th>
                      <th>Kind</th>
                      <th>Run</th>
                      <th>Previous Run</th>
                      <th>Timestamp</th>
                    </tr>
                  </thead>
                  <tbody>
                    {versions.length === 0 ? (
                      <tr><td colSpan={7} className="admin-empty-cell">No reruns yet. Run a company pipeline again to see version history.</td></tr>
                    ) : versions.slice(0, 250).map((version) => (
                      <tr key={version.version_id || `${version.company_id}-${version.created_at}`}>
                        <td>{version.version_id || "n/a"}</td>
                        <td>{version.user_name || version.user_id || "n/a"}</td>
                        <td>{version.company_name || version.company_id || "n/a"}</td>
                        <td><span className={statusPillClass(version.version_kind)}>{version.version_kind || "snapshot"}</span></td>
                        <td>{version.run_id || "n/a"}</td>
                        <td>{version.previous_run_id || "n/a"}</td>
                        <td>{formatDateTime(version.created_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          ) : null}

          {activeNav === "activity" ? (
            <section className="admin-section">
              <h3>Audit Trail</h3>
              <div className="admin-table-wrap">
                <table className="admin-table">
                  <thead>
                    <tr>
                      <th>Timestamp</th>
                      <th>Actor</th>
                      <th>Scope</th>
                      <th>Activity</th>
                      <th>Status</th>
                      <th>Company</th>
                    </tr>
                  </thead>
                  <tbody>
                    {activity.length === 0 ? (
                      <tr><td colSpan={6} className="admin-empty-cell">No activity logs recorded.</td></tr>
                    ) : activity.slice(0, 300).map((entry) => (
                      <tr key={entry.activity_id || `${entry.created_at}-${entry.activity_type}`}>
                        <td>{formatDateTime(entry.created_at)}</td>
                        <td>{entry.actor_user_id || "system"}</td>
                        <td>{entry.scope || "n/a"}</td>
                        <td>{entry.activity_type || "n/a"}</td>
                        <td><span className={statusPillClass(entry.activity_status)}>{entry.activity_status || "n/a"}</span></td>
                        <td>{entry.company_name || entry.company_id || "n/a"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          ) : null}

          {activeNav === "errors" ? (
            <section className="admin-section">
              <h3>Error Logging and Monitoring</h3>
              <div className="admin-table-wrap">
                <table className="admin-table">
                  <thead>
                    <tr>
                      <th>Timestamp</th>
                      <th>Type</th>
                      <th>Message</th>
                      <th>User</th>
                      <th>Source</th>
                    </tr>
                  </thead>
                  <tbody>
                    {errors.length === 0 ? (
                      <tr><td colSpan={5} className="admin-empty-cell">No errors logged.</td></tr>
                    ) : errors.slice(0, 300).map((entry) => (
                      <tr key={entry.error_id || `${entry.created_at}-${entry.message}`}>
                        <td>{formatDateTime(entry.created_at)}</td>
                        <td><span className={statusPillClass(entry.error_type)}>{entry.error_type || "runtime"}</span></td>
                        <td className="admin-long">{entry.message || "n/a"}</td>
                        <td>{entry.user_id || "n/a"}</td>
                        <td>{entry.source || "server"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          ) : null}
        </main>
      </div>
    </div>
  );
}

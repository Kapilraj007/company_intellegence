import { useCallback, useEffect, useMemo, useState } from "react";
import { T } from "../theme";
import { Badge, Btn, Card } from "../components";
import { API_BASE, approveUser, getOutputs, getPendingUsers, getTasks, rejectUser } from "../api";
import { formatDateTime } from "../utils";

function MiniStat({ label, value, color = T.amber }) {
  return (
    <div style={{ background: T.navy3, borderRadius: 10, padding: "14px 16px" }}>
      <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.7px" }}>{label}</div>
      <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 26, color, marginTop: 6 }}>
        {value}
      </div>
    </div>
  );
}

export default function ProfilePage({ user, apiInfo, onLogout }) {
  const [tasks, setTasks] = useState([]);
  const [outputs, setOutputs] = useState([]);
  const [pendingUsers, setPendingUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [approvalBusyId, setApprovalBusyId] = useState("");
  const [error, setError] = useState("");
  const [adminError, setAdminError] = useState("");
  const isAdmin = String(user?.role || "").toLowerCase() === "admin";
  const roleLabel = isAdmin ? "Admin" : "User";

  const refresh = useCallback(async () => {
    const [tasksResult, outputsResult, pendingUsersResult] = await Promise.allSettled([
      getTasks(),
      getOutputs(120),
      isAdmin ? getPendingUsers() : Promise.resolve({ users: [] }),
    ]);

    if (tasksResult.status === "fulfilled" && Array.isArray(tasksResult.value)) {
      setTasks(tasksResult.value);
    }
    if (outputsResult.status === "fulfilled" && Array.isArray(outputsResult.value)) {
      setOutputs(outputsResult.value);
    }
    if (pendingUsersResult.status === "fulfilled") {
      setPendingUsers(Array.isArray(pendingUsersResult.value?.users) ? pendingUsersResult.value.users : []);
      setAdminError("");
    } else if (isAdmin) {
      setPendingUsers([]);
      setAdminError(pendingUsersResult.reason?.message || "Unable to load approval requests.");
    } else {
      setPendingUsers([]);
      setAdminError("");
    }

    if (tasksResult.status === "rejected" && outputsResult.status === "rejected") {
      setError(`Backend data unavailable: ${tasksResult.reason?.message || outputsResult.reason?.message || "Unknown error"}`);
    } else {
      setError("");
    }

    setLoading(false);
  }, [isAdmin]);

  useEffect(() => {
    const timer = setTimeout(() => {
      refresh();
    }, 0);

    return () => clearTimeout(timer);
  }, [refresh]);

  const completedTasks = useMemo(() => tasks.filter((task) => task.status === "done"), [tasks]);
  const activeTasks = useMemo(() => tasks.filter((task) => task.status === "running" || task.status === "pending"), [tasks]);
  const distinctCompanies = useMemo(
    () => new Set(outputs.map((item) => item.company).filter(Boolean)).size,
    [outputs]
  );
  const lastOutput = outputs[0] || null;

  const handleApprovalAction = async (targetUserId, action) => {
    setApprovalBusyId(targetUserId);
    setAdminError("");
    try {
      if (action === "approve") {
        await approveUser(targetUserId);
      } else {
        await rejectUser(targetUserId);
      }
      await refresh();
    } catch (requestError) {
      setAdminError(requestError?.message || "Approval update failed.");
    } finally {
      setApprovalBusyId("");
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24, maxWidth: 980 }}>
      <style>{`
        .profile-grid {
          display: grid;
          grid-template-columns: minmax(0, 1.1fr) minmax(280px, 0.9fr);
          gap: 18px;
        }
        @media (max-width: 900px) {
          .profile-grid {
            grid-template-columns: 1fr;
          }
        }
      `}</style>

      <div style={{ display: "flex", justifyContent: "space-between", gap: 16, flexWrap: "wrap", alignItems: "flex-start" }}>
        <div>
          <h1 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 28, color: T.white }}>
            Workspace Profile
          </h1>
          <p style={{ color: T.muted, fontSize: 13, marginTop: 6 }}>
            Operator identity, backend status, and a quick snapshot of current research activity.
          </p>
        </div>
        <Btn variant="ghost" onClick={refresh} style={{ padding: "8px 16px", fontSize: 12 }}>
          Refresh
        </Btn>
      </div>

      <Card style={{ padding: 28, display: "flex", alignItems: "center", gap: 22, flexWrap: "wrap" }}>
        <div style={{
          width: 76,
          height: 76,
          borderRadius: 18,
          background: `linear-gradient(135deg, ${T.amber}30, ${T.cyan}18)`,
          border: `1px solid ${T.amber}40`,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          fontFamily: "'Syne', sans-serif",
          fontWeight: 800,
          fontSize: 30,
          color: T.amber,
        }}>
          {user.name[0].toUpperCase()}
        </div>

        <div style={{ flex: 1, minWidth: 240 }}>
          <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 22, color: T.white }}>{user.name}</div>
          <div style={{ color: T.muted, fontSize: 13, marginTop: 4 }}>{user.email}</div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 12 }}>
            <Badge>{roleLabel}</Badge>
            <Badge color={apiInfo.online ? T.green : T.red}>
              {apiInfo.online ? "API Connected" : "API Offline"}
            </Badge>
            <Badge color={activeTasks.length ? T.amber : T.cyan}>
              {activeTasks.length} live task{activeTasks.length === 1 ? "" : "s"}
            </Badge>
          </div>
        </div>

        <Btn variant="danger" onClick={onLogout}>
          Sign Out
        </Btn>
      </Card>

      {isAdmin && (
        <Card style={{ padding: 24 }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 16, flexWrap: "wrap", alignItems: "center" }}>
            <div>
              <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 700, fontSize: 16, color: T.white }}>
                Access Requests
              </h2>
              <p style={{ color: T.muted, fontSize: 13, marginTop: 6 }}>
                Review pending signup approvals before granting workspace access.
              </p>
            </div>
            <Badge color={pendingUsers.length ? T.amber : T.cyan}>
              {pendingUsers.length} pending
            </Badge>
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: 12, marginTop: 18 }}>
            {pendingUsers.length === 0 ? (
              <div style={{ color: T.muted, fontSize: 13 }}>
                No pending approval requests right now.
              </div>
            ) : (
              pendingUsers.map((pendingUser) => (
                <div key={pendingUser.user_id} style={{ background: T.navy3, borderRadius: 12, padding: "14px 16px", border: `1px solid ${T.border}` }}>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 16, flexWrap: "wrap", alignItems: "center" }}>
                    <div>
                      <div style={{ color: T.white, fontWeight: 700, fontSize: 14, fontFamily: "'Syne', sans-serif" }}>
                        {pendingUser.name}
                      </div>
                      <div style={{ color: T.muted, fontSize: 12, marginTop: 4 }}>{pendingUser.email}</div>
                      <div style={{ color: T.muted, fontSize: 11, marginTop: 6, fontFamily: "'JetBrains Mono', monospace" }}>
                        Requested {formatDateTime(pendingUser.created_at)}
                      </div>
                    </div>

                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                      <Btn
                        onClick={() => handleApprovalAction(pendingUser.user_id, "approve")}
                        disabled={approvalBusyId === pendingUser.user_id}
                        style={{ padding: "8px 14px", fontSize: 12 }}
                      >
                        Approve
                      </Btn>
                      <Btn
                        variant="danger"
                        onClick={() => handleApprovalAction(pendingUser.user_id, "reject")}
                        disabled={approvalBusyId === pendingUser.user_id}
                        style={{ padding: "8px 14px", fontSize: 12 }}
                      >
                        Reject
                      </Btn>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>

          {adminError && (
            <div style={{ marginTop: 14, color: T.amber, fontSize: 12, fontFamily: "'JetBrains Mono', monospace" }}>
              {adminError}
            </div>
          )}
        </Card>
      )}

      <Card style={{ padding: 24 }}>
        <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 700, fontSize: 16, color: T.white, marginBottom: 16 }}>
          Research Activity
        </h2>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 12 }}>
          <MiniStat label="Live Tasks" value={activeTasks.length} color={activeTasks.length ? T.amber : T.cyan}/>
          <MiniStat label="Completed Tasks" value={completedTasks.length} color={T.green}/>
          <MiniStat label="Stored Runs" value={outputs.length} color={T.amber}/>
          <MiniStat label="Companies" value={distinctCompanies} color={T.white}/>
        </div>
      </Card>

      <div className="profile-grid">
        <Card style={{ padding: 24 }}>
          <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 700, fontSize: 16, color: T.white, marginBottom: 16 }}>
            Backend Overview
          </h2>
          <div style={{ display: "grid", gap: 10 }}>
            {[
              ["API Base", API_BASE],
              ["Tasks In Memory", apiInfo.tasks_in_memory],
              ["Latest Stored Run", lastOutput ? `${lastOutput.company} · ${formatDateTime(lastOutput.created_at)}` : "—"],
              ["Workspace Mode", loading ? "Loading..." : "Backend-driven UI"],
            ].map(([label, value]) => (
              <div key={label} style={{ background: T.navy3, borderRadius: 10, padding: "12px 14px" }}>
                <div style={{ fontSize: 11, color: T.muted, marginBottom: 6 }}>{label}</div>
                <div style={{ color: T.white, fontSize: 13, lineHeight: 1.6, wordBreak: "break-word", fontFamily: label === "API Base" ? "'JetBrains Mono', monospace" : "inherit" }}>
                  {value}
                </div>
              </div>
            ))}
          </div>
        </Card>

        <Card style={{ padding: 24 }}>
          <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 700, fontSize: 16, color: T.white, marginBottom: 16 }}>
            Recent Companies
          </h2>
          {outputs.length === 0 ? (
            <div style={{ color: T.muted, fontSize: 13 }}>
              No stored outputs yet.
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              {outputs.slice(0, 5).map((item) => (
                <div key={item.id} style={{ background: T.navy3, borderRadius: 10, padding: "12px 14px" }}>
                  <div style={{ color: T.white, fontWeight: 700, fontSize: 14, fontFamily: "'Syne', sans-serif" }}>
                    {item.company}
                  </div>
                  <div style={{ color: T.muted, fontSize: 11, marginTop: 6, fontFamily: "'JetBrains Mono', monospace" }}>
                    {formatDateTime(item.created_at)}
                  </div>
                  <div style={{ color: (item.failed || 0) > 0 ? T.red : T.green, fontSize: 11, marginTop: 8 }}>
                    {typeof item.failed === "number"
                      ? item.failed > 0
                        ? `${item.failed} failed test${item.failed === 1 ? "" : "s"}`
                        : "All tests passed"
                      : "No pytest report"}
                  </div>
                </div>
              ))}
            </div>
          )}
        </Card>
      </div>

      {error && (
        <Card style={{ padding: 16, borderColor: `${T.amber}35` }}>
          <div style={{ color: T.amber, fontSize: 12, fontFamily: "'JetBrains Mono', monospace", lineHeight: 1.6 }}>
            {error}
          </div>
        </Card>
      )}
    </div>
  );
}

import { startTransition, useCallback, useEffect, useMemo, useState } from "react";
import { T } from "../theme";
import { Badge, Btn, Card, Dot, StatusPill } from "../components";
import { getOutputs, getTasks, submitJob } from "../api";
import { formatDateTime, outputIdFromPath, truncateId } from "../utils";

const COMPANY_HINTS = ["NVIDIA", "Apple", "Infosys", "TCS", "OpenAI", "Tesla"];
const LIVE_TASK_STATUSES = new Set(["pending", "running"]);

function normalizeCompanyName(value) {
  return String(value || "").trim().replace(/\s+/g, " ").toLowerCase();
}

function MetricCard({ label, value, sub, color = T.amber }) {
  return (
    <Card style={{ padding: "18px 20px", minHeight: 110 }}>
      <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 10 }}>
        {label}
      </div>
      <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 30, color }}>
        {value}
      </div>
      <div style={{ fontSize: 12, color: T.muted, marginTop: 6, lineHeight: 1.5 }}>
        {sub}
      </div>
    </Card>
  );
}

export default function DashboardPage({ setPage, setActivePipeline, setActiveOutput, apiInfo }) {
  const [company, setCompany] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [tasks, setTasks] = useState([]);
  const [outputs, setOutputs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [rerunPrompt, setRerunPrompt] = useState(null);

  const refreshData = useCallback(async () => {
    const [tasksResult, outputsResult] = await Promise.allSettled([getTasks(), getOutputs(120)]);

    startTransition(() => {
      if (tasksResult.status === "fulfilled" && Array.isArray(tasksResult.value)) {
        setTasks(tasksResult.value);
      }

      if (outputsResult.status === "fulfilled" && Array.isArray(outputsResult.value)) {
        setOutputs(outputsResult.value);
      }

      if (tasksResult.status === "rejected" && outputsResult.status === "rejected") {
        setError(`Could not load backend data: ${tasksResult.reason?.message || outputsResult.reason?.message || "Unknown error"}`);
      } else {
        setError("");
      }

      setLoading(false);
    });
  }, []);

  useEffect(() => {
    refreshData();
    const timer = setInterval(refreshData, 4000);
    return () => clearInterval(timer);
  }, [refreshData]);

  const liveTasks = useMemo(
    () => tasks.filter((task) => task.status === "pending" || task.status === "running"),
    [tasks]
  );
  const doneTasks = useMemo(() => tasks.filter((task) => task.status === "done"), [tasks]);
  const healthyOutputs = useMemo(
    () => outputs.filter((item) => typeof item.failed === "number" && item.failed === 0),
    [outputs]
  );
  const avgFields = useMemo(() => {
    const withFields = outputs.filter((item) => typeof item.fields === "number" && item.fields > 0);
    if (!withFields.length) return "—";
    return Math.round(withFields.reduce((sum, item) => sum + item.fields, 0) / withFields.length);
  }, [outputs]);
  const trackedCompanies = useMemo(
    () => new Set(outputs.map((item) => item.company).filter(Boolean)).size,
    [outputs]
  );
  const passRate = outputs.length ? `${Math.round((healthyOutputs.length / outputs.length) * 100)}%` : "—";

  const stats = [
    {
      label: "Active Queue",
      value: liveTasks.length,
      sub: apiInfo.online
        ? `${apiInfo.tasks_in_memory} task${apiInfo.tasks_in_memory === 1 ? "" : "s"} currently held by the API`
        : "Backend offline",
      color: liveTasks.length ? T.amber : T.cyan,
    },
    {
      label: "Stored Runs",
      value: outputs.length,
      sub: "Historical outputs discovered in output/",
      color: T.amber,
    },
    {
      label: "Average Fields",
      value: avgFields,
      sub: "Golden-record coverage per completed output",
      color: T.cyan,
    },
    {
      label: "Test Pass Rate",
      value: passRate,
      sub: "Runs with zero pytest failures",
      color: healthyOutputs.length === outputs.length && outputs.length ? T.green : T.amber,
    },
    {
      label: "Companies",
      value: trackedCompanies,
      sub: "Distinct companies already researched",
      color: T.green,
    },
    {
      label: "Completed Live Tasks",
      value: doneTasks.length,
      sub: "Runs completed since this API process started",
      color: T.white,
    },
  ];

  const findExistingRunMeta = useCallback((companyName) => {
    const normalizedName = normalizeCompanyName(companyName);
    if (!normalizedName) return null;

    const matchingOutputs = outputs.filter(
      (item) => normalizeCompanyName(item.company) === normalizedName
    );
    const matchingTasks = tasks.filter(
      (task) => normalizeCompanyName(task.company) === normalizedName
    );
    const liveTaskMatches = matchingTasks.filter((task) => LIVE_TASK_STATUSES.has(task.status));

    if (!matchingOutputs.length && !matchingTasks.length) {
      return null;
    }

    const latestOutputAt = matchingOutputs.reduce((latest, item) => {
      const timestamp = Date.parse(item.created_at || "");
      if (Number.isNaN(timestamp)) return latest;
      return Math.max(latest, timestamp);
    }, 0);

    const latestTaskAt = matchingTasks.reduce((latest, task) => {
      const timestamp = Date.parse(task.created_at || "");
      if (Number.isNaN(timestamp)) return latest;
      return Math.max(latest, timestamp);
    }, 0);

    return {
      company: companyName.trim(),
      outputCount: matchingOutputs.length,
      taskCount: matchingTasks.length,
      liveCount: liveTaskMatches.length,
      latestOutputAt: latestOutputAt ? new Date(latestOutputAt).toISOString() : null,
      latestTaskAt: latestTaskAt ? new Date(latestTaskAt).toISOString() : null,
    };
  }, [outputs, tasks]);

  const companyCacheSignal = useMemo(() => {
    const name = company.trim();
    if (!name) return null;
    return findExistingRunMeta(name);
  }, [company, findExistingRunMeta]);

  const handleSubmit = useCallback(async ({ force = false, companyOverride = "" } = {}) => {
    const name = (companyOverride || company).trim();
    if (!name) {
      setError("Please enter a company name.");
      return;
    }

    if (!force) {
      const existingRunMeta = findExistingRunMeta(name);
      if (existingRunMeta) {
        setRerunPrompt(existingRunMeta);
        setError("");
        return;
      }
    }

    setRerunPrompt(null);
    setSubmitting(true);
    setError("");
    try {
      const data = await submitJob(name);
      setCompany("");
      setActivePipeline(data.task_id);
      setTimeout(refreshData, 500);
    } catch (requestError) {
      setError(`Failed to start pipeline: ${requestError.message}`);
    } finally {
      setSubmitting(false);
    }
  }, [company, findExistingRunMeta, refreshData, setActivePipeline]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <style>{`
        .dashboard-hero {
          display: grid;
          grid-template-columns: minmax(0, 1.55fr) minmax(300px, 0.95fr);
          gap: 18px;
        }
        .dashboard-grid {
          display: grid;
          grid-template-columns: minmax(0, 1.2fr) minmax(320px, 0.8fr);
          gap: 18px;
          align-items: start;
        }
        @media (max-width: 1120px) {
          .dashboard-hero,
          .dashboard-grid {
            grid-template-columns: 1fr;
          }
        }
      `}</style>

      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 16, flexWrap: "wrap" }}>
        <div>
          <h1 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 28, color: T.white }}>
            Research Command Center
          </h1>
          <p style={{ color: T.muted, fontSize: 13, marginTop: 6 }}>
            Run new company jobs, monitor the live queue, and jump into stored outputs.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <Badge color={apiInfo.online ? T.green : T.red}>
            <Dot color={apiInfo.online ? T.green : T.red} pulse={apiInfo.online}/>
            {apiInfo.online ? "API Online" : "API Offline"}
          </Badge>
          <Badge color={liveTasks.length ? T.amber : T.cyan}>
            {liveTasks.length} live run{liveTasks.length === 1 ? "" : "s"}
          </Badge>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 14 }}>
        {stats.map((item) => (
          <MetricCard key={item.label} {...item} />
        ))}
      </div>

      <div className="dashboard-hero">
        <Card style={{ padding: 24, position: "relative", overflow: "hidden" }}>
          <div style={{
            position: "absolute",
            inset: 0,
            background: `radial-gradient(circle at top right, ${T.amber}14, transparent 32%)`,
            pointerEvents: "none",
          }}/>
          <div style={{ position: "relative" }}>
            <Badge color={T.cyan}>New Research Run</Badge>
            <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 24, color: T.white, marginTop: 14 }}>
              Launch the pipeline from one place
            </h2>
            <p style={{ color: T.muted, fontSize: 13, lineHeight: 1.7, marginTop: 8, maxWidth: 680 }}>
              The backend already supports live task streams, output files, and semantic search. This panel is now the entry point into all of that.
            </p>

            <div style={{ marginTop: 20 }}>
              <label style={{ fontSize: 12, fontWeight: 500, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px" }}>
                Company to research
              </label>
              <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginTop: 8 }}>
                <input
                  type="text"
                  value={company}
                  onChange={(event) => {
                    setCompany(event.target.value);
                    setRerunPrompt(null);
                    setError("");
                  }}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") handleSubmit();
                  }}
                  placeholder="e.g. NVIDIA, Apple, Infosys, TCS"
                  style={{
                    flex: 1,
                    minWidth: 240,
                    background: T.navy3,
                    border: `1px solid ${T.border}`,
                    borderRadius: 10,
                    color: T.white,
                    padding: "14px 16px",
                    fontSize: 15,
                    fontFamily: "'Syne', sans-serif",
                    fontWeight: 600,
                    outline: "none",
                  }}
                />
                <Btn
                  onClick={handleSubmit}
                  disabled={!company.trim() || !apiInfo.online || submitting}
                  style={{ minWidth: 170, padding: "14px 24px", fontSize: 14 }}
                >
                  {submitting ? "Submitting..." : companyCacheSignal ? "Run Fresh Version" : "Run Pipeline"}
                </Btn>
              </div>
            </div>

            <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginTop: 14 }}>
              {COMPANY_HINTS.map((name) => (
                <button
                  key={name}
                  onClick={() => {
                    setCompany(name);
                    setRerunPrompt(null);
                    setError("");
                  }}
                  style={{
                    background: T.navy3,
                    border: `1px solid ${T.border}`,
                    color: T.text,
                    borderRadius: 999,
                    padding: "8px 12px",
                    cursor: "pointer",
                    fontSize: 12,
                    fontFamily: "'JetBrains Mono', monospace",
                  }}
                >
                  {name}
                </button>
              ))}
            </div>

            {companyCacheSignal && (
              <div style={{ marginTop: 14, padding: "10px 12px", borderRadius: 10, background: T.cyan + "14", border: `1px solid ${T.cyan}35`, color: T.cyan, fontSize: 12, lineHeight: 1.6 }}>
                {companyCacheSignal.outputCount} stored run{companyCacheSignal.outputCount === 1 ? "" : "s"} found for this company.
                {companyCacheSignal.latestOutputAt ? ` Latest output: ${formatDateTime(companyCacheSignal.latestOutputAt)}.` : ""}
                {companyCacheSignal.liveCount > 0 ? ` ${companyCacheSignal.liveCount} live run${companyCacheSignal.liveCount === 1 ? "" : "s"} currently in queue.` : ""}
              </div>
            )}

            {rerunPrompt && (
              <div style={{ marginTop: 14, padding: "14px", borderRadius: 10, background: T.amber + "12", border: `1px solid ${T.amber}45`, color: T.amber }}>
                <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 15, fontWeight: 700 }}>
                  Existing data found for "{rerunPrompt.company}".
                </div>
                <div style={{ marginTop: 6, fontSize: 12, color: T.text, lineHeight: 1.6 }}>
                  This company already has {rerunPrompt.outputCount} stored run{rerunPrompt.outputCount === 1 ? "" : "s"} and {rerunPrompt.liveCount} active queue item{rerunPrompt.liveCount === 1 ? "" : "s"}.
                  {rerunPrompt.latestOutputAt ? ` Latest output: ${formatDateTime(rerunPrompt.latestOutputAt)}.` : ""}
                  Start a new run anyway?
                </div>
                <div style={{ marginTop: 12, display: "flex", gap: 8, flexWrap: "wrap" }}>
                  <Btn
                    variant="ghost"
                    onClick={() => setRerunPrompt(null)}
                    disabled={submitting}
                    style={{ padding: "8px 14px", fontSize: 12 }}
                  >
                    Cancel
                  </Btn>
                  <Btn
                    onClick={() => handleSubmit({ force: true, companyOverride: rerunPrompt.company })}
                    disabled={submitting}
                    style={{ padding: "8px 14px", fontSize: 12 }}
                  >
                    {submitting ? "Starting..." : "Run New Version"}
                  </Btn>
                </div>
              </div>
            )}

            {error && (
              <div style={{ marginTop: 14, padding: "12px 14px", background: T.red + "12", border: `1px solid ${T.red}35`, borderRadius: 10, color: T.red, fontSize: 13 }}>
                {error}
              </div>
            )}

            {!apiInfo.online && (
              <div style={{ marginTop: 14, padding: "12px 14px", background: T.amber + "10", border: `1px solid ${T.amber}30`, borderRadius: 10, color: T.amber, fontSize: 13 }}>
                Backend offline. Run <code>uvicorn server:app --port 8000 --reload</code> to restore jobs, search, and output browsing.
              </div>
            )}
          </div>
        </Card>

        <Card style={{ padding: 24 }}>
          <Badge color={T.amber}>Workflow Shortcuts</Badge>
          <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 20, color: T.white, marginTop: 14 }}>
            Move from job launch to analysis faster
          </h2>
          <div style={{ display: "flex", flexDirection: "column", gap: 12, marginTop: 18 }}>
            <Card style={{ padding: 16, background: T.navy3 }}>
              <div style={{ fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.7px" }}>Search Workspace</div>
              <div style={{ color: T.white, fontSize: 15, fontWeight: 600, marginTop: 6 }}>Search by capability or benchmark company</div>
              <div style={{ color: T.muted, fontSize: 12, marginTop: 6, lineHeight: 1.6 }}>
                Uses both <code>/search-companies</code> and <code>/search/similar</code>, then opens a dedicated detail page for each result.
              </div>
              <Btn variant="ghost" onClick={() => setPage("explorer")} style={{ marginTop: 12, padding: "9px 14px", fontSize: 12 }}>
                Open Search
              </Btn>
            </Card>

            <Card style={{ padding: 16, background: T.navy3 }}>
              <div style={{ fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.7px" }}>Output Library</div>
              <div style={{ color: T.white, fontSize: 15, fontWeight: 600, marginTop: 6 }}>Inspect golden records, validation, and pytest reports</div>
              <div style={{ color: T.muted, fontSize: 12, marginTop: 6, lineHeight: 1.6 }}>
                Reads grouped JSON artifacts directly from <code>output/</code> via <code>/outputs</code> and <code>/file</code>.
              </div>
              <Btn variant="ghost" onClick={() => setPage("outputs")} style={{ marginTop: 12, padding: "9px 14px", fontSize: 12 }}>
                Browse Outputs
              </Btn>
            </Card>

            <Card style={{ padding: 16, background: T.navy3 }}>
              <div style={{ fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.7px" }}>Live Queue</div>
              <div style={{ color: T.white, fontSize: 15, fontWeight: 600, marginTop: 6 }}>Watch task events in the pipeline visualizer</div>
              <div style={{ color: T.muted, fontSize: 12, marginTop: 6, lineHeight: 1.6 }}>
                Active tasks stream directly from <code>/events</code> and <code>/status</code> while the run is executing.
              </div>
              <Btn variant="ghost" onClick={() => setPage("pipeline")} style={{ marginTop: 12, padding: "9px 14px", fontSize: 12 }}>
                Open Visualizer
              </Btn>
            </Card>
          </div>
        </Card>
      </div>

      <div className="dashboard-grid">
        <Card style={{ padding: 0, overflow: "hidden" }}>
          <div style={{
            padding: "18px 22px",
            borderBottom: `1px solid ${T.border}`,
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: 10,
            flexWrap: "wrap",
          }}>
            <div>
              <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
                Live Queue
              </h2>
              <div style={{ color: T.muted, fontSize: 12, marginTop: 4 }}>
                Active and recent in-memory tasks from the running FastAPI process
              </div>
            </div>
            <Btn variant="ghost" onClick={refreshData} style={{ padding: "8px 14px", fontSize: 12 }}>
              Refresh
            </Btn>
          </div>

          {loading ? (
            <div style={{ padding: "34px 22px", color: T.muted, fontSize: 13 }}>Loading queue...</div>
          ) : tasks.length === 0 ? (
            <div style={{ padding: "34px 22px", color: T.muted, fontSize: 13 }}>
              No in-memory tasks yet. Submit a pipeline run to start streaming task events.
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column" }}>
              {tasks.slice(0, 8).map((task) => (
                <button
                  key={task.task_id}
                  onClick={() => setActivePipeline(task.task_id)}
                  style={{
                    display: "grid",
                    gridTemplateColumns: "minmax(0, 1fr) auto auto",
                    gap: 16,
                    padding: "16px 22px",
                    background: "transparent",
                    border: "none",
                    borderTop: `1px solid ${T.border}`,
                    textAlign: "left",
                    cursor: "pointer",
                    color: T.text,
                  }}
                >
                  <div style={{ minWidth: 0 }}>
                    <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 700, color: T.white, fontSize: 15 }}>
                      {task.company || "Unknown company"}
                    </div>
                    <div style={{ fontSize: 11, color: T.muted, marginTop: 6, fontFamily: "'JetBrains Mono', monospace" }}>
                      {truncateId(task.task_id)} · {formatDateTime(task.created_at)}
                    </div>
                  </div>
                  <div style={{ display: "flex", alignItems: "center" }}>
                    <StatusPill status={task.status}/>
                  </div>
                  <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", justifyContent: "center", gap: 4 }}>
                    <div style={{ color: typeof task.fields === "number" && task.fields > 0 ? T.amber : T.muted, fontSize: 12, fontFamily: "'JetBrains Mono', monospace" }}>
                      {typeof task.fields === "number" && task.fields > 0 ? `${task.fields} fields` : "No result yet"}
                    </div>
                    <div style={{ color: T.muted, fontSize: 11 }}>
                      {typeof task.failed === "number" && typeof task.passed === "number"
                        ? `${task.passed} passed / ${task.failed} failed`
                        : "Awaiting tests"}
                    </div>
                  </div>
                </button>
              ))}
            </div>
          )}
        </Card>

        <Card style={{ padding: 0, overflow: "hidden" }}>
          <div style={{ padding: "18px 22px", borderBottom: `1px solid ${T.border}` }}>
            <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
              Latest Output Library
            </h2>
            <div style={{ color: T.muted, fontSize: 12, marginTop: 4 }}>
              Disk-backed runs grouped from the backend output folder
            </div>
          </div>

          {loading ? (
            <div style={{ padding: "34px 22px", color: T.muted, fontSize: 13 }}>Loading outputs...</div>
          ) : outputs.length === 0 ? (
            <div style={{ padding: "34px 22px", color: T.muted, fontSize: 13 }}>
              No stored outputs yet. Completed runs will appear here automatically.
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column" }}>
              {outputs.slice(0, 6).map((item) => {
                const outputId = item.id || outputIdFromPath(item.golden_record_path);
                return (
                  <button
                    key={item.id}
                    onClick={() => outputId && setActiveOutput(outputId)}
                    style={{
                      padding: "16px 22px",
                      textAlign: "left",
                      background: "transparent",
                      border: "none",
                      borderTop: `1px solid ${T.border}`,
                      cursor: outputId ? "pointer" : "default",
                      color: T.text,
                    }}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 14 }}>
                      <div>
                        <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 700, color: T.white, fontSize: 15 }}>
                          {item.company}
                        </div>
                        <div style={{ fontSize: 11, color: T.muted, marginTop: 6, fontFamily: "'JetBrains Mono', monospace" }}>
                          {formatDateTime(item.created_at)}
                        </div>
                      </div>
                      <div style={{ textAlign: "right" }}>
                        <div style={{ color: typeof item.fields === "number" ? T.amber : T.muted, fontSize: 12 }}>
                          {typeof item.fields === "number" ? `${item.fields} fields` : "Field count unavailable"}
                        </div>
                        <div style={{ color: (item.failed || 0) > 0 ? T.red : T.green, fontSize: 11, marginTop: 6 }}>
                          {typeof item.failed === "number"
                            ? item.failed > 0
                              ? `${item.failed} failed`
                              : "All tests passed"
                            : "No pytest report"}
                        </div>
                      </div>
                    </div>
                  </button>
                );
              })}
            </div>
          )}
        </Card>

      </div>
    </div>
  );
}

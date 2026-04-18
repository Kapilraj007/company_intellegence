import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { T } from "../theme";
import { Badge, Btn, Card, Dot, StatusPill } from "../components";
import { API_BASE, getResult, getStatus, getTaskEvents, getTasks } from "../api";
import { formatDateTime, formatTime, outputIdFromPath, truncateId } from "../utils";

const NODES = [
  { id: "llm1", label: "LLM 1", sub: "Primary research", x: 80, y: 80 },
  { id: "llm2", label: "LLM 2", sub: "Secondary research", x: 80, y: 210 },
  { id: "llm3", label: "LLM 3", sub: "Tertiary research", x: 80, y: 340 },
  { id: "combine", label: "Combine", sub: "Merge candidate fields", x: 320, y: 210 },
  { id: "consolidate", label: "Consolidate", sub: "Golden-record build", x: 540, y: 210 },
  { id: "save", label: "Save", sub: "Persist artifacts", x: 760, y: 210 },
  { id: "run_tests", label: "Tests", sub: "Validation and pytest", x: 980, y: 150 },
  { id: "retry", label: "Retry", sub: "Targeted repair loop", x: 980, y: 320 },
  { id: "end", label: "End", sub: "Pipeline complete", x: 1170, y: 235 },
];

const EDGES = [
  { from: "llm1", to: "combine" },
  { from: "llm2", to: "combine" },
  { from: "llm3", to: "combine" },
  { from: "combine", to: "consolidate" },
  { from: "consolidate", to: "save" },
  { from: "save", to: "run_tests" },
  { from: "run_tests", to: "end" },
  { from: "run_tests", to: "retry", optional: true },
  { from: "retry", to: "llm1", optional: true },
];

const STATUS_COLOR = { idle: T.muted, pending: T.muted, running: T.amber, done: T.green, failed: T.red };
const NODE_ORDER = NODES.map((node) => node.id);

function detectNodeFromText(text) {
  if (text.startsWith("llm1 ") || text.includes("[llm1]") || text.includes("llama-3.3-70b")) return "llm1";
  if (text.startsWith("llm2 ") || text.includes("[llm2]") || text.includes("llama-3.1-70b")) return "llm2";
  if (text.startsWith("llm3 ") || text.includes("[llm3]") || text.includes("llama-3.1-8b")) return "llm3";
  if (text.includes("[combine]") || text.includes("combine")) return "combine";
  if (text.includes("agent2") || text.includes("consolidat")) return "consolidate";
  if (text.includes("agent3") || text.includes("golden record") || text.includes("validation report")) return "save";
  if (text.includes("agent4") || text.includes("pytest")) return "run_tests";
  if (text.includes("retry") || text.includes("regen")) return "retry";
  if (text.includes("pipeline complete") || text.includes("→ end") || text.includes("router returned end")) return "end";
  return null;
}

function shouldMarkDone(nodeId, text) {
  if (nodeId === "llm1" || nodeId === "llm2" || nodeId === "llm3") {
    return text.includes("total:") || text.includes("→ pass");
  }
  if (nodeId === "combine") return text.includes("[combine]");
  if (nodeId === "consolidate") return text.includes("golden record:") || text.includes("consolidation → pass");
  if (nodeId === "save") return text.includes("saved:") || (text.includes("golden record") && text.includes("saved"));
  if (nodeId === "run_tests") {
    return text.includes("tests:") || text.includes("pytest exit code") || text.includes("all tests passed");
  }
  if (nodeId === "retry") return text.includes("retrying ids") || text.includes("round");
  if (nodeId === "end") return text.includes("pipeline complete") || text.includes("→ end");
  return false;
}

function deriveNodeStates(taskStatus, logs) {
  const states = Object.fromEntries(NODE_ORDER.map((id) => [id, "idle"]));
  let runningNode = null;

  const markRunning = (nodeId) => {
    if (!nodeId) return;
    if (runningNode && runningNode !== nodeId && states[runningNode] === "running") {
      states[runningNode] = "done";
    }
    if (states[nodeId] !== "done") states[nodeId] = "running";
    runningNode = nodeId;
  };

  const markDone = (nodeId) => {
    if (!nodeId) return;
    states[nodeId] = "done";
    if (runningNode === nodeId) runningNode = null;
  };

  const markFailed = (nodeId) => {
    if (!nodeId) return;
    states[nodeId] = "failed";
    if (runningNode === nodeId) runningNode = null;
  };

  for (const log of logs) {
    const text = `${log.src || ""} ${log.msg || ""}`.toLowerCase();
    const nodeId = detectNodeFromText(text);
    if (!nodeId) continue;

    const retrySignal = text.includes("retrying ids") || (text.includes("router") && text.includes("retry"));
    if (retrySignal) {
      markFailed("run_tests");
      markRunning("retry");
      continue;
    }

    markRunning(nodeId);

    const pytestFail =
      nodeId === "run_tests" &&
      (text.includes("→ retry") ||
        text.includes("❌") ||
        (text.includes("failed") &&
          !text.includes("0 failed") &&
          !text.includes("failed: 0") &&
          !text.includes("failed parameter ids: []")));

    if (pytestFail) {
      markFailed(nodeId);
    }
    if (shouldMarkDone(nodeId, text)) {
      markDone(nodeId);
    }

    if ((nodeId === "llm1" || nodeId === "llm2" || nodeId === "llm3") && states.retry === "running") {
      markDone("retry");
      markRunning(nodeId);
    }
  }

  if (taskStatus === "pending") return states;

  if (taskStatus === "running" && !Object.values(states).includes("running")) {
    const nextIdle = NODE_ORDER.find((id) => states[id] === "idle");
    if (nextIdle) states[nextIdle] = "running";
  }

  if (taskStatus === "done") {
    NODE_ORDER.forEach((id) => {
      if (id === "retry" && states.retry === "idle") return;
      states[id] = "done";
    });
  }

  if (taskStatus === "failed" && !Object.values(states).includes("failed")) {
    if (runningNode) {
      states[runningNode] = "failed";
    } else if (states.run_tests === "running" || states.run_tests === "done") {
      states.run_tests = "failed";
    } else {
      states.combine = "failed";
    }
  }

  return states;
}

function progressFrom(status, nodeStates) {
  if (status === "idle") return 0;
  if (status === "pending") return 5;
  if (status === "done") return 100;

  const done = NODE_ORDER.filter((id) => nodeStates[id] === "done").length;
  const running = NODE_ORDER.filter((id) => nodeStates[id] === "running").length;
  const unit = 100 / NODE_ORDER.length;
  let progress = Math.round(done * unit + (running ? unit * 0.5 : 0));

  if (status === "running") progress = Math.min(96, Math.max(10, progress));
  if (status === "failed") progress = Math.min(99, Math.max(14, progress));
  if (nodeStates.retry === "running") progress = Math.max(progress, 72);

  return progress;
}

function PipelineGraph({ nodeStates }) {
  const radius = 32;
  const retryVisible = ["running", "done", "failed"].includes(nodeStates.retry) || nodeStates.run_tests === "failed";

  const renderedEdges = EDGES
    .filter((edge) => !edge.optional || retryVisible)
    .map((edge) => {
      const from = NODES.find((node) => node.id === edge.from);
      const to = NODES.find((node) => node.id === edge.to);
      const fromState = nodeStates[edge.from] || "idle";
      const toState = nodeStates[edge.to] || "idle";
      const active = ["done", "running", "failed"].includes(fromState) || ["done", "running", "failed"].includes(toState);

      let d;
      if (edge.from === "retry" && edge.to === "llm1") {
        d = `M${from.x - radius},${from.y} C${from.x - 160},${from.y + 170} ${to.x + 150},${to.y + 170} ${to.x},${to.y + radius}`;
      } else if (Math.abs(from.y - to.y) < 2) {
        d = `M${from.x + radius},${from.y} L${to.x - radius},${to.y}`;
      } else {
        const middleX = (from.x + to.x) / 2;
        d = `M${from.x + radius},${from.y} C${middleX},${from.y} ${middleX},${to.y} ${to.x - radius},${to.y}`;
      }

      return { key: `${edge.from}-${edge.to}`, d, active, optional: !!edge.optional };
    });

  return (
    <svg viewBox="0 0 1260 440" preserveAspectRatio="xMidYMid meet" style={{ width: "100%", height: "auto", display: "block" }}>
      <defs>
        <marker id="pipeline-arr" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
          <path d="M0,0 L0,6 L6,3 z" fill={T.border}/>
        </marker>
        <marker id="pipeline-arr-active" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
          <path d="M0,0 L0,6 L6,3 z" fill={T.amber}/>
        </marker>
        <filter id="pipeline-glow">
          <feGaussianBlur stdDeviation="3" result="blur"/>
          <feMerge>
            <feMergeNode in="blur"/>
            <feMergeNode in="SourceGraphic"/>
          </feMerge>
        </filter>
      </defs>

      {renderedEdges.map((edge) => (
        <path
          key={`${edge.key}-base`}
          d={edge.d}
          fill="none"
          stroke={edge.optional ? T.border : `${T.cyan}66`}
          strokeWidth={edge.optional ? 1.2 : 1.4}
          strokeDasharray={edge.optional ? "5 4" : "none"}
          markerEnd={edge.optional ? "url(#pipeline-arr)" : "none"}
          opacity={0.95}
        />
      ))}

      {renderedEdges.filter((edge) => edge.active).map((edge) => (
        <path
          key={`${edge.key}-active`}
          d={edge.d}
          fill="none"
          stroke={T.amber}
          strokeWidth={2.3}
          markerEnd="url(#pipeline-arr-active)"
          filter="url(#pipeline-glow)"
        />
      ))}

      {NODES.map((node) => {
        const state = nodeStates[node.id] || "idle";
        const color = STATUS_COLOR[state];
        const isRunning = state === "running";

        return (
          <g key={node.id} transform={`translate(${node.x},${node.y})`}>
            {isRunning && (
              <circle
                r={radius + 10}
                fill="none"
                stroke={T.amber}
                strokeWidth="1"
                opacity="0.24"
                style={{ animation: "pulse 1.2s ease-in-out infinite" }}
              />
            )}
            <circle
              r={radius}
              fill={T.navy3}
              stroke={color}
              strokeWidth={state === "idle" ? 1 : 2.5}
              filter={state !== "idle" ? "url(#pipeline-glow)" : "none"}
            />
            <text textAnchor="middle" dominantBaseline="middle" fontSize={16} fill={color}>
              {state === "done" ? "✓" : state === "failed" ? "✕" : state === "running" ? "" : "○"}
            </text>
            {isRunning && (
              <circle
                r={16}
                fill="none"
                stroke={T.amber}
                strokeWidth="2.5"
                strokeDasharray="25 75"
                strokeLinecap="round"
                style={{ animation: "spin 0.8s linear infinite", transformOrigin: "0 0" }}
              />
            )}
            <text y={radius + 16} textAnchor="middle" fontSize={12} fontFamily="'Syne', sans-serif" fontWeight={700} fill={color}>
              {node.label}
            </text>
            <text y={radius + 30} textAnchor="middle" fontSize={10} fontFamily="'DM Sans', sans-serif" fill={T.muted}>
              {node.sub.length > 24 ? `${node.sub.slice(0, 22)}...` : node.sub}
            </text>
          </g>
        );
      })}
    </svg>
  );
}

function LogFeed({ logs }) {
  const ref = useRef(null);

  useEffect(() => {
    if (ref.current) ref.current.scrollTop = ref.current.scrollHeight;
  }, [logs]);

  return (
    <div
      ref={ref}
      style={{
        background: T.navy,
        border: `1px solid ${T.border}`,
        borderRadius: 10,
        height: 280,
        overflowY: "auto",
        padding: "14px 16px",
        fontFamily: "'JetBrains Mono', monospace",
        fontSize: 12,
        lineHeight: 1.85,
      }}
    >
      {logs.length === 0 ? (
        <span style={{ color: T.muted }}>Waiting for task events...</span>
      ) : (
        logs.map((entry, index) => (
          <div
            key={`${entry.seq || index}-${entry.time || index}`}
            style={{
              color:
                entry.level === "error"
                  ? T.red
                  : entry.level === "success"
                    ? T.green
                    : entry.level === "warn"
                      ? T.amber
                      : T.text,
            }}
          >
            <span style={{ color: T.muted }}>{entry.time}</span>{"  "}
            <span style={{ color: T.cyan }}>[{entry.src}]</span>{"  "}
            {entry.msg}
          </div>
        ))
      )}
    </div>
  );
}

function SummaryStat({ label, value, color = T.white }) {
  return (
    <div style={{ minWidth: 120 }}>
      <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px" }}>{label}</div>
      <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 24, color, marginTop: 4 }}>
        {value}
      </div>
    </div>
  );
}

export default function PipelinePage({ activePipelineId, setActiveOutput }) {
  const [taskId, setTaskId] = useState(activePipelineId || "");
  const [taskInput, setTaskInput] = useState(activePipelineId || "");
  const [taskStatus, setTaskStatus] = useState(activePipelineId ? "pending" : "idle");
  const [result, setResult] = useState(null);
  const [logs, setLogs] = useState(() => (
    activePipelineId
      ? [{ msg: `Watching task ${truncateId(activePipelineId)}...`, src: "server", level: "info", time: formatTime(null), seq: 0 }]
      : []
  ));
  const [tasks, setTasks] = useState([]);
  const [lastError, setLastError] = useState("");
  const [loadingTasks, setLoadingTasks] = useState(true);
  const cursorRef = useRef(0);
  const resultFetchedRef = useRef(false);

  const refreshTaskList = useCallback(async () => {
    try {
      const rows = await getTasks();
      setTasks(Array.isArray(rows) ? rows : []);
      setLastError("");
    } catch (error) {
      setLastError(`Task list unavailable: ${error.message}`);
    } finally {
      setLoadingTasks(false);
    }
  }, []);

  useEffect(() => {
    refreshTaskList();
    const timer = setInterval(refreshTaskList, 4000);
    return () => clearInterval(timer);
  }, [refreshTaskList]);

  useEffect(() => {
    if (!taskId) return undefined;

    let cancelled = false;
    let eventTimer = null;
    let statusTimer = null;
    cursorRef.current = 0;
    resultFetchedRef.current = false;

    const stopPolling = () => {
      if (eventTimer) clearInterval(eventTimer);
      if (statusTimer) clearInterval(statusTimer);
    };

    const pollEvents = async () => {
      try {
        const data = await getTaskEvents(taskId, cursorRef.current, 300);
        if (cancelled) return;

        if (data?.status) setTaskStatus(data.status);
        if (Array.isArray(data?.events) && data.events.length > 0) {
          setLogs((current) => {
            const incoming = data.events.map((event) => ({
              seq: event.seq,
              msg: event.message,
              src: event.source || "pipeline",
              level: event.level || "info",
              time: formatTime(event.time),
            }));
            return [...current, ...incoming].slice(-600);
          });
        }
        if (typeof data?.cursor === "number") cursorRef.current = data.cursor;
      } catch (error) {
        if (!cancelled) setLastError(`Live events unavailable: ${error.message}`);
      }
    };

    const pollStatus = async () => {
      try {
        const data = await getStatus(taskId);
        if (cancelled) return;

        setTaskStatus(data.status);

        if (data.status === "done" && !resultFetchedRef.current) {
          resultFetchedRef.current = true;
          const payload = await getResult(taskId);
          if (!cancelled && !payload?.notReady) setResult(payload);
        }

        if (data.status === "failed" && data.error) {
          setLogs((current) => {
            const msg = `Pipeline failed: ${data.error}`;
            if (current.some((item) => item.msg === msg)) return current;
            return [...current, { msg, src: "server", level: "error", time: formatTime(null), seq: current.length + 1 }].slice(-600);
          });
        }

        if (data.status === "done" || data.status === "failed") {
          await pollEvents();
          stopPolling();
        }
      } catch (error) {
        if (!cancelled) setLastError(`Status polling failed: ${error.message}`);
      }
    };

    pollEvents();
    pollStatus();
    eventTimer = setInterval(pollEvents, 1500);
    statusTimer = setInterval(pollStatus, 2500);

    return () => {
      cancelled = true;
      stopPolling();
    };
  }, [taskId]);

  const nodeStates = useMemo(() => deriveNodeStates(taskStatus, logs), [taskStatus, logs]);
  const progress = useMemo(() => progressFrom(taskStatus, nodeStates), [taskStatus, nodeStates]);
  const selectedTask = useMemo(
    () => tasks.find((item) => item.task_id === taskId) || null,
    [taskId, tasks]
  );

  const statusMeta = {
    idle: { label: "Ready", color: T.muted },
    pending: { label: "Pending", color: T.muted },
    running: { label: "Running", color: T.amber },
    done: { label: "Complete", color: T.green },
    failed: { label: "Failed", color: T.red },
  }[taskStatus] || { label: taskStatus, color: T.muted };

  const resultOutputId =
    outputIdFromPath(result?.golden_record_path) ||
    outputIdFromPath(result?.validation_report_path) ||
    outputIdFromPath(result?.pytest_report_path);

  const watchTask = (id) => {
    if (!id) return;
    cursorRef.current = 0;
    resultFetchedRef.current = false;
    setTaskInput(id);
    setTaskId(id);
    setResult(null);
    setTaskStatus("pending");
    setLogs([{ msg: `Watching task ${truncateId(id)}...`, src: "server", level: "info", time: formatTime(null), seq: 0 }]);
    setLastError("");
  };

  const clearTask = () => {
    setTaskInput("");
    setTaskId("");
    cursorRef.current = 0;
    resultFetchedRef.current = false;
    setTaskStatus("idle");
    setLogs([]);
    setResult(null);
    setLastError("");
  };

  const openFile = (path) => `${API_BASE}/file?path=${encodeURIComponent(path)}`;
  const liveCount = tasks.filter((item) => item.status === "running" || item.status === "pending").length;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <style>{`
        .pipeline-layout {
          display: grid;
          grid-template-columns: minmax(0, 1.2fr) minmax(320px, 0.8fr);
          gap: 18px;
          align-items: start;
        }
        @media (max-width: 1100px) {
          .pipeline-layout {
            grid-template-columns: 1fr;
          }
        }
      `}</style>

      <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "flex-start", flexWrap: "wrap" }}>
        <div>
          <h1 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 28, color: T.white }}>
            Pipeline Visualizer
          </h1>
          <p style={{ color: T.muted, fontSize: 13, marginTop: 6 }}>
            Stream task events, inspect stage progression, and open artifacts as soon as a run completes.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <Badge color={statusMeta.color}>
            <Dot color={statusMeta.color} pulse={taskStatus === "running"}/>
            {statusMeta.label}
          </Badge>
          <Badge color={liveCount ? T.amber : T.cyan}>
            {liveCount} live task{liveCount === 1 ? "" : "s"}
          </Badge>
        </div>
      </div>

      <Card style={{ padding: 20 }}>
        <div style={{ display: "flex", gap: 12, alignItems: "flex-end", flexWrap: "wrap" }}>
          <div style={{ flex: 1, minWidth: 220 }}>
            <label style={{ fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", display: "block", marginBottom: 8 }}>
              Task ID
            </label>
            <input
              value={taskInput}
              onChange={(event) => setTaskInput(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter" && taskInput.trim()) watchTask(taskInput.trim());
              }}
              placeholder="Paste task_id or pick a recent task from the right rail"
              style={{
                width: "100%",
                background: T.navy3,
                border: `1px solid ${T.border}`,
                borderRadius: 10,
                color: T.white,
                padding: "12px 14px",
                fontSize: 13,
                fontFamily: "'JetBrains Mono', monospace",
                outline: "none",
              }}
            />
          </div>
          <Btn onClick={() => watchTask(taskInput.trim())} disabled={!taskInput.trim()}>
            Watch Task
          </Btn>
          <Btn variant="ghost" onClick={clearTask}>
            Clear
          </Btn>
        </div>

        {taskStatus !== "idle" && (
          <div style={{ marginTop: 18 }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6, fontSize: 11, color: T.muted, fontFamily: "'JetBrains Mono', monospace" }}>
              <span>Pipeline Progress</span>
              <span>{progress}%</span>
            </div>
            <div style={{ height: 6, background: T.navy3, borderRadius: 999 }}>
              <div
                style={{
                  height: "100%",
                  borderRadius: 999,
                  background: `linear-gradient(90deg, ${T.amber}, ${T.cyan})`,
                  width: `${progress}%`,
                  transition: "width 0.6s ease",
                  boxShadow: `0 0 12px ${T.amber}66`,
                }}
              />
            </div>
          </div>
        )}
      </Card>

      <div className="pipeline-layout">
        <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
          <Card style={{ padding: 22 }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
              <div>
                <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
                  Execution Graph
                </h2>
                <div style={{ color: T.muted, fontSize: 12, marginTop: 4 }}>
                  Agent-by-agent progression inferred from the live event stream
                </div>
              </div>
              {selectedTask && <StatusPill status={selectedTask.status}/>}
            </div>
            <div style={{ marginTop: 18, overflowX: "auto" }}>
              <div style={{ minWidth: 980 }}>
                <PipelineGraph nodeStates={nodeStates} />
              </div>
            </div>
          </Card>

          <Card style={{ padding: 22 }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap", marginBottom: 14 }}>
              <div>
                <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
                  Live Event Stream
                </h2>
                <div style={{ color: T.muted, fontSize: 12, marginTop: 4 }}>
                  Streaming messages from <code>/events/{taskId || ":task_id"}</code>
                </div>
              </div>
              <div style={{ fontSize: 11, color: T.muted, fontFamily: "'JetBrains Mono', monospace" }}>
                {logs.length} event{logs.length === 1 ? "" : "s"}
              </div>
            </div>
            <LogFeed logs={logs} />
          </Card>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
          <Card style={{ padding: 20 }}>
            <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
              Task Snapshot
            </h2>
            {selectedTask ? (
              <div style={{ display: "flex", flexDirection: "column", gap: 14, marginTop: 16 }}>
                <div>
                  <div style={{ color: T.white, fontFamily: "'Syne', sans-serif", fontWeight: 700, fontSize: 18 }}>
                    {selectedTask.company}
                  </div>
                  <div style={{ color: T.muted, fontSize: 11, marginTop: 6, fontFamily: "'JetBrains Mono', monospace" }}>
                    {selectedTask.task_id}
                  </div>
                </div>

                <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
                  <SummaryStat label="Status" value={selectedTask.status} color={statusMeta.color}/>
                  <SummaryStat label="Fields" value={selectedTask.fields ?? "—"} color={T.amber}/>
                  <SummaryStat label="Passed" value={selectedTask.passed ?? "—"} color={T.green}/>
                  <SummaryStat label="Failed" value={selectedTask.failed ?? "—"} color={(selectedTask.failed || 0) > 0 ? T.red : T.white}/>
                </div>

                <div style={{ display: "grid", gap: 10 }}>
                  {[
                    ["Created", formatDateTime(selectedTask.created_at)],
                    ["Completed", formatDateTime(selectedTask.completed_at)],
                    ["Error", selectedTask.error || "—"],
                  ].map(([label, value]) => (
                    <div key={label} style={{ background: T.navy3, borderRadius: 10, padding: "12px 14px" }}>
                      <div style={{ fontSize: 11, color: T.muted, marginBottom: 6 }}>{label}</div>
                      <div style={{ color: label === "Error" && value !== "—" ? T.red : T.white, fontSize: 13, lineHeight: 1.6, wordBreak: "break-word" }}>
                        {value}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ) : (
              <div style={{ color: T.muted, fontSize: 13, marginTop: 14 }}>
                Pick a task to show summary data from <code>/tasks</code>.
              </div>
            )}
          </Card>

          <Card style={{ padding: 20 }}>
            <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
              Result Artifacts
            </h2>
            {result ? (
              <div style={{ display: "flex", flexDirection: "column", gap: 12, marginTop: 16 }}>
                <div style={{ display: "flex", gap: 14, flexWrap: "wrap" }}>
                  <SummaryStat label="Golden Fields" value={result.golden_record_count ?? "—"} color={T.amber}/>
                  <SummaryStat label="Passed" value={result.test_results?.passed ?? "—"} color={T.green}/>
                  <SummaryStat label="Failed" value={result.test_results?.failed ?? "—"} color={(result.test_results?.failed || 0) > 0 ? T.red : T.white}/>
                </div>

                {[
                  ["Golden Record", result.golden_record_path],
                  ["Validation Report", result.validation_report_path],
                  ["Pytest Report", result.pytest_report_path],
                  ["Semantic Chunks", result.chunk_record_path],
                ].map(([label, path]) => (
                  <div key={label} style={{ display: "flex", justifyContent: "space-between", gap: 10, padding: "12px 14px", borderRadius: 10, background: T.navy3 }}>
                    <div style={{ minWidth: 0 }}>
                      <div style={{ fontSize: 11, color: T.muted, marginBottom: 6 }}>{label}</div>
                      <div style={{ color: path ? T.white : T.muted, fontSize: 12, fontFamily: "'JetBrains Mono', monospace", wordBreak: "break-word" }}>
                        {path || "Not available"}
                      </div>
                    </div>
                    {path && (
                      <a href={openFile(path)} target="_blank" rel="noreferrer" style={{ alignSelf: "center", textDecoration: "none" }}>
                        <Btn variant="ghost" style={{ padding: "7px 12px", fontSize: 11 }}>
                          Open
                        </Btn>
                      </a>
                    )}
                  </div>
                ))}

                {resultOutputId && (
                  <Btn variant="ghost" onClick={() => setActiveOutput(resultOutputId)}>
                    Open In Outputs Library
                  </Btn>
                )}
              </div>
            ) : (
              <div style={{ color: T.muted, fontSize: 13, marginTop: 14 }}>
                Completed tasks automatically load artifact paths from <code>/result</code>.
              </div>
            )}
          </Card>

          <Card style={{ padding: 0, overflow: "hidden" }}>
            <div style={{ padding: "18px 20px", borderBottom: `1px solid ${T.border}`, display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center" }}>
              <div>
                <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
                  Recent Tasks
                </h2>
                <div style={{ color: T.muted, fontSize: 12, marginTop: 4 }}>
                  Quick-pick any task from the live task list
                </div>
              </div>
              <Btn variant="ghost" onClick={refreshTaskList} style={{ padding: "8px 12px", fontSize: 12 }}>
                Refresh
              </Btn>
            </div>

            {loadingTasks ? (
              <div style={{ padding: "24px 20px", color: T.muted, fontSize: 13 }}>Loading tasks...</div>
            ) : tasks.length === 0 ? (
              <div style={{ padding: "24px 20px", color: T.muted, fontSize: 13 }}>No tasks available yet.</div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column" }}>
                {tasks.slice(0, 8).map((item) => (
                  <button
                    key={item.task_id}
                    onClick={() => watchTask(item.task_id)}
                    style={{
                      border: "none",
                      borderTop: `1px solid ${T.border}`,
                      background: item.task_id === taskId ? T.amber + "10" : "transparent",
                      padding: "14px 20px",
                      textAlign: "left",
                      cursor: "pointer",
                    }}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                      <div>
                        <div style={{ color: T.white, fontSize: 14, fontWeight: 700, fontFamily: "'Syne', sans-serif" }}>
                          {item.company}
                        </div>
                        <div style={{ color: T.muted, fontSize: 11, marginTop: 6, fontFamily: "'JetBrains Mono', monospace" }}>
                          {truncateId(item.task_id)} · {formatDateTime(item.created_at)}
                        </div>
                      </div>
                      <StatusPill status={item.status}/>
                    </div>
                  </button>
                ))}
              </div>
            )}
          </Card>

          {lastError && (
            <Card style={{ padding: 16, borderColor: `${T.red}35` }}>
              <div style={{ color: T.red, fontSize: 12, fontFamily: "'JetBrains Mono', monospace", lineHeight: 1.6 }}>
                {lastError}
              </div>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}

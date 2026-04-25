import { useCallback, useDeferredValue, useEffect, useMemo, useState } from "react";
import { T } from "../theme";
import { Badge, Btn, Card, Dot } from "../components";
import { API_BASE, getFileJson, getOutputs } from "../api";
import {
  countFields,
  formatDateTime,
  normalizeChunkRows,
  normalizeGoldenRecord,
  normalizeValidationResults,
  prettyJson,
} from "../utils";

function SummaryTile({ label, value, color = T.white }) {
  return (
    <div style={{ minWidth: 120 }}>
      <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px" }}>{label}</div>
      <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 24, color, marginTop: 4 }}>
        {value}
      </div>
    </div>
  );
}

export default function OutputsPage({ activeOutputId }) {
  const [runs, setRuns] = useState([]);
  const [selectedId, setSelectedId] = useState(activeOutputId || "");
  const [activeTab, setActiveTab] = useState("golden");
  const [runQuery, setRunQuery] = useState("");
  const [fieldQuery, setFieldQuery] = useState("");
  const [payloads, setPayloads] = useState({
    golden: null,
    validation: null,
    pytest: null,
    chunks: null,
  });
  const [loadingRuns, setLoadingRuns] = useState(true);
  const [loadingFiles, setLoadingFiles] = useState(false);
  const [error, setError] = useState("");

  const deferredRunQuery = useDeferredValue(runQuery);
  const deferredFieldQuery = useDeferredValue(fieldQuery);

  const refreshRuns = useCallback(async () => {
    try {
      const data = await getOutputs(250);
      const list = Array.isArray(data) ? data : [];
      setRuns(list);

      if (activeOutputId && list.some((item) => item.id === activeOutputId)) {
        setSelectedId(activeOutputId);
      } else if (!selectedId && list.length > 0) {
        setSelectedId(list[0].id);
      } else if (selectedId && !list.some((item) => item.id === selectedId) && list.length > 0) {
        setSelectedId(list[0].id);
      }

      setError("");
    } catch (requestError) {
      setError(`Failed to load outputs: ${requestError.message}`);
    } finally {
      setLoadingRuns(false);
    }
  }, [activeOutputId, selectedId]);

  useEffect(() => {
    refreshRuns();
    const timer = setInterval(refreshRuns, 5000);
    return () => clearInterval(timer);
  }, [refreshRuns]);

  useEffect(() => {
    if (activeOutputId) setSelectedId(activeOutputId);
  }, [activeOutputId]);

  const filteredRuns = useMemo(() => {
    const query = deferredRunQuery.trim().toLowerCase();
    if (!query) return runs;
    return runs.filter((item) => {
      const haystack = `${item.company} ${item.company_slug} ${item.timestamp}`.toLowerCase();
      return haystack.includes(query);
    });
  }, [deferredRunQuery, runs]);

  const selected = useMemo(
    () => filteredRuns.find((item) => item.id === selectedId) || runs.find((item) => item.id === selectedId) || null,
    [filteredRuns, runs, selectedId]
  );

  useEffect(() => {
    if (!selected) {
      setPayloads({ golden: null, validation: null, pytest: null, chunks: null });
      return;
    }

    let cancelled = false;
    const loadFiles = async () => {
      setLoadingFiles(true);
      setError("");

      try {
        const [goldenResult, validationResult, pytestResult, chunkResult] = await Promise.allSettled([
          selected.golden_record_path ? getFileJson(selected.golden_record_path) : Promise.resolve(null),
          selected.validation_report_path ? getFileJson(selected.validation_report_path) : Promise.resolve(null),
          selected.pytest_report_path ? getFileJson(selected.pytest_report_path) : Promise.resolve(null),
          selected.semantic_chunks_path ? getFileJson(selected.semantic_chunks_path) : Promise.resolve(null),
        ]);

        if (cancelled) return;

        setPayloads({
          golden: goldenResult.status === "fulfilled" ? goldenResult.value : null,
          validation: validationResult.status === "fulfilled" ? validationResult.value : null,
          pytest: pytestResult.status === "fulfilled" ? pytestResult.value : null,
          chunks: chunkResult.status === "fulfilled" ? chunkResult.value : null,
        });
      } catch (requestError) {
        if (!cancelled) setError(`Could not load file content: ${requestError.message}`);
      } finally {
        if (!cancelled) setLoadingFiles(false);
      }
    };

    loadFiles();
    return () => {
      cancelled = true;
    };
  }, [selected]);

  const goldenRows = useMemo(() => normalizeGoldenRecord(payloads.golden), [payloads.golden]);
  const validationRows = useMemo(() => normalizeValidationResults(payloads.validation), [payloads.validation]);
  const chunkRows = useMemo(() => normalizeChunkRows(payloads.chunks), [payloads.chunks]);

  const filteredGoldenRows = useMemo(() => {
    const query = deferredFieldQuery.trim().toLowerCase();
    if (!query) return goldenRows;
    return goldenRows.filter((row) => {
      const haystack = `${row.field} ${row.rawKey} ${row.value}`.toLowerCase();
      return haystack.includes(query);
    });
  }, [deferredFieldQuery, goldenRows]);

  const availableTabs = useMemo(() => {
    const tabs = [
      { id: "golden", label: "Golden Record" },
      { id: "validation", label: "Validation", enabled: !!selected?.validation_report_path },
      { id: "pytest", label: "Pytest", enabled: !!selected?.pytest_report_path },
      { id: "chunks", label: "Chunks", enabled: !!selected?.semantic_chunks_path },
      { id: "raw", label: "Raw JSON", enabled: true },
    ];
    return tabs.filter((tab) => tab.enabled !== false);
  }, [selected]);

  useEffect(() => {
    if (!availableTabs.some((tab) => tab.id === activeTab)) {
      setActiveTab(availableTabs[0]?.id || "golden");
    }
  }, [activeTab, availableTabs]);

  const downloadLink = (path) => `${API_BASE}/file?path=${encodeURIComponent(path)}`;
  const totalOutputs = runs.length;
  const avgFields = runs.length
    ? Math.round(runs.filter((item) => typeof item.fields === "number").reduce((sum, item) => sum + (item.fields || 0), 0) / Math.max(runs.filter((item) => typeof item.fields === "number").length, 1))
    : 0;
  const perfectRuns = runs.filter((item) => typeof item.failed === "number" && item.failed === 0).length;
  const selectedCoverage = selected ? Math.round((countFields(payloads.golden) / 163) * 100) : 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <style>{`
        .outputs-layout {
          display: grid;
          grid-template-columns: minmax(280px, 340px) minmax(0, 1fr);
          gap: 18px;
          align-items: start;
        }
        @media (max-width: 1080px) {
          .outputs-layout {
            grid-template-columns: 1fr;
          }
        }
      `}</style>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16, flexWrap: "wrap" }}>
        <div>
          <h1 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 28, color: T.white }}>
            Output Library
          </h1>
          <p style={{ color: T.muted, fontSize: 13, marginTop: 6 }}>
            Browse grouped artifacts from <code>output/</code>, including golden records, validation reports, pytest results, and chunk files.
          </p>
        </div>
        <Btn variant="ghost" onClick={refreshRuns} style={{ padding: "8px 16px", fontSize: 12 }}>
          Refresh Files
        </Btn>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 14 }}>
        <Card style={{ padding: "18px 20px" }}>
          <SummaryTile label="Stored Runs" value={totalOutputs} color={T.amber}/>
        </Card>
        <Card style={{ padding: "18px 20px" }}>
          <SummaryTile label="Avg Fields" value={avgFields || "—"} color={T.cyan}/>
        </Card>
        <Card style={{ padding: "18px 20px" }}>
          <SummaryTile label="Zero-Failure Runs" value={perfectRuns} color={T.green}/>
        </Card>
        <Card style={{ padding: "18px 20px" }}>
          <SummaryTile label="Selected Coverage" value={selected ? `${selectedCoverage}%` : "—"} color={T.white}/>
        </Card>
      </div>

      <div className="outputs-layout">
        <Card style={{ padding: 0, overflow: "hidden" }}>
          <div style={{ padding: "18px 20px", borderBottom: `1px solid ${T.border}` }}>
            <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
              Runs
            </h2>
            <div style={{ color: T.muted, fontSize: 12, marginTop: 4 }}>
              Output groups discovered by the backend
            </div>
            <input
              value={runQuery}
              onChange={(event) => setRunQuery(event.target.value)}
              placeholder="Filter by company"
              style={{
                width: "100%",
                marginTop: 14,
                background: T.navy3,
                border: `1px solid ${T.border}`,
                borderRadius: 10,
                color: T.white,
                padding: "11px 12px",
                fontSize: 13,
                outline: "none",
              }}
            />
          </div>

          {loadingRuns ? (
            <div style={{ padding: "26px 20px", color: T.muted, fontSize: 13 }}>Loading output groups...</div>
          ) : filteredRuns.length === 0 ? (
            <div style={{ padding: "26px 20px", color: T.muted, fontSize: 13 }}>
              No outputs match the current filter.
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", maxHeight: "70dvh", overflowY: "auto" }}>
              {filteredRuns.map((run) => (
                <button
                  key={run.id}
                  onClick={() => setSelectedId(run.id)}
                  style={{
                    padding: "16px 20px",
                    border: "none",
                    borderTop: `1px solid ${T.border}`,
                    background: selected?.id === run.id ? T.amber + "10" : "transparent",
                    textAlign: "left",
                    cursor: "pointer",
                    color: T.text,
                  }}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
                    <div>
                      <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 700, color: T.white, fontSize: 15 }}>
                        {run.company}
                      </div>
                      <div style={{ fontSize: 11, color: T.muted, marginTop: 6, fontFamily: "'JetBrains Mono', monospace" }}>
                        {formatDateTime(run.created_at)}
                      </div>
                    </div>
                    <div style={{ textAlign: "right" }}>
                      <div style={{ color: typeof run.fields === "number" ? T.amber : T.muted, fontSize: 12 }}>
                        {typeof run.fields === "number" ? `${run.fields} fields` : "No field count"}
                      </div>
                      <div style={{ color: (run.failed || 0) > 0 ? T.red : T.green, fontSize: 11, marginTop: 6 }}>
                        {typeof run.failed === "number"
                          ? run.failed > 0
                            ? `${run.failed} failed`
                            : "All pass"
                          : "No tests"}
                      </div>
                    </div>
                  </div>
                </button>
              ))}
            </div>
          )}
        </Card>

        {selected ? (
          <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
            <Card style={{ padding: 22 }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 16, flexWrap: "wrap" }}>
                <div>
                  <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 24, color: T.white }}>
                    {selected.company}
                  </h2>
                  <div style={{ color: T.muted, fontSize: 12, marginTop: 6, fontFamily: "'JetBrains Mono', monospace" }}>
                    {selected.id}
                  </div>
                </div>
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  {selected.golden_record_path && (
                    <a href={downloadLink(selected.golden_record_path)} target="_blank" rel="noreferrer" style={{ textDecoration: "none" }}>
                      <Btn variant="ghost" style={{ fontSize: 12, padding: "8px 14px" }}>Golden JSON</Btn>
                    </a>
                  )}
                  {selected.validation_report_path && (
                    <a href={downloadLink(selected.validation_report_path)} target="_blank" rel="noreferrer" style={{ textDecoration: "none" }}>
                      <Btn variant="ghost" style={{ fontSize: 12, padding: "8px 14px" }}>Validation JSON</Btn>
                    </a>
                  )}
                  {selected.pytest_report_path && (
                    <a href={downloadLink(selected.pytest_report_path)} target="_blank" rel="noreferrer" style={{ textDecoration: "none" }}>
                      <Btn variant="ghost" style={{ fontSize: 12, padding: "8px 14px" }}>Pytest JSON</Btn>
                    </a>
                  )}
                  {selected.semantic_chunks_path && (
                    <a href={downloadLink(selected.semantic_chunks_path)} target="_blank" rel="noreferrer" style={{ textDecoration: "none" }}>
                      <Btn variant="ghost" style={{ fontSize: 12, padding: "8px 14px" }}>Chunks JSON</Btn>
                    </a>
                  )}
                </div>
              </div>

              <div style={{ display: "flex", gap: 20, flexWrap: "wrap", marginTop: 18, paddingTop: 16, borderTop: `1px solid ${T.border}` }}>
                <SummaryTile label="Fields" value={countFields(payloads.golden) || selected.fields || "—"} color={T.amber}/>
                <SummaryTile label="Passed" value={payloads.pytest?.passed ?? selected.passed ?? "—"} color={T.green}/>
                <SummaryTile label="Failed" value={payloads.pytest?.failed ?? selected.failed ?? "—"} color={(payloads.pytest?.failed || selected.failed || 0) > 0 ? T.red : T.white}/>
                <SummaryTile label="Created" value={formatDateTime(selected.created_at)} color={T.white}/>
              </div>
            </Card>

            <Card style={{ padding: 20 }}>
              <h3 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 700, fontSize: 13, color: T.muted, marginBottom: 14, textTransform: "uppercase", letterSpacing: "1px" }}>
                Files
              </h3>
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {[
                  ["Golden Record", selected.golden_record_path],
                  ["Validation Report", selected.validation_report_path],
                  ["Pytest Report", selected.pytest_report_path],
                  ["Semantic Chunks", selected.semantic_chunks_path],
                ].map(([label, path]) => (
                  <div key={label} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "10px 14px", background: T.navy3, borderRadius: 10 }}>
                    <div style={{ minWidth: 0 }}>
                      <div style={{ fontSize: 11, color: T.muted, marginBottom: 4 }}>{label}</div>
                      <div style={{ fontSize: 12, color: path ? T.amber : T.muted, fontFamily: "'JetBrains Mono', monospace", wordBreak: "break-word" }}>
                        {path ? path.split("/").pop() : "Not available"}
                      </div>
                    </div>
                    <Dot color={path ? T.green : T.muted}/>
                  </div>
                ))}
              </div>
            </Card>

            <Card style={{ padding: 20 }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap", marginBottom: 14 }}>
                <div>
                  <h3 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
                    File Data
                  </h3>
                  <div style={{ color: T.muted, fontSize: 12, marginTop: 4 }}>
                    Structured viewer backed by <code>/file</code>
                  </div>
                </div>
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  {availableTabs.map((tab) => (
                    <Btn
                      key={tab.id}
                      variant={activeTab === tab.id ? "primary" : "ghost"}
                      onClick={() => setActiveTab(tab.id)}
                      style={{ padding: "7px 12px", fontSize: 12 }}
                    >
                      {tab.label}
                    </Btn>
                  ))}
                </div>
              </div>

              {loadingFiles ? (
                <div style={{ color: T.muted, fontSize: 13 }}>Loading file content...</div>
              ) : activeTab === "golden" ? (
                <>
                  <input
                    value={fieldQuery}
                    onChange={(event) => setFieldQuery(event.target.value)}
                    placeholder="Filter fields"
                    style={{
                      width: "100%",
                      marginBottom: 14,
                      background: T.navy3,
                      border: `1px solid ${T.border}`,
                      borderRadius: 10,
                      color: T.white,
                      padding: "11px 12px",
                      fontSize: 13,
                      outline: "none",
                    }}
                  />

                  {filteredGoldenRows.length > 0 ? (
                    <div style={{ maxHeight: 520, overflow: "auto", border: `1px solid ${T.border}`, borderRadius: 10 }}>
                      <table style={{ width: "100%", borderCollapse: "collapse" }}>
                        <thead style={{ position: "sticky", top: 0, background: T.navy3 }}>
                          <tr>
                            {["Field", "Value", "Source"].map((header) => (
                              <th key={header} style={{ padding: "10px 14px", textAlign: "left", fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.7px" }}>
                                {header}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {filteredGoldenRows.map((row) => (
                            <tr key={row.rawKey} style={{ borderTop: `1px solid ${T.border}` }}>
                              <td style={{ padding: "12px 14px", fontSize: 12, color: T.white, minWidth: 180 }}>{row.field}</td>
                              <td style={{ padding: "12px 14px", fontSize: 12, color: T.text, whiteSpace: "pre-wrap", lineHeight: 1.6 }}>{row.value}</td>
                              <td style={{ padding: "12px 14px", fontSize: 11, color: T.muted }}>{row.source}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div style={{ color: T.muted, fontSize: 13 }}>Golden record not available.</div>
                  )}
                </>
              ) : activeTab === "validation" ? (
                payloads.validation ? (
                  <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                    <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
                      <SummaryTile label="Expected" value={payloads.validation.total_expected ?? "—"} color={T.white}/>
                      <SummaryTile label="Received" value={payloads.validation.total_received ?? "—"} color={T.cyan}/>
                      <SummaryTile label="Passed" value={payloads.validation.total_passed ?? "—"} color={T.green}/>
                      <SummaryTile label="Failed" value={payloads.validation.total_failed ?? "—"} color={(payloads.validation.total_failed || 0) > 0 ? T.red : T.white}/>
                      <SummaryTile label="Completeness" value={payloads.validation.completeness_pct != null ? `${payloads.validation.completeness_pct}%` : "—"} color={T.amber}/>
                    </div>

                    {validationRows.length > 0 && (
                      <div style={{ maxHeight: 420, overflow: "auto", border: `1px solid ${T.border}`, borderRadius: 10 }}>
                        <table style={{ width: "100%", borderCollapse: "collapse" }}>
                          <thead style={{ position: "sticky", top: 0, background: T.navy3 }}>
                            <tr>
                              {["Parameter", "Category", "Status", "Issue"].map((header) => (
                                <th key={header} style={{ padding: "10px 14px", textAlign: "left", fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.7px" }}>
                                  {header}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {validationRows.map((row) => (
                              <tr key={`${row.id}-${row.parameter}`} style={{ borderTop: `1px solid ${T.border}` }}>
                                <td style={{ padding: "12px 14px", fontSize: 12, color: T.white }}>{row.parameter}</td>
                                <td style={{ padding: "12px 14px", fontSize: 12, color: T.text }}>{row.category}</td>
                                <td style={{ padding: "12px 14px", fontSize: 12, color: row.status.includes("FAIL") ? T.red : T.green }}>{row.status}</td>
                                <td style={{ padding: "12px 14px", fontSize: 12, color: row.issue ? T.red : T.muted }}>{row.issue || "—"}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                ) : (
                  <div style={{ color: T.muted, fontSize: 13 }}>Validation report not available.</div>
                )
              ) : activeTab === "pytest" ? (
                payloads.pytest ? (
                  <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                    <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
                      <SummaryTile label="Total" value={payloads.pytest.total ?? "—"} color={T.white}/>
                      <SummaryTile label="Passed" value={payloads.pytest.passed ?? "—"} color={T.green}/>
                      <SummaryTile label="Failed" value={payloads.pytest.failed ?? "—"} color={(payloads.pytest.failed || 0) > 0 ? T.red : T.white}/>
                      <SummaryTile label="Skipped" value={payloads.pytest.skipped ?? "—"} color={T.cyan}/>
                      <SummaryTile label="Duration" value={payloads.pytest.duration_sec != null ? `${payloads.pytest.duration_sec}s` : "—"} color={T.amber}/>
                    </div>

                    {[
                      ["Failed Tests", payloads.pytest.failed_tests],
                      ["Error Tests", payloads.pytest.error_tests],
                      ["Failed Parameter IDs", payloads.pytest.failed_parameter_ids],
                    ].map(([label, items]) => (
                      <div key={label} style={{ background: T.navy3, borderRadius: 10, padding: "12px 14px" }}>
                        <div style={{ fontSize: 11, color: T.muted, marginBottom: 8 }}>{label}</div>
                        <div style={{ color: Array.isArray(items) && items.length ? T.white : T.muted, fontSize: 12, lineHeight: 1.6, fontFamily: "'JetBrains Mono', monospace" }}>
                          {Array.isArray(items) && items.length ? prettyJson(items) : "None"}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div style={{ color: T.muted, fontSize: 13 }}>Pytest report not available.</div>
                )
              ) : activeTab === "chunks" ? (
                chunkRows.length > 0 ? (
                  <div style={{ display: "flex", flexDirection: "column", gap: 10, maxHeight: 520, overflow: "auto" }}>
                    {chunkRows.map((row) => (
                      <div key={`${row.id}-${row.title}`} style={{ background: T.navy3, borderRadius: 10, padding: "14px 16px" }}>
                        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
                          <div style={{ color: T.white, fontWeight: 700, fontSize: 14 }}>
                            {row.title}
                          </div>
                          <div style={{ color: row.score != null ? T.amber : T.muted, fontSize: 11, fontFamily: "'JetBrains Mono', monospace" }}>
                            {row.score != null ? `score=${row.score}` : row.category}
                          </div>
                        </div>
                        <div style={{ color: T.text, fontSize: 12, lineHeight: 1.7, marginTop: 10, whiteSpace: "pre-wrap" }}>
                          {row.text}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div style={{ color: T.muted, fontSize: 13 }}>Chunk file not available.</div>
                )
              ) : (
                <pre
                  style={{
                    background: T.navy,
                    border: `1px solid ${T.border}`,
                    borderRadius: 10,
                    padding: "14px 16px",
                    color: T.text,
                    fontSize: 12,
                    lineHeight: 1.7,
                    overflow: "auto",
                    maxHeight: 520,
                    fontFamily: "'JetBrains Mono', monospace",
                  }}
                >
                  {prettyJson({
                    golden_record: payloads.golden,
                    validation_report: payloads.validation,
                    pytest_report: payloads.pytest,
                    semantic_chunks: payloads.chunks,
                  })}
                </pre>
              )}
            </Card>

            {error && (
              <Card style={{ padding: 16, borderColor: `${T.amber}35` }}>
                <div style={{ color: T.amber, fontSize: 12, fontFamily: "'JetBrains Mono', monospace", lineHeight: 1.6 }}>
                  {error}
                </div>
              </Card>
            )}
          </div>
        ) : (
          <Card style={{ padding: 40, textAlign: "center" }}>
            <div style={{ color: T.muted, fontSize: 13 }}>
              Select a run from the left to inspect its output files.
            </div>
          </Card>
        )}
      </div>
    </div>
  );
}

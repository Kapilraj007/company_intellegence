import { startTransition, useMemo, useState } from "react";
import { T } from "../theme";
import { Badge, Btn, Card, Dot, Spinner } from "../components";
import { searchCompanies, searchSimilarCompanies, submitJob } from "../api";
import { formatDateTime, normalizeGoldenRecord, searchResultKey } from "../utils";

const SEARCH_MODES = {
  companies: {
    title: "SEMANTIC SEARCH",
    description: "Search companies using semantic understanding of capabilities, technologies, and business problems instead of simple keyword matching.",
    endpoint: "/search-companies",
    placeholder: "Fraud detection and AI risk monitoring for banks",
    examples: [
      "fraud detection and AI risk monitoring for banks",
      "enterprise ERP implementation and managed services",
      "customer support automation for large retailers",
      "cybersecurity SOC services for mid-market companies",
    ],
  },
  similar: {
    title: "SIMILAR COMPANIES",
    description: "Find companies similar to a selected company using vector similarity across innovation signals, technologies, and business models.",
    endpoint: "/search/similar",
    placeholder: "Stripe",
    examples: ["Stripe", "Apple", "NVIDIA", "Tesla"],
  },
};

function ResultStat({ label, value, color = T.white }) {
  return (
    <div style={{ minWidth: 120 }}>
      <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px" }}>{label}</div>
      <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 22, color, marginTop: 4 }}>
        {value}
      </div>
    </div>
  );
}

export default function ExplorerPage({
  apiInfo,
  setActivePipeline,
  searchState,
  setSearchState,
  openSearchDetail,
}) {
  const [loading, setLoading] = useState(false);
  const [runningCompany, setRunningCompany] = useState("");
  const [error, setError] = useState("");

  const modeConfig = SEARCH_MODES[searchState.mode] || SEARCH_MODES.companies;
  const sourceCompany = searchState.meta?.company || null;
  const selected = useMemo(
    () => searchState.results.find((item) => searchResultKey(item) === searchState.selectedKey) || searchState.results[0] || null,
    [searchState.results, searchState.selectedKey]
  );
  const selectedGoldenRows = useMemo(() => {
    const consolidatedJson = selected?.full_company_data?.consolidated?.json;
    return normalizeGoldenRecord(consolidatedJson).slice(0, 8);
  }, [selected]);
  const selectedStoredChunkCount =
    (selected?.full_company_data?.consolidated?.chunk_count > 0
      ? selected.full_company_data.consolidated.chunk_count
      : selected?.full_company_data?.chunks?.length) ?? "—";
  const selectedUpdatedAt =
    selected?.full_company_data?.updated_at ||
    selected?.full_company_data?.consolidated?.generated_at ||
    selected?.full_company_data?.raw_data?.updated_at ||
    null;

  const updateSearchState = (patch) => {
    setSearchState((current) => ({ ...current, ...patch }));
  };

  const handleModeChange = (mode) => {
    const config = SEARCH_MODES[mode];
    setSearchState((current) => ({
      ...current,
      mode,
      query: config.placeholder,
      meta: null,
      results: [],
      selectedKey: "",
      detailKey: "",
    }));
    setError("");
  };

  const handleSearch = async () => {
    const query = searchState.query.trim();
    if (!query) {
      setError("Enter a search query first.");
      return;
    }

    setLoading(true);
    setError("");
    try {
      const searchFn = searchState.mode === "similar" ? searchSimilarCompanies : searchCompanies;
      const data = await searchFn({
        query,
        top_k: searchState.topK,
        top_k_chunks: Math.max(120, searchState.topK * 20),
        exclude_company: searchState.excludeCompany.trim(),
        include_full_data: true,
      });

      startTransition(() => {
        const nextResults = Array.isArray(data?.results) ? data.results : [];
        updateSearchState({
          meta: { ...data, search_mode: searchState.mode, endpoint: modeConfig.endpoint },
          results: nextResults,
          selectedKey: nextResults[0] ? searchResultKey(nextResults[0]) : "",
          detailKey: "",
        });
      });
    } catch (requestError) {
      setError(`Search failed: ${requestError.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleRunPipeline = async (companyName) => {
    if (!companyName) return;
    setRunningCompany(companyName);
    setError("");
    try {
      const data = await submitJob(companyName);
      setActivePipeline(data.task_id);
    } catch (requestError) {
      setError(`Could not queue ${companyName}: ${requestError.message}`);
    } finally {
      setRunningCompany("");
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <style>{`
        .search-workspace {
          display: grid;
          grid-template-columns: minmax(300px, 0.95fr) minmax(0, 1.05fr);
          gap: 18px;
          align-items: start;
        }
        .search-controls {
          display: grid;
          grid-template-columns: minmax(0, 1fr) 120px 220px auto;
          gap: 12px;
          align-items: end;
        }
        .search-modes {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 12px;
        }
        @media (max-width: 1100px) {
          .search-workspace {
            grid-template-columns: 1fr;
          }
        }
        @media (max-width: 900px) {
          .search-controls,
          .search-modes {
            grid-template-columns: 1fr;
          }
        }
      `}</style>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16, flexWrap: "wrap" }}>
        <div>
          <h1 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 28, color: T.white }}>
            Search Workspace
          </h1>
          <p style={{ color: T.muted, fontSize: 13, marginTop: 6 }}>
            Two search intents, two backend endpoints, one result workflow with a dedicated details page.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <Badge color={apiInfo.online ? T.green : T.red}>
            <Dot color={apiInfo.online ? T.green : T.red} pulse={apiInfo.online}/>
            {apiInfo.online ? "Backend Ready" : "Backend Offline"}
          </Badge>
          <Badge color={T.amber}>{modeConfig.endpoint}</Badge>
          {searchState.mode === "similar" && sourceCompany?.company_name && (
            <Badge color={T.cyan}>Source: {sourceCompany.company_name}</Badge>
          )}
          {searchState.meta?.backend && (
            <Badge color={searchState.meta.backend === "pinecone" ? T.green : T.amber}>
              {searchState.meta.backend}
            </Badge>
          )}
        </div>
      </div>

      <div className="search-modes">
        {Object.entries(SEARCH_MODES).map(([mode, config]) => {
          const active = mode === searchState.mode;
          return (
            <button
              key={mode}
              onClick={() => handleModeChange(mode)}
              style={{
                textAlign: "left",
                border: `1px solid ${active ? `${T.amber}50` : T.border}`,
                background: active ? T.amber + "10" : T.navy2,
                borderRadius: 14,
                padding: "18px 20px",
                cursor: "pointer",
                color: T.text,
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
                <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
                  {config.title}
                </div>
                <Badge color={active ? T.amber : T.cyan}>{config.endpoint}</Badge>
              </div>
              <div style={{ color: T.muted, fontSize: 12, marginTop: 8, lineHeight: 1.6 }}>
                {config.description}
              </div>
              {mode === "similar" && (
                <div style={{ color: T.muted, fontSize: 11, marginTop: 8, lineHeight: 1.6 }}>
                  The source company needs indexed vectors first, so run the pipeline for it before using this mode.
                </div>
              )}
            </button>
          );
        })}
      </div>

      <Card style={{ padding: 24 }}>
        <div className="search-controls">
          <div style={{ minWidth: 0 }}>
            <label style={{ fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", display: "block", marginBottom: 8 }}>
              Query
            </label>
            <input
              value={searchState.query}
              onChange={(event) => updateSearchState({ query: event.target.value })}
              onKeyDown={(event) => {
                if (event.key === "Enter") handleSearch();
              }}
              placeholder={modeConfig.placeholder}
              style={{
                width: "100%",
                background: T.navy3,
                border: `1px solid ${T.border}`,
                borderRadius: 10,
                color: T.white,
                padding: "13px 14px",
                fontSize: 14,
                outline: "none",
              }}
            />
          </div>

          <div>
            <label style={{ fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", display: "block", marginBottom: 8 }}>
              Top K
            </label>
            <select
              value={searchState.topK}
              onChange={(event) => updateSearchState({ topK: Number(event.target.value) })}
              style={{
                width: "100%",
                background: T.navy3,
                border: `1px solid ${T.border}`,
                borderRadius: 10,
                color: T.white,
                padding: "13px 14px",
                fontSize: 14,
                outline: "none",
              }}
            >
              {[3, 5, 8, 10].map((count) => (
                <option key={count} value={count}>{count}</option>
              ))}
            </select>
          </div>

          <div style={{ minWidth: 0 }}>
            <label style={{ fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", display: "block", marginBottom: 8 }}>
              Exclude Company
            </label>
            <input
              value={searchState.excludeCompany}
              onChange={(event) => updateSearchState({ excludeCompany: event.target.value })}
              placeholder="Optional"
              style={{
                width: "100%",
                background: T.navy3,
                border: `1px solid ${T.border}`,
                borderRadius: 10,
                color: T.white,
                padding: "13px 14px",
                fontSize: 14,
                outline: "none",
              }}
            />
          </div>

          <Btn
            onClick={handleSearch}
            disabled={!apiInfo.online || loading || !searchState.query.trim()}
            style={{ minWidth: 150, padding: "13px 20px", fontSize: 14 }}
          >
            {loading ? (
              <>
                <Spinner size={14} color="#000"/>
                Searching...
              </>
            ) : "Search"}
          </Btn>
        </div>

        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 14 }}>
          {modeConfig.examples.map((example) => (
            <button
              key={example}
              onClick={() => updateSearchState({ query: example })}
              style={{
                background: T.navy3,
                color: T.text,
                border: `1px solid ${T.border}`,
                borderRadius: 999,
                cursor: "pointer",
                padding: "7px 11px",
                fontSize: 12,
                fontFamily: "'JetBrains Mono', monospace",
              }}
            >
              {example}
            </button>
          ))}
        </div>

        {searchState.meta && (
          <div style={{ display: "flex", gap: 18, flexWrap: "wrap", marginTop: 18, paddingTop: 16, borderTop: `1px solid ${T.border}` }}>
            <ResultStat label="Endpoint" value={searchState.meta.endpoint} color={T.amber}/>
            <ResultStat label="Results" value={searchState.meta.result_count ?? searchState.results.length} color={T.green}/>
            <ResultStat label="Top Chunks" value={searchState.meta.top_k_chunks ?? "—"} color={T.cyan}/>
            <ResultStat label="Searched At" value={formatDateTime(searchState.meta.date)} color={T.white}/>
          </div>
        )}

        {searchState.mode === "similar" && sourceCompany?.company_name && (
          <div style={{ marginTop: 16, background: T.cyan + "10", border: `1px solid ${T.cyan}30`, borderRadius: 12, padding: "14px 16px" }}>
            <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 8 }}>
              Compared Source Company
            </div>
            <div style={{ color: T.white, fontFamily: "'Syne', sans-serif", fontWeight: 700, fontSize: 18, overflowWrap: "anywhere" }}>
              {sourceCompany.company_name}
            </div>
            <div style={{ color: T.muted, fontSize: 12, marginTop: 6, fontFamily: "'JetBrains Mono', monospace", overflowWrap: "anywhere" }}>
              {sourceCompany.company_id || searchState.query}
            </div>
          </div>
        )}

        {error && (
          <div style={{ marginTop: 16, background: T.red + "12", border: `1px solid ${T.red}35`, borderRadius: 10, padding: "12px 14px", color: T.red, fontSize: 13 }}>
            {error}
          </div>
        )}
      </Card>

      <div className="search-workspace">
        <Card style={{ padding: 0, overflow: "hidden" }}>
          <div style={{ padding: "18px 20px", borderBottom: `1px solid ${T.border}` }}>
            <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
              Results
            </h2>
            <div style={{ color: T.muted, fontSize: 12, marginTop: 4 }}>
              Ranked matches for the current search intent
            </div>
          </div>

          {searchState.results.length === 0 ? (
            <div
              style={{
                padding: "28px 20px",
                color: T.muted,
                fontSize: 13,
                display: "flex",
                alignItems: "center",
                gap: 10,
              }}
            >
              {loading && <Spinner size={16} color={T.amber}/>}
              <span>{loading ? "Searching the indexed company graph..." : "Run a search to see matching companies."}</span>
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column" }}>
              {searchState.results.map((item) => {
                const active = searchResultKey(item) === searchResultKey(selected);
                const matchLabel = item.match_count
                  ? searchState.mode === "similar"
                    ? `${item.match_count} shared categories`
                    : `${item.match_count} chunk hits`
                  : item.category || "match";
                return (
                  <div
                    key={searchResultKey(item)}
                    style={{
                      borderTop: `1px solid ${T.border}`,
                      background: active ? T.amber + "10" : "transparent",
                      padding: "16px 20px",
                    }}
                  >
                    <button
                      onClick={() => updateSearchState({ selectedKey: searchResultKey(item) })}
                      style={{
                        width: "100%",
                        background: "transparent",
                        border: "none",
                        textAlign: "left",
                        cursor: "pointer",
                        color: T.text,
                        padding: 0,
                      }}
                    >
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                        <div>
                          <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 700, color: T.white, fontSize: 15, overflowWrap: "anywhere" }}>
                            {item.company_name || "Unknown company"}
                          </div>
                          <div style={{ fontSize: 11, color: T.muted, marginTop: 6, fontFamily: "'JetBrains Mono', monospace", overflowWrap: "anywhere" }}>
                            {item.company_id || "no-company-id"}
                          </div>
                        </div>
                        <div style={{ textAlign: "right" }}>
                          <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 22, color: T.amber }}>
                            {typeof item.score === "number" ? item.score.toFixed(3) : "—"}
                          </div>
                          <div style={{ fontSize: 11, color: T.muted, marginTop: 4 }}>
                            {matchLabel}
                          </div>
                        </div>
                      </div>

                      {item.snippet && (
                        <div style={{ color: T.muted, fontSize: 12, lineHeight: 1.6, marginTop: 10, overflowWrap: "anywhere" }}>
                          {item.snippet}
                        </div>
                      )}
                    </button>

                    {searchState.mode === "similar" && Array.isArray(item.shared_categories) && item.shared_categories.length > 0 && (
                      <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 10 }}>
                        {item.shared_categories.slice(0, 4).map((category) => (
                          <Badge key={category} color={T.cyan}>{category}</Badge>
                        ))}
                      </div>
                    )}

                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 12 }}>
                      <Btn
                        variant="ghost"
                        onClick={() => openSearchDetail(searchResultKey(item))}
                        style={{ padding: "7px 12px", fontSize: 11 }}
                      >
                        View Details
                      </Btn>
                      <Btn
                        variant="ghost"
                        onClick={() => handleRunPipeline(item.company_name)}
                        disabled={!apiInfo.online || runningCompany === item.company_name}
                        style={{ padding: "7px 12px", fontSize: 11 }}
                      >
                        {runningCompany === item.company_name ? (
                          <>
                            <Spinner size={13} color={T.amber}/>
                            Queueing...
                          </>
                        ) : "Run Pipeline"}
                      </Btn>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </Card>

        <Card style={{ padding: 24 }}>
          {selected ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
                <div>
                  <Badge color={T.amber}>Selected Result</Badge>
                  <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 24, color: T.white, marginTop: 12 }}>
                    {selected.company_name || "Unknown company"}
                  </h2>
                  <div style={{ color: T.muted, fontSize: 12, marginTop: 6, fontFamily: "'JetBrains Mono', monospace", overflowWrap: "anywhere" }}>
                    {selected.company_id || "No company_id"} {selected.category ? `· ${selected.category}` : ""}
                  </div>
                </div>
                <Btn onClick={() => openSearchDetail(searchResultKey(selected))} style={{ minWidth: 170 }}>
                  View Full Details
                </Btn>
              </div>

              <div style={{ display: "flex", gap: 18, flexWrap: "wrap" }}>
                <ResultStat label="Score" value={typeof selected.score === "number" ? selected.score.toFixed(3) : "—"} color={T.amber}/>
                <ResultStat label="Matches" value={selected.match_count || selected.top_chunks?.length || 0} color={T.green}/>
                {searchState.mode === "similar" && (
                  <ResultStat
                    label="Shared Categories"
                    value={Array.isArray(selected.shared_categories) ? selected.shared_categories.length : "—"}
                    color={T.cyan}
                  />
                )}
                <ResultStat
                  label="Stored Chunks"
                  value={selectedStoredChunkCount}
                  color={T.cyan}
                />
                <ResultStat label="Updated" value={formatDateTime(selectedUpdatedAt)} color={T.white}/>
              </div>

              {Array.isArray(selected.top_chunks) && selected.top_chunks.length > 0 && (
                <Card style={{ padding: 18, background: T.navy3 }}>
                  <div style={{ fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 12 }}>
                    Top Chunk Evidence
                  </div>
                  <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                    {selected.top_chunks.slice(0, 3).map((chunk, index) => (
                      <div key={`${chunk.chunk_id || chunk.chunk_title || index}`} style={{ padding: "12px 14px", borderRadius: 10, background: T.navy2, border: `1px solid ${T.border}` }}>
                        <div style={{ display: "flex", justifyContent: "space-between", gap: 10, flexWrap: "wrap" }}>
                        <div style={{ color: T.white, fontWeight: 600, fontSize: 13 }}>
                          {chunk.chunk_title || `Chunk ${index + 1}`}
                        </div>
                        <div style={{ color: T.amber, fontSize: 12, fontFamily: "'JetBrains Mono', monospace" }}>
                          {typeof chunk.score === "number" ? chunk.score.toFixed(3) : "—"}
                        </div>
                      </div>
                      {searchState.mode === "similar" && chunk.source_category && (
                        <div style={{ color: T.muted, fontSize: 11, marginTop: 6, fontFamily: "'JetBrains Mono', monospace", overflowWrap: "anywhere" }}>
                          source category: {chunk.source_category}
                        </div>
                      )}
                      {(chunk.snippet || selected.snippet) && (
                        <div style={{ color: T.muted, fontSize: 12, lineHeight: 1.6, marginTop: 10, overflowWrap: "anywhere" }}>
                          {chunk.snippet || selected.snippet}
                        </div>
                      )}
                      </div>
                    ))}
                  </div>
                </Card>
              )}

              {selectedGoldenRows.length > 0 && (
                <Card style={{ padding: 18, background: T.navy3 }}>
                  <div style={{ fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 12 }}>
                    Stored Snapshot Preview
                  </div>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 10 }}>
                    {selectedGoldenRows.map((row) => (
                      <div key={row.rawKey} style={{ padding: "12px 14px", borderRadius: 10, background: T.navy2, border: `1px solid ${T.border}` }}>
                        <div style={{ fontSize: 11, color: T.muted, marginBottom: 8 }}>{row.field}</div>
                        <div style={{ color: T.white, fontSize: 13, lineHeight: 1.6, whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
                          {row.value}
                        </div>
                      </div>
                    ))}
                  </div>
                </Card>
              )}
            </div>
          ) : (
            <div style={{ color: T.muted, fontSize: 13 }}>
              Select a result to preview it here, then open the dedicated details page.
            </div>
          )}
        </Card>
      </div>
    </div>
  );
}

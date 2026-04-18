import { useDeferredValue, useMemo, useState } from "react";
import { T } from "../theme";
import { Badge, Btn, Card, Dot, Spinner } from "../components";
import { submitJob } from "../api";
import {
  formatDateTime,
  humanizeKey,
  normalizeGoldenRecord,
  searchResultKey,
  stringifyValue,
} from "../utils";

const SEARCH_MODE_LABEL = {
  companies: "Capability Search",
  similar: "Similarity Search",
};

const SNIPPET_META_KEYS = new Set(["Company", "Category", "Section", "Semantic focus"]);

function DetailStat({ label, value, color = T.white }) {
  return (
    <div style={{ minWidth: 130 }}>
      <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px" }}>{label}</div>
      <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 24, color, marginTop: 4 }}>
        {value}
      </div>
    </div>
  );
}

function InsightTile({ label, value, tone = T.cyan }) {
  if (!value) return null;
  return (
    <div
      style={{
        background: T.navy3,
        border: `1px solid ${tone}28`,
        borderRadius: 12,
        padding: "12px 14px",
        minWidth: 0,
      }}
    >
      <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 6 }}>
        {label}
      </div>
      <div style={{ color: T.white, fontSize: 14, lineHeight: 1.6, overflowWrap: "anywhere" }}>
        {value}
      </div>
    </div>
  );
}

function formatPercent(value) {
  if (value == null || value === "") return "—";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return String(value);
  const decimals = Number.isInteger(numeric) ? 0 : 1;
  return `${numeric.toFixed(decimals)}%`;
}

function parseSnippetDetails(text) {
  if (!text || typeof text !== "string") {
    return { entries: [], focusItems: [], highlightEntries: [] };
  }

  const pattern = /([A-Z][A-Za-z0-9 &/()+#.'-]{1,40}?):\s*(.*?)(?=(?:\s+[A-Z][A-Za-z0-9 &/()+#.'-]{1,40}:\s)|$)/g;
  const entries = [];
  let match;

  while ((match = pattern.exec(text)) !== null) {
    const key = match[1].trim();
    const value = match[2].trim().replace(/\s+/g, " ");
    if (!value) continue;
    entries.push({ key, value });
  }

  const focusEntry = entries.find((entry) => entry.key === "Semantic focus");
  const focusItems = focusEntry
    ? focusEntry.value.split(/,\s*/).map((item) => item.trim()).filter(Boolean)
    : [];
  const highlightEntries = entries.filter((entry) => !SNIPPET_META_KEYS.has(entry.key)).slice(0, 4);

  return { entries, focusItems, highlightEntries };
}

export default function SearchDetailPage({
  apiInfo,
  setPage,
  setActivePipeline,
  searchState,
  setSearchState,
  openSearchDetail,
}) {
  const [runningCompany, setRunningCompany] = useState("");
  const [error, setError] = useState("");
  const [fieldQuery, setFieldQuery] = useState("");
  const deferredFieldQuery = useDeferredValue(fieldQuery);

  const result = useMemo(
    () => searchState.results.find((item) => searchResultKey(item) === searchState.detailKey) || null,
    [searchState.detailKey, searchState.results]
  );

  const goldenRows = useMemo(() => {
    const consolidatedJson = result?.full_company_data?.consolidated?.json;
    return normalizeGoldenRecord(consolidatedJson);
  }, [result]);
  const sourceCompany = searchState.meta?.company || null;
  const snippetDetails = useMemo(() => parseSnippetDetails(result?.snippet), [result]);
  const storedMetadata = useMemo(() => {
    const fullData = result?.full_company_data || {};
    const consolidated = fullData.consolidated || {};
    const rawData = fullData.raw_data || {};
    const chunkCount =
      (consolidated.chunk_count > 0 ? consolidated.chunk_count : fullData.chunks?.length) ?? null;
    const chunkCoverage = consolidated.chunk_coverage_pct;
    const schemaFieldCount = rawData.schema_field_count ?? goldenRows.length ?? null;

    return {
      company_id: fullData.company_id,
      company_name: fullData.company_name,
      chunk_count: chunkCount,
      chunk_coverage_pct: formatPercent(chunkCoverage),
      last_run_id: rawData.last_run_id || consolidated.run_id || null,
      schema_field_count: schemaFieldCount,
    };
  }, [goldenRows.length, result]);
  const resultUpdatedAt =
    result?.full_company_data?.updated_at ||
    result?.full_company_data?.consolidated?.generated_at ||
    result?.full_company_data?.raw_data?.updated_at ||
    null;

  const filteredGoldenRows = useMemo(() => {
    const query = deferredFieldQuery.trim().toLowerCase();
    if (!query) return goldenRows;
    return goldenRows.filter((row) => `${row.field} ${row.rawKey} ${row.value}`.toLowerCase().includes(query));
  }, [deferredFieldQuery, goldenRows]);

  const relatedResults = useMemo(
    () => searchState.results.filter((item) => searchResultKey(item) !== searchState.detailKey).slice(0, 5),
    [searchState.detailKey, searchState.results]
  );

  const handleRunPipeline = async () => {
    if (!result?.company_name) return;
    setRunningCompany(result.company_name);
    setError("");
    try {
      const data = await submitJob(result.company_name);
      setActivePipeline(data.task_id);
    } catch (requestError) {
      setError(`Could not queue ${result.company_name}: ${requestError.message}`);
    } finally {
      setRunningCompany("");
    }
  };

  if (!result) {
    return (
      <Card style={{ padding: 32 }}>
        <div style={{ color: T.white, fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 22 }}>
          No search result selected
        </div>
        <div style={{ color: T.muted, fontSize: 13, marginTop: 8 }}>
          Go back to the search workspace, run a query, and open a result from there.
        </div>
        <Btn onClick={() => setPage("explorer")} style={{ marginTop: 16 }}>
          Back To Search
        </Btn>
      </Card>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <style>{`
        .search-detail-layout {
          display: grid;
          grid-template-columns: minmax(0, 1.15fr) minmax(300px, 0.85fr);
          gap: 18px;
          align-items: start;
        }
        .detail-highlight-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
          gap: 10px;
        }
        @media (max-width: 1100px) {
          .search-detail-layout {
            grid-template-columns: 1fr;
          }
        }
      `}</style>

      <div style={{ display: "flex", justifyContent: "space-between", gap: 16, flexWrap: "wrap", alignItems: "flex-start" }}>
        <div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <Badge color={T.cyan}>{SEARCH_MODE_LABEL[searchState.meta?.search_mode] || "Search Detail"}</Badge>
            {searchState.meta?.endpoint && <Badge color={T.amber}>{searchState.meta.endpoint}</Badge>}
            {sourceCompany?.company_name && <Badge color={T.cyan}>Source: {sourceCompany.company_name}</Badge>}
            {searchState.meta?.backend && <Badge color={searchState.meta.backend === "pinecone" ? T.green : T.amber}>{searchState.meta.backend}</Badge>}
          </div>
          <h1 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 30, color: T.white, marginTop: 14 }}>
            {result.company_name || "Unknown company"}
          </h1>
          <p style={{ color: T.muted, fontSize: 13, marginTop: 6 }}>
            Detailed view for this search result, including stored company context and chunk-level evidence.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <Btn variant="ghost" onClick={() => setPage("explorer")}>
            Back To Search
          </Btn>
          <Btn onClick={handleRunPipeline} disabled={!apiInfo.online || runningCompany === result.company_name}>
            {runningCompany === result.company_name ? (
              <>
                <Spinner size={14} color="#000"/>
                Queueing...
              </>
            ) : "Run Pipeline"}
          </Btn>
        </div>
      </div>

      <Card
        style={{
          padding: 24,
          background: `linear-gradient(180deg, ${T.navy2} 0%, ${T.navy3} 100%)`,
        }}
      >
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
          <div style={{ maxWidth: 900 }}>
            <div style={{ color: T.muted, fontSize: 12, fontFamily: "'JetBrains Mono', monospace", overflowWrap: "anywhere" }}>
              {result.company_id || "No company_id"} {result.category ? `· ${result.category}` : ""}
            </div>
            <div style={{ color: T.white, fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 26, marginTop: 10, lineHeight: 1.2 }}>
              Why this company surfaced
            </div>
            <div style={{ color: T.text, fontSize: 14, lineHeight: 1.7, marginTop: 10, maxWidth: 860, overflowWrap: "anywhere" }}>
              {snippetDetails.focusItems.length > 0
                ? `The strongest evidence is clustering around ${result.category || "this company"} signals, led by ${snippetDetails.focusItems.join(", ")}.`
                : result.category
                  ? `This result surfaced because the backend found strong overlap inside ${result.category}.`
                  : "This result surfaced from the strongest evidence returned by the search backend."}
            </div>
            {snippetDetails.focusItems.length > 0 && (
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 14 }}>
                {snippetDetails.focusItems.map((item) => (
                  <Badge key={item} color={T.cyan}>{item}</Badge>
                ))}
              </div>
            )}
            {snippetDetails.highlightEntries.length > 0 ? (
              <div className="detail-highlight-grid" style={{ marginTop: 18 }}>
                {snippetDetails.highlightEntries.map((entry) => (
                  <InsightTile key={entry.key} label={entry.key} value={entry.value} tone={T.amber}/>
                ))}
              </div>
            ) : (
              <div style={{ color: T.text, fontSize: 14, lineHeight: 1.7, marginTop: 18, overflowWrap: "anywhere" }}>
                {result.snippet || "No snippet available from the search backend for this result."}
              </div>
            )}
          </div>
          <Badge color={apiInfo.online ? T.green : T.red}>
            <Dot color={apiInfo.online ? T.green : T.red} pulse={apiInfo.online}/>
            {apiInfo.online ? "Ready" : "Offline"}
          </Badge>
        </div>

        <div style={{ display: "flex", gap: 18, flexWrap: "wrap", marginTop: 20, paddingTop: 16, borderTop: `1px solid ${T.border}` }}>
          <DetailStat label="Score" value={typeof result.score === "number" ? result.score.toFixed(3) : "—"} color={T.amber}/>
          <DetailStat label="Max Score" value={typeof result.max_score === "number" ? result.max_score.toFixed(3) : "—"} color={T.cyan}/>
          <DetailStat label="Matches" value={result.match_count || result.top_chunks?.length || 0} color={T.green}/>
          {Array.isArray(result.shared_categories) && (
            <DetailStat label="Shared Categories" value={result.shared_categories.length} color={T.cyan}/>
          )}
          <DetailStat label="Updated" value={formatDateTime(resultUpdatedAt)} color={T.white}/>
        </div>
      </Card>

      <div className="search-detail-layout">
        <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
          <Card style={{ padding: 20 }}>
            <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
              Search Context
            </h2>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 10, marginTop: 16 }}>
              {Object.entries({
                query: searchState.meta?.query || searchState.query,
                search_mode: SEARCH_MODE_LABEL[searchState.meta?.search_mode] || searchState.mode,
                endpoint: searchState.meta?.endpoint || "—",
                backend: searchState.meta?.backend || "—",
                top_k: searchState.meta?.top_k || searchState.topK,
                searched_at: formatDateTime(searchState.meta?.date),
                source_company: sourceCompany?.company_name || "—",
              }).map(([key, value]) => (
                <div key={key} style={{ background: T.navy3, borderRadius: 10, padding: "12px 14px" }}>
                  <div style={{ fontSize: 11, color: T.muted, marginBottom: 6 }}>{humanizeKey(key)}</div>
                  <div style={{ color: T.white, fontSize: 13, lineHeight: 1.6, whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
                    {stringifyValue(value)}
                  </div>
                </div>
              ))}
            </div>
          </Card>

          {sourceCompany?.company_name && (
            <Card style={{ padding: 20 }}>
              <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
                Source Company
              </h2>
              <div style={{ background: T.navy3, borderRadius: 10, padding: "14px 16px", marginTop: 16 }}>
                <div style={{ color: T.white, fontFamily: "'Syne', sans-serif", fontWeight: 700, fontSize: 18, overflowWrap: "anywhere" }}>
                  {sourceCompany.company_name}
                </div>
                <div style={{ color: T.muted, fontSize: 12, marginTop: 6, fontFamily: "'JetBrains Mono', monospace", overflowWrap: "anywhere" }}>
                  {sourceCompany.company_id || searchState.query}
                </div>
                <div style={{ color: T.muted, fontSize: 12, lineHeight: 1.6, marginTop: 10 }}>
                  Similarity mode compares this indexed company against neighboring company vectors section by section.
                </div>
              </div>
            </Card>
          )}

          <Card style={{ padding: 20 }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
              <div>
                <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
                  Stored Company Snapshot
                </h2>
                <div style={{ color: T.muted, fontSize: 12, marginTop: 4 }}>
                  Hydrated company data returned by the backend for this result
                </div>
              </div>
              <input
                value={fieldQuery}
                onChange={(event) => setFieldQuery(event.target.value)}
                placeholder="Filter fields"
                style={{
                  width: 220,
                  maxWidth: "100%",
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

            {filteredGoldenRows.length > 0 ? (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 10, marginTop: 16 }}>
                {filteredGoldenRows.map((row) => (
                  <div key={row.rawKey} style={{ padding: "12px 14px", borderRadius: 10, background: T.navy3 }}>
                    <div style={{ fontSize: 11, color: T.muted, marginBottom: 8 }}>{row.field}</div>
                    <div style={{ color: T.white, fontSize: 13, lineHeight: 1.6, whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
                      {row.value}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div style={{ color: T.muted, fontSize: 13, marginTop: 16 }}>
                No stored company snapshot is available for this result.
              </div>
            )}
          </Card>

          <Card style={{ padding: 20 }}>
            <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
              Chunk Evidence
            </h2>
            {Array.isArray(result.top_chunks) && result.top_chunks.length > 0 ? (
              <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 16 }}>
                {result.top_chunks.map((chunk, index) => {
                  const chunkDetails = parseSnippetDetails(chunk.snippet || result.snippet);
                  return (
                    <div key={`${chunk.chunk_id || chunk.chunk_title || index}`} style={{ background: T.navy3, borderRadius: 10, padding: "14px 16px" }}>
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
                        <div style={{ color: T.white, fontWeight: 700, fontSize: 14 }}>
                          {chunk.chunk_title || `Chunk ${index + 1}`}
                        </div>
                        <div style={{ color: T.amber, fontSize: 11, fontFamily: "'JetBrains Mono', monospace" }}>
                          {typeof chunk.score === "number" ? chunk.score.toFixed(3) : "—"}
                        </div>
                      </div>
                      {chunk.source_category && (
                        <div style={{ color: T.muted, fontSize: 11, marginTop: 6, fontFamily: "'JetBrains Mono', monospace", overflowWrap: "anywhere" }}>
                          source category: {chunk.source_category}
                        </div>
                      )}

                      {chunkDetails.highlightEntries.length > 0 && (
                        <div className="detail-highlight-grid" style={{ marginTop: 12 }}>
                          {chunkDetails.highlightEntries.slice(0, 3).map((entry) => (
                            <InsightTile key={`${chunk.chunk_id || index}-${entry.key}`} label={entry.key} value={entry.value} tone={T.cyan}/>
                          ))}
                        </div>
                      )}

                      {(chunk.snippet || result.snippet) && (
                        <div style={{ color: T.text, fontSize: 12, lineHeight: 1.7, marginTop: 10, overflowWrap: "anywhere" }}>
                          {chunk.snippet || result.snippet}
                        </div>
                      )}

                      {Array.isArray(chunk.overlap_terms) && chunk.overlap_terms.length > 0 && (
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginTop: 10 }}>
                          {chunk.overlap_terms.map((term) => (
                            <span
                              key={term}
                              style={{
                                background: T.cyan + "14",
                                border: `1px solid ${T.cyan}35`,
                                color: T.cyan,
                                borderRadius: 999,
                                padding: "4px 8px",
                                fontSize: 11,
                                fontFamily: "'JetBrains Mono', monospace",
                              }}
                            >
                              {term}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            ) : (
              <div style={{ color: T.muted, fontSize: 13, marginTop: 16 }}>
                No chunk evidence was returned for this result.
              </div>
            )}
          </Card>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
          <Card style={{ padding: 20 }}>
            <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
              Stored Metadata
            </h2>
            <div style={{ display: "grid", gap: 10, marginTop: 16 }}>
              {Object.entries(storedMetadata).map(([key, value]) => (
                <div key={key} style={{ background: T.navy3, borderRadius: 10, padding: "12px 14px" }}>
                  <div style={{ fontSize: 11, color: T.muted, marginBottom: 6 }}>{humanizeKey(key)}</div>
                  <div style={{ color: T.white, fontSize: 13, lineHeight: 1.6, overflowWrap: "anywhere" }}>
                    {stringifyValue(value)}
                  </div>
                </div>
              ))}
            </div>
          </Card>

          <Card style={{ padding: 20 }}>
            <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
              Related Results
            </h2>
            {relatedResults.length > 0 ? (
              <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 16 }}>
                {relatedResults.map((item) => (
                  <button
                    key={searchResultKey(item)}
                    onClick={() => {
                      setSearchState((current) => ({ ...current, detailKey: searchResultKey(item), selectedKey: searchResultKey(item) }));
                      openSearchDetail(searchResultKey(item));
                    }}
                    style={{
                      background: T.navy3,
                      border: `1px solid ${T.border}`,
                      borderRadius: 10,
                      padding: "12px 14px",
                      textAlign: "left",
                      cursor: "pointer",
                      color: T.text,
                    }}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
                      <div>
                        <div style={{ color: T.white, fontWeight: 700, fontSize: 14, fontFamily: "'Syne', sans-serif", overflowWrap: "anywhere" }}>
                          {item.company_name}
                        </div>
                        <div style={{ color: T.muted, fontSize: 11, marginTop: 6, fontFamily: "'JetBrains Mono', monospace", overflowWrap: "anywhere" }}>
                          {item.company_id || "no-company-id"}
                        </div>
                      </div>
                      <div style={{ color: T.amber, fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 20 }}>
                        {typeof item.score === "number" ? item.score.toFixed(3) : "—"}
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            ) : (
              <div style={{ color: T.muted, fontSize: 13, marginTop: 16 }}>
                No related results from this search session.
              </div>
            )}
          </Card>

          {error && (
            <Card style={{ padding: 16, borderColor: `${T.red}35` }}>
              <div style={{ color: T.red, fontSize: 12, fontFamily: "'JetBrains Mono', monospace", lineHeight: 1.6 }}>
                {error}
              </div>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}

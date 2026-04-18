import { startTransition, useMemo, useState } from "react";
import { Badge, Btn, Card, Dot, Spinner } from "../components";
import { T } from "../theme";
import { getInnovationClusters, getPredictiveAnalytics } from "../api";
import AnalyticsMetricCard from "../components/insights/AnalyticsMetricCard";
import SectionCard from "../components/insights/SectionCard";
import ClusterCard from "../components/insights/ClusterCard";
import CompanyTable from "../components/insights/CompanyTable";
import { ClusterScatterChart } from "../components/insights/charts";
import {
  buildPredictiveLookup,
  clusterColor,
  formatScore,
  parseNameList,
  predictiveMetaForCompany,
  primaryIndustry,
  toNumber,
} from "../components/insights/utils";

const DEFAULT_FORM = {
  companyNames: "",
  limit: 24,
  algorithm: "auto",
  reduction: "auto",
  nClusters: "",
  minClusterSize: 2,
  includeNoise: false,
};

function DistributionList({ rows, emptyMessage }) {
  if (!rows.length) {
    return <div style={{ color: T.muted, fontSize: 13 }}>{emptyMessage}</div>;
  }

  const maximum = Math.max(...rows.map((row) => row.value), 1);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      {rows.map((row) => (
        <div key={row.label}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 10, marginBottom: 6 }}>
            <div style={{ color: T.text, fontSize: 13 }}>{row.label}</div>
            <div style={{ color: T.white, fontSize: 12 }}>{row.value}</div>
          </div>
          <div style={{ height: 8, borderRadius: 999, background: T.navy3 }}>
            <div
              style={{
                width: `${(row.value / maximum) * 100}%`,
                height: "100%",
                borderRadius: 999,
                background: row.color || T.cyan,
              }}
            />
          </div>
        </div>
      ))}
    </div>
  );
}

export default function InnovationClustersPage({ apiInfo }) {
  const [form, setForm] = useState(DEFAULT_FORM);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [warning, setWarning] = useState("");
  const [clusterData, setClusterData] = useState(null);
  const [predictiveData, setPredictiveData] = useState(null);
  const [selectedClusterId, setSelectedClusterId] = useState(null);
  const [industryFilter, setIndustryFilter] = useState("all");
  const [domainFilter, setDomainFilter] = useState("all");

  const predictiveLookup = useMemo(
    () => buildPredictiveLookup(predictiveData?.predictions || []),
    [predictiveData]
  );

  const clusters = useMemo(() => clusterData?.clusters || [], [clusterData]);
  const clusterLabels = useMemo(
    () => Object.fromEntries(clusters.map((cluster) => [Number(cluster.cluster_id), cluster.label || `Cluster ${Number(cluster.cluster_id) + 1}`])),
    [clusters]
  );

  const selectedCluster = useMemo(() => {
    if (!clusters.length) return null;
    return clusters.find((cluster) => Number(cluster.cluster_id) === Number(selectedClusterId)) || clusters[0];
  }, [clusters, selectedClusterId]);

  const companyRows = useMemo(
    () => clusters.flatMap((cluster) => (
      cluster.members || []
    ).map((member) => {
      const predictiveMeta = predictiveMetaForCompany(predictiveLookup, member);
      return {
        company_id: member.company_id,
        company_name: member.company_name,
        industry: primaryIndustry(predictiveMeta),
        sector: predictiveMeta?.sector || "Unknown",
        cluster_id: Number(cluster.cluster_id),
        cluster_label: cluster.label,
        similarity_score: Number(member.similarity_to_centroid) || 0,
        vector_source: member.vector_source || predictiveMeta?.model_input_summary?.vector_source || "unknown",
        dominant_categories: Array.isArray(member.dominant_categories) ? member.dominant_categories : [],
      };
    })),
    [clusters, predictiveLookup]
  );

  const selectedClusterRows = useMemo(
    () => companyRows.filter((row) => selectedCluster && row.cluster_id === Number(selectedCluster.cluster_id)),
    [companyRows, selectedCluster]
  );

  const industryOptions = useMemo(
    () => [...new Set(selectedClusterRows.map((row) => row.industry).filter(Boolean))].sort((left, right) => left.localeCompare(right)),
    [selectedClusterRows]
  );

  const domainOptions = useMemo(
    () => [...new Set(selectedClusterRows.flatMap((row) => row.dominant_categories || []).filter(Boolean))].sort((left, right) => left.localeCompare(right)),
    [selectedClusterRows]
  );

  const filteredRows = useMemo(
    () => selectedClusterRows.filter((row) => {
      if (industryFilter !== "all" && row.industry !== industryFilter) return false;
      if (domainFilter !== "all" && !(row.dominant_categories || []).includes(domainFilter)) return false;
      return true;
    }),
    [domainFilter, industryFilter, selectedClusterRows]
  );

  const industryDistribution = useMemo(() => {
    const counts = new Map();
    selectedClusterRows.forEach((row) => {
      counts.set(row.industry, (counts.get(row.industry) || 0) + 1);
    });
    return [...counts.entries()]
      .map(([label, value]) => ({ label, value, color: T.cyan }))
      .sort((left, right) => right.value - left.value)
      .slice(0, 5);
  }, [selectedClusterRows]);

  const largestCluster = useMemo(
    () => clusters.reduce((largest, cluster) => (cluster.size > (largest?.size || 0) ? cluster : largest), null),
    [clusters]
  );

  const averageCohesion = useMemo(() => {
    if (!clusters.length) return "—";
    const total = clusters.reduce((sum, cluster) => sum + (Number(cluster.cohesion_score) || 0), 0);
    return formatScore(total / clusters.length);
  }, [clusters]);

  const handleRun = async () => {
    setLoading(true);
    setError("");
    setWarning("");

    const companyNames = parseNameList(form.companyNames);
    const limit = companyNames.length ? null : Math.max(4, toNumber(form.limit, 24));
    const nClusters = form.nClusters === "" ? null : Math.max(1, toNumber(form.nClusters, 0));

    try {
      const [clusterResult, predictiveResult] = await Promise.allSettled([
        getInnovationClusters({
          company_names: companyNames.length ? companyNames : null,
          limit,
          algorithm: form.algorithm,
          reduction: form.reduction,
          n_clusters: nClusters,
          min_cluster_size: Math.max(2, toNumber(form.minClusterSize, 2)),
          include_noise: form.includeNoise,
        }),
        getPredictiveAnalytics({
          company_names: companyNames.length ? companyNames : null,
          limit,
          top_n: Math.max(6, Math.min(10, limit || companyNames.length || 6)),
        }),
      ]);

      if (clusterResult.status !== "fulfilled") {
        throw clusterResult.reason;
      }

      startTransition(() => {
        setClusterData(clusterResult.value);
        setPredictiveData(predictiveResult.status === "fulfilled" ? predictiveResult.value : null);
        setSelectedClusterId(
          clusterResult.value.clusters?.find((cluster) => Number(cluster.cluster_id) !== -1)?.cluster_id
            ?? clusterResult.value.clusters?.[0]?.cluster_id
            ?? null
        );
        setIndustryFilter("all");
        setDomainFilter("all");

        if (predictiveResult.status === "rejected") {
          setWarning(`Clusters loaded, but industry enrichment could not be computed: ${predictiveResult.reason?.message || "Unknown error"}`);
        }
      });
    } catch (requestError) {
      setClusterData(null);
      setPredictiveData(null);
      setError(`Cluster analysis failed: ${requestError.message}`);
    } finally {
      setLoading(false);
    }
  };

  const tableColumns = [
    {
      key: "company_name",
      label: "Company",
      defaultDirection: "asc",
      render: (row) => (
        <div>
          <div style={{ color: T.white, fontFamily: "'Syne', sans-serif", fontWeight: 700, fontSize: 14 }}>
            {row.company_name}
          </div>
          <div style={{ color: T.muted, fontSize: 11, marginTop: 5, fontFamily: "'JetBrains Mono', monospace" }}>
            {row.company_id}
          </div>
        </div>
      ),
    },
    {
      key: "industry",
      label: "Industry",
      defaultDirection: "asc",
      render: (row) => (
        <div style={{ color: T.text, fontSize: 13, lineHeight: 1.5 }}>
          {row.industry}
          <div style={{ color: T.muted, fontSize: 11, marginTop: 4 }}>{row.sector}</div>
        </div>
      ),
    },
    {
      key: "cluster_label",
      label: "Cluster",
      defaultDirection: "asc",
      render: (row) => (
        <div style={{ color: clusterColor(row.cluster_id), fontSize: 13, fontWeight: 600 }}>
          {row.cluster_label}
        </div>
      ),
    },
    {
      key: "similarity_score",
      label: "Similarity",
      align: "right",
      value: (row) => row.similarity_score,
      render: (row) => <span style={{ color: T.amber }}>{formatScore(row.similarity_score)}</span>,
    },
    {
      key: "dominant_categories",
      label: "Innovation Focus",
      sortable: false,
      render: (row) => (
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
          {(row.dominant_categories || []).slice(0, 3).map((category) => (
            <Badge key={category} color={T.cyan}>{category}</Badge>
          ))}
        </div>
      ),
    },
  ];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <style>{`
        .clusters-controls {
          display: grid;
          grid-template-columns: minmax(0, 1.4fr) repeat(4, minmax(120px, 0.45fr)) auto;
          gap: 12px;
          align-items: end;
        }
        .clusters-grid {
          display: grid;
          grid-template-columns: minmax(0, 1.15fr) minmax(320px, 0.85fr);
          gap: 18px;
          align-items: start;
        }
        .clusters-overview {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
          gap: 12px;
        }
        @media (max-width: 1180px) {
          .clusters-grid {
            grid-template-columns: 1fr;
          }
        }
        @media (max-width: 1080px) {
          .clusters-controls {
            grid-template-columns: 1fr 1fr;
          }
        }
        @media (max-width: 720px) {
          .clusters-controls {
            grid-template-columns: 1fr;
          }
        }
      `}</style>

      <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "flex-start", flexWrap: "wrap" }}>
        <div>
          <h1 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 28, color: T.white }}>
            Innovation Clusters
          </h1>
          <p style={{ color: T.muted, fontSize: 13, marginTop: 6, lineHeight: 1.7, maxWidth: 760 }}>
            Explore company groups formed by shared innovation patterns, inspect the cluster map, and drill into the companies that sit closest to each cluster center.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <Badge color={apiInfo.online ? T.green : T.red}>
            <Dot color={apiInfo.online ? T.green : T.red} pulse={apiInfo.online}/>
            {apiInfo.online ? "ML Routes Ready" : "Backend Offline"}
          </Badge>
          <Badge color={T.cyan}>/ml/innovation-clusters</Badge>
          <Badge color={T.amber}>Predictive Enrichment</Badge>
        </div>
      </div>

      <Card style={{ padding: 24 }}>
        <div className="clusters-controls">
          <div style={{ minWidth: 0 }}>
            <label style={{ display: "block", fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 8 }}>
              Company Names
            </label>
            <textarea
              value={form.companyNames}
              onChange={(event) => setForm((current) => ({ ...current, companyNames: event.target.value }))}
              placeholder="Optional: Apple, NVIDIA, Tesla"
              rows={3}
              style={{
                width: "100%",
                resize: "vertical",
                background: T.navy3,
                border: `1px solid ${T.border}`,
                borderRadius: 12,
                color: T.white,
                padding: "12px 14px",
                fontSize: 13,
                outline: "none",
                lineHeight: 1.6,
              }}
            />
          </div>

          <div>
            <label style={{ display: "block", fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 8 }}>
              Limit
            </label>
            <input
              type="number"
              min="4"
              value={form.limit}
              onChange={(event) => setForm((current) => ({ ...current, limit: event.target.value }))}
              style={{
                width: "100%",
                background: T.navy3,
                border: `1px solid ${T.border}`,
                borderRadius: 10,
                color: T.white,
                padding: "12px 14px",
                fontSize: 14,
                outline: "none",
              }}
            />
          </div>

          <div>
            <label style={{ display: "block", fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 8 }}>
              Algorithm
            </label>
            <select
              value={form.algorithm}
              onChange={(event) => setForm((current) => ({ ...current, algorithm: event.target.value }))}
              style={{
                width: "100%",
                background: T.navy3,
                border: `1px solid ${T.border}`,
                borderRadius: 10,
                color: T.white,
                padding: "12px 14px",
                fontSize: 14,
                outline: "none",
              }}
            >
              {["auto", "kmeans", "dbscan", "hdbscan"].map((value) => (
                <option key={value} value={value}>{value}</option>
              ))}
            </select>
          </div>

          <div>
            <label style={{ display: "block", fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 8 }}>
              Reduction
            </label>
            <select
              value={form.reduction}
              onChange={(event) => setForm((current) => ({ ...current, reduction: event.target.value }))}
              style={{
                width: "100%",
                background: T.navy3,
                border: `1px solid ${T.border}`,
                borderRadius: 10,
                color: T.white,
                padding: "12px 14px",
                fontSize: 14,
                outline: "none",
              }}
            >
              {["auto", "none", "pca", "svd", "umap"].map((value) => (
                <option key={value} value={value}>{value}</option>
              ))}
            </select>
          </div>

          <div>
            <label style={{ display: "block", fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 8 }}>
              Min Size
            </label>
            <input
              type="number"
              min="2"
              value={form.minClusterSize}
              onChange={(event) => setForm((current) => ({ ...current, minClusterSize: event.target.value }))}
              style={{
                width: "100%",
                background: T.navy3,
                border: `1px solid ${T.border}`,
                borderRadius: 10,
                color: T.white,
                padding: "12px 14px",
                fontSize: 14,
                outline: "none",
              }}
            />
          </div>

          <Btn onClick={handleRun} disabled={!apiInfo.online || loading} style={{ minWidth: 180, padding: "12px 22px", fontSize: 14 }}>
            {loading ? (
              <>
                <Spinner size={14} color="#000"/>
                Running Clusters...
              </>
            ) : "Run Clustering"}
          </Btn>
        </div>

        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginTop: 14, flexWrap: "wrap", alignItems: "center" }}>
          <label style={{ display: "inline-flex", alignItems: "center", gap: 8, color: T.text, fontSize: 13 }}>
            <input
              type="checkbox"
              checked={form.includeNoise}
              onChange={(event) => setForm((current) => ({ ...current, includeNoise: event.target.checked }))}
            />
            Include noise cluster if the backend emits one
          </label>
          <div style={{ color: T.muted, fontSize: 12, lineHeight: 1.6 }}>
            Leave company names blank to cluster the indexed population using the selected limit.
          </div>
        </div>

        {error && (
          <div style={{ marginTop: 16, background: `${T.red}12`, border: `1px solid ${T.red}35`, borderRadius: 10, padding: "12px 14px", color: T.red, fontSize: 13 }}>
            {error}
          </div>
        )}
        {warning && (
          <div style={{ marginTop: 16, background: `${T.amber}12`, border: `1px solid ${T.amber}35`, borderRadius: 10, padding: "12px 14px", color: T.amber, fontSize: 13 }}>
            {warning}
          </div>
        )}
      </Card>

      {!clusterData ? (
        <Card style={{ padding: 28 }}>
          <div style={{ color: T.white, fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 22 }}>
            Run a clustering job to map the innovation landscape
          </div>
          <div style={{ color: T.muted, fontSize: 13, marginTop: 8, lineHeight: 1.7, maxWidth: 760 }}>
            This page keeps the existing search workflow untouched and adds a separate cluster analysis surface for innovation themes, cluster cohesion, and company-level drilldowns.
          </div>
        </Card>
      ) : (
        <>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 14 }}>
            <AnalyticsMetricCard
              label="Companies"
              value={clusterData.company_count || 0}
              subtitle="Companies included in the clustering run"
              tone={T.amber}
            />
            <AnalyticsMetricCard
              label="Clusters"
              value={clusterData.cluster_count || 0}
              subtitle="Non-noise groups identified by the backend"
              tone={T.cyan}
            />
            <AnalyticsMetricCard
              label="Largest Cluster"
              value={largestCluster ? largestCluster.size : "—"}
              subtitle={largestCluster ? largestCluster.label : "No cluster selected"}
              tone={T.green}
            />
            <AnalyticsMetricCard
              label="Average Cohesion"
              value={averageCohesion}
              subtitle="Mean similarity-to-centroid across returned clusters"
              tone={T.white}
            />
            <AnalyticsMetricCard
              label="Noise Points"
              value={clusterData.noise_count || 0}
              subtitle={clusterData.algorithm?.effective_algorithm || "Algorithm not reported"}
              tone={(clusterData.noise_count || 0) > 0 ? T.amber : T.cyan}
            />
          </div>

          <SectionCard
            title="Cluster Overview"
            subtitle="Click any cluster to sync the map, detail panel, and company table."
          >
            <div className="clusters-overview">
              {clusters.map((cluster) => (
                <ClusterCard
                  key={cluster.cluster_id}
                  cluster={cluster}
                  active={selectedCluster && Number(selectedCluster.cluster_id) === Number(cluster.cluster_id)}
                  onSelect={setSelectedClusterId}
                />
              ))}
            </div>
          </SectionCard>

          <div className="clusters-grid">
            <SectionCard
              title="Cluster Visualization"
              subtitle="2D projection of company vectors. Selecting a point or legend item focuses the corresponding cluster."
            >
              <ClusterScatterChart
                points={clusterData.points || []}
                clusterLabels={clusterLabels}
                selectedClusterId={selectedCluster ? Number(selectedCluster.cluster_id) : null}
                onSelect={setSelectedClusterId}
              />
            </SectionCard>

            <SectionCard
              title={selectedCluster ? selectedCluster.label : "Cluster Detail"}
              subtitle={selectedCluster ? "Inspect cohesion, leading themes, industries, and the companies nearest the cluster center." : "Select a cluster to inspect it."}
            >
              {selectedCluster ? (
                <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                    <Badge color={clusterColor(selectedCluster.cluster_id)}>
                      Cluster {Number(selectedCluster.cluster_id) >= 0 ? Number(selectedCluster.cluster_id) + 1 : "Noise"}
                    </Badge>
                    <Badge color={T.cyan}>{selectedCluster.size} members</Badge>
                    <Badge color={T.green}>Cohesion {formatScore(selectedCluster.cohesion_score)}</Badge>
                  </div>

                  <div>
                    <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 8 }}>
                      Innovation Focus
                    </div>
                    <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                      {(selectedCluster.top_terms || []).map((term) => (
                        <Badge key={term} color={T.amber}>{term}</Badge>
                      ))}
                      {(selectedCluster.top_categories || []).map((category) => (
                        <Badge key={category} color={T.cyan}>{category}</Badge>
                      ))}
                    </div>
                  </div>

                  <div>
                    <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 10 }}>
                      Industry Distribution
                    </div>
                    <DistributionList rows={industryDistribution} emptyMessage="Industry metadata is not available for this cluster." />
                  </div>

                  <div>
                    <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 10 }}>
                      Closest Companies
                    </div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                      {(selectedCluster.members || []).slice(0, 6).map((member) => {
                        const predictiveMeta = predictiveMetaForCompany(predictiveLookup, member);
                        return (
                          <div key={member.company_id} style={{ background: T.navy3, borderRadius: 10, padding: "12px 14px" }}>
                            <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
                              <div>
                                <div style={{ color: T.white, fontFamily: "'Syne', sans-serif", fontWeight: 700, fontSize: 14 }}>
                                  {member.company_name}
                                </div>
                                <div style={{ color: T.muted, fontSize: 12, marginTop: 5 }}>
                                  {primaryIndustry(predictiveMeta)}
                                </div>
                              </div>
                              <div style={{ color: T.amber, fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 20 }}>
                                {formatScore(member.similarity_to_centroid)}
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </div>
              ) : (
                <div style={{ color: T.muted, fontSize: 13 }}>Select a cluster card to inspect it.</div>
              )}
            </SectionCard>
          </div>

          <SectionCard
            title="Company Table"
            subtitle="The table is filtered to the selected cluster and can be narrowed by industry or innovation domain."
            action={(
              <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                <select
                  value={industryFilter}
                  onChange={(event) => setIndustryFilter(event.target.value)}
                  style={{
                    background: T.navy3,
                    border: `1px solid ${T.border}`,
                    borderRadius: 10,
                    color: T.white,
                    padding: "10px 12px",
                    fontSize: 13,
                    outline: "none",
                  }}
                >
                  <option value="all">All industries</option>
                  {industryOptions.map((industry) => (
                    <option key={industry} value={industry}>{industry}</option>
                  ))}
                </select>
                <select
                  value={domainFilter}
                  onChange={(event) => setDomainFilter(event.target.value)}
                  style={{
                    background: T.navy3,
                    border: `1px solid ${T.border}`,
                    borderRadius: 10,
                    color: T.white,
                    padding: "10px 12px",
                    fontSize: 13,
                    outline: "none",
                  }}
                >
                  <option value="all">All innovation domains</option>
                  {domainOptions.map((domain) => (
                    <option key={domain} value={domain}>{domain}</option>
                  ))}
                </select>
              </div>
            )}
          >
            <CompanyTable
              rows={filteredRows}
              columns={tableColumns}
              initialSortKey="similarity_score"
              initialSortDirection="desc"
              emptyMessage="No companies match the active filters for this cluster."
            />
          </SectionCard>
        </>
      )}
    </div>
  );
}

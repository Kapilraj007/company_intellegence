import { startTransition, useMemo, useState } from "react";
import { Badge, Btn, Card, Dot, Spinner } from "../components";
import { T } from "../theme";
import { getDescriptiveAnalytics, getInnovationClusters, getPredictiveAnalytics } from "../api";
import AnalyticsMetricCard from "../components/insights/AnalyticsMetricCard";
import SectionCard from "../components/insights/SectionCard";
import CompanyTable from "../components/insights/CompanyTable";
import { BarChart, DonutChart, HeatmapChart, LineChart } from "../components/insights/charts";
import { average, buildHeatmapModel, formatPercent, parseNameList, primaryIndustry, toNumber } from "../components/insights/utils";

const DEFAULT_FORM = {
  companyNames: "",
  limit: 40,
  topN: 6,
  minTrainingSamples: 6,
};

function LeaderCard({ title, subtitle, rows, metricKey, metricFormatter = (value) => value }) {
  return (
    <Card style={{ padding: 18 }}>
      <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 17, color: T.white }}>
        {title}
      </div>
      <div style={{ color: T.muted, fontSize: 12, marginTop: 5, lineHeight: 1.6 }}>
        {subtitle}
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 16 }}>
        {(rows || []).slice(0, 5).map((row) => (
          <div key={row.company_id || row.company_name} style={{ background: T.navy3, borderRadius: 10, padding: "12px 14px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
              <div>
                <div style={{ color: T.white, fontWeight: 700, fontSize: 14 }}>{row.company_name}</div>
                <div style={{ color: T.muted, fontSize: 12, marginTop: 4 }}>{primaryIndustry(row)}</div>
              </div>
              <div style={{ color: T.amber, fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18 }}>
                {metricFormatter(row[metricKey])}
              </div>
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
}

export default function AnalyticsDashboardPage({ apiInfo }) {
  const [form, setForm] = useState(DEFAULT_FORM);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [warning, setWarning] = useState("");
  const [descriptive, setDescriptive] = useState(null);
  const [predictive, setPredictive] = useState(null);
  const [clusters, setClusters] = useState(null);

  const heatmapModel = useMemo(
    () => buildHeatmapModel(
      descriptive?.technology_adoption_patterns?.industry_patterns || [],
      descriptive?.technology_adoption_patterns?.top_technologies || []
    ),
    [descriptive]
  );

  const averageInnovationScore = useMemo(() => {
    const predictions = predictive?.predictions || [];
    if (!predictions.length) return "—";
    return formatPercent(average(predictions.map((row) => row.composite_prediction_score)), 1);
  }, [predictive]);

  const clusterDistribution = useMemo(
    () => (clusters?.clusters || [])
      .filter((cluster) => Number(cluster.cluster_id) !== -1)
      .map((cluster) => ({ label: cluster.label, value: cluster.size })),
    [clusters]
  );

  const modelSummary = useMemo(() => {
    if (!predictive?.models) return [];
    return [
      {
        label: "Growth",
        value: predictive.models.growth?.training_mode || "—",
        details: (predictive.models.growth?.models_used || []).join(", "),
      },
      {
        label: "Technology",
        value: predictive.models.technology_leadership?.training_mode || "—",
        details: (predictive.models.technology_leadership?.models_used || []).join(", "),
      },
      {
        label: "Expansion",
        value: predictive.models.market_expansion?.training_mode || "—",
        details: (predictive.models.market_expansion?.models_used || []).join(", "),
      },
    ];
  }, [predictive]);

  const handleRun = async () => {
    setLoading(true);
    setError("");
    setWarning("");

    const companyNames = parseNameList(form.companyNames);
    const limit = companyNames.length ? null : Math.max(6, toNumber(form.limit, 40));
    const topN = Math.max(3, toNumber(form.topN, 6));
    const minTrainingSamples = Math.max(3, toNumber(form.minTrainingSamples, 6));

    try {
      const [descriptiveResult, predictiveResult, clusterResult] = await Promise.allSettled([
        getDescriptiveAnalytics({
          company_names: companyNames.length ? companyNames : null,
          limit,
          top_n: topN,
        }),
        getPredictiveAnalytics({
          company_names: companyNames.length ? companyNames : null,
          limit,
          top_n: topN,
          min_training_samples: minTrainingSamples,
        }),
        getInnovationClusters({
          company_names: companyNames.length ? companyNames : null,
          limit,
          min_cluster_size: 2,
        }),
      ]);

      if (descriptiveResult.status !== "fulfilled" && predictiveResult.status !== "fulfilled" && clusterResult.status !== "fulfilled") {
        throw descriptiveResult.reason || predictiveResult.reason || clusterResult.reason;
      }

      startTransition(() => {
        setDescriptive(descriptiveResult.status === "fulfilled" ? descriptiveResult.value : null);
        setPredictive(predictiveResult.status === "fulfilled" ? predictiveResult.value : null);
        setClusters(clusterResult.status === "fulfilled" ? clusterResult.value : null);

        const warnings = [];
        if (descriptiveResult.status === "rejected") warnings.push(`descriptive analytics: ${descriptiveResult.reason?.message || "failed"}`);
        if (predictiveResult.status === "rejected") warnings.push(`predictive analytics: ${predictiveResult.reason?.message || "failed"}`);
        if (clusterResult.status === "rejected") warnings.push(`innovation clusters: ${clusterResult.reason?.message || "failed"}`);
        setWarning(warnings.length ? `Partial dashboard load: ${warnings.join(" | ")}` : "");
      });
    } catch (requestError) {
      setDescriptive(null);
      setPredictive(null);
      setClusters(null);
      setError(`Analytics dashboard failed: ${requestError.message}`);
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
          <div style={{ color: T.muted, fontSize: 11, marginTop: 5 }}>
            {primaryIndustry(row)}
          </div>
        </div>
      ),
    },
    {
      key: "growth_prediction_score",
      label: "Growth",
      align: "right",
      render: (row) => <span style={{ color: T.green }}>{formatPercent(row.growth_prediction_score, 1)}</span>,
      value: (row) => row.growth_prediction_score,
    },
    {
      key: "technology_leadership_score",
      label: "Tech Leadership",
      align: "right",
      render: (row) => <span style={{ color: T.cyan }}>{formatPercent(row.technology_leadership_score, 1)}</span>,
      value: (row) => row.technology_leadership_score,
    },
    {
      key: "market_expansion_potential",
      label: "Expansion",
      align: "right",
      render: (row) => <span style={{ color: T.amber }}>{formatPercent(row.market_expansion_potential, 1)}</span>,
      value: (row) => row.market_expansion_potential,
    },
    {
      key: "composite_prediction_score",
      label: "Composite",
      align: "right",
      render: (row) => <span style={{ color: T.white, fontWeight: 700 }}>{formatPercent(row.composite_prediction_score, 1)}</span>,
      value: (row) => row.composite_prediction_score,
    },
  ];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <style>{`
        .analytics-controls {
          display: grid;
          grid-template-columns: minmax(0, 1.45fr) repeat(3, minmax(130px, 0.4fr)) auto;
          gap: 12px;
          align-items: end;
        }
        .analytics-grid {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 18px;
          align-items: start;
        }
        .analytics-leaders {
          display: grid;
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap: 18px;
        }
        @media (max-width: 1120px) {
          .analytics-grid,
          .analytics-leaders {
            grid-template-columns: 1fr;
          }
        }
        @media (max-width: 980px) {
          .analytics-controls {
            grid-template-columns: 1fr 1fr;
          }
        }
        @media (max-width: 720px) {
          .analytics-controls {
            grid-template-columns: 1fr;
          }
        }
      `}</style>

      <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "flex-start", flexWrap: "wrap" }}>
        <div>
          <h1 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 28, color: T.white }}>
            Analytics Dashboard
          </h1>
          <p style={{ color: T.muted, fontSize: 13, marginTop: 6, lineHeight: 1.7, maxWidth: 760 }}>
            Combine descriptive analytics, predictive scoring, and cluster structure into one analyst-facing dashboard for innovation, technology adoption, and market momentum.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <Badge color={apiInfo.online ? T.green : T.red}>
            <Dot color={apiInfo.online ? T.green : T.red} pulse={apiInfo.online}/>
            {apiInfo.online ? "Analytics Ready" : "Backend Offline"}
          </Badge>
          <Badge color={T.cyan}>/ml/analytics/descriptive</Badge>
          <Badge color={T.amber}>/ml/analytics/predictive</Badge>
        </div>
      </div>

      <Card style={{ padding: 24 }}>
        <div className="analytics-controls">
          <div style={{ minWidth: 0 }}>
            <label style={{ display: "block", fontSize: 12, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 8 }}>
              Company Names
            </label>
            <textarea
              value={form.companyNames}
              onChange={(event) => setForm((current) => ({ ...current, companyNames: event.target.value }))}
              placeholder="Optional: Stripe, Apple, NVIDIA"
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
              min="6"
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
              Top N
            </label>
            <input
              type="number"
              min="3"
              value={form.topN}
              onChange={(event) => setForm((current) => ({ ...current, topN: event.target.value }))}
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
              Min Samples
            </label>
            <input
              type="number"
              min="3"
              value={form.minTrainingSamples}
              onChange={(event) => setForm((current) => ({ ...current, minTrainingSamples: event.target.value }))}
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
                Building Dashboard...
              </>
            ) : "Run Analytics"}
          </Btn>
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

      {!descriptive && !predictive && !clusters ? (
        <Card style={{ padding: 28 }}>
          <div style={{ color: T.white, fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 22 }}>
            Run the analytics suite to generate dashboard insights
          </div>
          <div style={{ color: T.muted, fontSize: 13, marginTop: 8, lineHeight: 1.7, maxWidth: 780 }}>
            This view is isolated from the search workflow and combines the three backend ML capabilities into one dashboard: descriptive trends, predictive company scores, and cluster distribution.
          </div>
        </Card>
      ) : (
        <>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 14 }}>
            <AnalyticsMetricCard
              label="Total Companies"
              value={predictive?.company_count || descriptive?.company_count || clusters?.company_count || "—"}
              subtitle="Companies included in this analytics run"
              tone={T.amber}
            />
            <AnalyticsMetricCard
              label="Total Clusters"
              value={clusters?.cluster_count ?? "—"}
              subtitle="Innovation groups found in the same population"
              tone={T.cyan}
            />
            <AnalyticsMetricCard
              label="Most Innovative Industry"
              value={descriptive?.most_innovative_industries?.[0]?.industry || "—"}
              subtitle="Top industry by innovation intensity score"
              tone={T.green}
            />
            <AnalyticsMetricCard
              label="Average Innovation Score"
              value={averageInnovationScore}
              subtitle="Mean composite prediction score across returned companies"
              tone={T.white}
            />
            <AnalyticsMetricCard
              label="Top AI Industry"
              value={descriptive?.top_ai_adopting_industries?.[0]?.industry || "—"}
              subtitle="Strongest AI readiness signal in the current run"
              tone={T.cyan}
            />
            <AnalyticsMetricCard
              label="Training Samples"
              value={predictive?.training_sample_count ?? "—"}
              subtitle="Rows used by the predictive ensemble"
              tone={T.amber}
            />
          </div>

          <div className="analytics-grid">
            <SectionCard
              title="Industry Innovation Distribution"
              subtitle="Most innovative industries ranked by support-adjusted innovation intensity."
            >
              <BarChart
                data={descriptive?.most_innovative_industries || []}
                labelKey="industry"
                valueKey="score"
                color={T.cyan}
                valueFormatter={(value) => value.toFixed(2)}
              />
            </SectionCard>

            <SectionCard
              title="Technology Adoption Trends"
              subtitle="Top technology signals ranked by blended adoption and innovation strength."
            >
              <LineChart
                data={descriptive?.technology_adoption_patterns?.top_technologies || []}
                labelKey="technology"
                valueKey="score"
                color={T.amber}
                valueFormatter={(value) => value.toFixed(2)}
              />
            </SectionCard>

            <SectionCard
              title="Cluster Distribution"
              subtitle="Company volume across the innovation clusters returned in the same run."
            >
              <DonutChart
                data={clusterDistribution}
                labelKey="label"
                valueKey="value"
                centerLabel="Clusters"
              />
            </SectionCard>

            <SectionCard
              title="Model Signals"
              subtitle="Training mode and ensemble composition for each predictive target."
            >
              <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                {modelSummary.map((item) => (
                  <div key={item.label} style={{ background: T.navy3, borderRadius: 12, padding: "14px 16px" }}>
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
                      <div style={{ color: T.white, fontFamily: "'Syne', sans-serif", fontWeight: 700, fontSize: 16 }}>
                        {item.label}
                      </div>
                      <Badge color={item.value.includes("ensemble") ? T.green : T.amber}>{item.value}</Badge>
                    </div>
                    <div style={{ color: T.muted, fontSize: 12, lineHeight: 1.6, marginTop: 8 }}>
                      {item.details || "No model details returned."}
                    </div>
                  </div>
                ))}
              </div>
            </SectionCard>
          </div>

          <SectionCard
            title="Innovation Heatmap"
            subtitle="Industry-to-technology signal matrix built from the descriptive analytics pattern output."
          >
            <HeatmapChart rows={heatmapModel.rows} columns={heatmapModel.columns} />
          </SectionCard>

          <div className="analytics-leaders">
            <LeaderCard
              title="High Growth Startups"
              subtitle="Companies with the strongest growth prediction score."
              rows={predictive?.high_growth_startups || []}
              metricKey="growth_prediction_score"
              metricFormatter={(value) => formatPercent(value, 1)}
            />
            <LeaderCard
              title="Future Tech Leaders"
              subtitle="Companies projected to lead technology adoption."
              rows={predictive?.future_technology_leaders || []}
              metricKey="technology_leadership_score"
              metricFormatter={(value) => formatPercent(value, 1)}
            />
            <LeaderCard
              title="Expansion Candidates"
              subtitle="Companies with the highest market expansion potential."
              rows={predictive?.market_expansion_candidates || []}
              metricKey="market_expansion_potential"
              metricFormatter={(value) => formatPercent(value, 1)}
            />
          </div>

          <SectionCard
            title="Top Emerging Companies"
            subtitle="Sortable view of the predictive company ranking for analysts reviewing near-term momentum and innovation leadership."
          >
            <CompanyTable
              rows={predictive?.predictions || []}
              columns={tableColumns}
              initialSortKey="composite_prediction_score"
              initialSortDirection="desc"
              emptyMessage="No predictive company rows were returned."
            />
          </SectionCard>
        </>
      )}
    </div>
  );
}

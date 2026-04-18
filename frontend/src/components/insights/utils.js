import { T } from "../../theme";

const CLUSTER_PALETTE = [
  T.amber,
  T.cyan,
  T.green,
  "#7dc4ff",
  "#f97316",
  "#6ee7b7",
  "#f472b6",
  "#a3e635",
];

export function parseNameList(value) {
  return String(value || "")
    .split(/[\n,]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

export function toNumber(value, fallback = 0) {
  const next = Number(value);
  return Number.isFinite(next) ? next : fallback;
}

export function average(values) {
  const numeric = values.map((value) => Number(value)).filter((value) => Number.isFinite(value));
  if (!numeric.length) return 0;
  return numeric.reduce((sum, value) => sum + value, 0) / numeric.length;
}

export function formatPercent(value, digits = 1) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "—";
  return `${numeric.toFixed(digits)}%`;
}

export function formatScore(value, digits = 3) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "—";
  return numeric.toFixed(digits);
}

export function truncateLabel(value, maxLength = 18) {
  const text = String(value || "");
  if (text.length <= maxLength) return text;
  return `${text.slice(0, Math.max(3, maxLength - 3))}...`;
}

export function clusterColor(clusterId) {
  if (Number(clusterId) === -1) return T.muted;
  return CLUSTER_PALETTE[Math.abs(Number(clusterId) || 0) % CLUSTER_PALETTE.length];
}

export function buildPredictiveLookup(predictions = []) {
  const map = new Map();
  predictions.forEach((row) => {
    const companyId = String(row?.company_id || "").trim();
    const companyName = String(row?.company_name || "").trim().toLowerCase();
    if (companyId) map.set(companyId, row);
    if (companyName) map.set(companyName, row);
  });
  return map;
}

export function predictiveMetaForCompany(lookup, row) {
  if (!lookup) return null;
  const companyId = String(row?.company_id || "").trim();
  const companyName = String(row?.company_name || "").trim().toLowerCase();
  return lookup.get(companyId) || lookup.get(companyName) || null;
}

export function primaryIndustry(row) {
  if (!row) return "Unknown";
  if (Array.isArray(row.industries) && row.industries[0]) return row.industries[0];
  if (row.sector) return row.sector;
  return "Unknown";
}

export function buildHeatmapModel(industryPatterns = [], topTechnologies = []) {
  const columns = topTechnologies.map((item) => item.technology).filter(Boolean);
  const rows = industryPatterns.map((entry) => {
    const technologies = Array.isArray(entry.top_technologies) ? entry.top_technologies : [];
    const values = columns.map((technology) => {
      const index = technologies.findIndex((item) => item === technology);
      if (index === -1) return 0;
      return Math.max(0.35, 1 - index * 0.25);
    });
    return {
      label: entry.industry,
      values,
    };
  });

  return { columns, rows };
}

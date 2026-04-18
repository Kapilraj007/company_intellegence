import { Badge, Card } from "../../components";
import { T } from "../../theme";
import { clusterColor, formatScore } from "./utils";

export default function ClusterCard({ cluster, active, onSelect }) {
  const tone = clusterColor(cluster?.cluster_id);

  return (
    <button
      onClick={() => onSelect?.(cluster?.cluster_id)}
      style={{
        background: "transparent",
        border: "none",
        padding: 0,
        textAlign: "left",
        cursor: "pointer",
      }}
    >
      <Card
        style={{
          padding: 18,
          borderColor: active ? `${tone}55` : T.border,
          background: active ? `${tone}10` : T.navy2,
        }}
      >
        <div style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "flex-start" }}>
          <div>
            <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px" }}>
              Cluster {Number(cluster?.cluster_id) >= 0 ? Number(cluster.cluster_id) + 1 : "Noise"}
            </div>
            <div style={{ color: T.white, fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, marginTop: 8 }}>
              {cluster?.label || "Unlabeled cluster"}
            </div>
          </div>
          <Badge color={tone}>{cluster?.size || 0} companies</Badge>
        </div>

        <div style={{ display: "flex", gap: 16, marginTop: 14, flexWrap: "wrap" }}>
          <div>
            <div style={{ fontSize: 11, color: T.muted, marginBottom: 4 }}>Cohesion</div>
            <div style={{ color: tone, fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 22 }}>
              {formatScore(cluster?.cohesion_score)}
            </div>
          </div>
          <div>
            <div style={{ fontSize: 11, color: T.muted, marginBottom: 4 }}>Top Theme</div>
            <div style={{ color: T.white, fontSize: 13, lineHeight: 1.5, maxWidth: 220 }}>
              {Array.isArray(cluster?.top_terms) && cluster.top_terms.length
                ? cluster.top_terms.join(", ")
                : Array.isArray(cluster?.top_categories) && cluster.top_categories.length
                  ? cluster.top_categories.join(", ")
                  : "Theme not resolved"}
            </div>
          </div>
        </div>

        {Array.isArray(cluster?.top_categories) && cluster.top_categories.length > 0 && (
          <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 14 }}>
            {cluster.top_categories.map((category) => (
              <Badge key={category} color={T.cyan}>{category}</Badge>
            ))}
          </div>
        )}
      </Card>
    </button>
  );
}

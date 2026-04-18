import { Card } from "../../components";
import { T } from "../../theme";

export default function AnalyticsMetricCard({ label, value, subtitle, tone = T.amber, style }) {
  const valueText = String(value ?? "—");
  const valueSize = valueText.length > 16 ? 18 : valueText.length > 10 ? 22 : 30;

  return (
    <Card
      style={{
        padding: "18px 20px",
        minHeight: 116,
        position: "relative",
        overflow: "hidden",
        ...style,
      }}
    >
      <div
        style={{
          position: "absolute",
          inset: 0,
          background: `radial-gradient(circle at top right, ${tone}18, transparent 35%)`,
          pointerEvents: "none",
        }}
      />
      <div style={{ position: "relative" }}>
        <div style={{ fontSize: 11, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px" }}>
          {label}
        </div>
        <div
          style={{
            fontFamily: "'Syne', sans-serif",
            fontWeight: 800,
            fontSize: valueSize,
            color: tone,
            marginTop: 10,
            lineHeight: 1.15,
            overflowWrap: "anywhere",
          }}
        >
          {valueText}
        </div>
        {subtitle && (
          <div style={{ fontSize: 12, color: T.muted, marginTop: 8, lineHeight: 1.6 }}>
            {subtitle}
          </div>
        )}
      </div>
    </Card>
  );
}

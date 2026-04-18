import { Card } from "../../components";
import { T } from "../../theme";

export default function SectionCard({ title, subtitle, action = null, children, style }) {
  return (
    <Card style={{ padding: 20, ...style }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start", flexWrap: "wrap" }}>
        <div>
          <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18, color: T.white }}>
            {title}
          </h2>
          {subtitle && (
            <div style={{ color: T.muted, fontSize: 12, marginTop: 5, lineHeight: 1.6 }}>
              {subtitle}
            </div>
          )}
        </div>
        {action}
      </div>
      <div style={{ marginTop: 18 }}>
        {children}
      </div>
    </Card>
  );
}


import { T } from "../../theme";
import { clusterColor, truncateLabel } from "./utils";

function EmptyChart({ message }) {
  return (
    <div
      style={{
        minHeight: 220,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: T.muted,
        fontSize: 13,
        border: `1px dashed ${T.border}`,
        borderRadius: 12,
      }}
    >
      {message}
    </div>
  );
}

export function BarChart({
  data,
  labelKey,
  valueKey,
  color = T.cyan,
  valueFormatter = (value) => value,
}) {
  if (!Array.isArray(data) || !data.length) {
    return <EmptyChart message="No chart data available." />;
  }

  const width = 560;
  const rowHeight = 40;
  const topPadding = 18;
  const labelWidth = 154;
  const chartWidth = 340;
  const height = data.length * rowHeight + topPadding * 2;
  const maxValue = Math.max(...data.map((item) => Number(item?.[valueKey]) || 0), 1);

  return (
    <svg viewBox={`0 0 ${width} ${height}`} style={{ width: "100%", height: "auto", display: "block" }}>
      {data.map((item, index) => {
        const value = Number(item?.[valueKey]) || 0;
        const barWidth = (value / maxValue) * chartWidth;
        const y = topPadding + index * rowHeight;
        const label = String(item?.[labelKey] || "—");

        return (
          <g key={`${label}-${index}`}>
            <text x="0" y={y + 18} fill={T.text} fontSize="12">
              {truncateLabel(label, 22)}
            </text>
            <rect x={labelWidth} y={y + 4} width={chartWidth} height="16" rx="8" fill={T.navy3} />
            <rect x={labelWidth} y={y + 4} width={barWidth} height="16" rx="8" fill={color} />
            <text x={labelWidth + chartWidth + 12} y={y + 18} fill={T.white} fontSize="12">
              {valueFormatter(value, item)}
            </text>
            <title>{`${label}: ${valueFormatter(value, item)}`}</title>
          </g>
        );
      })}
    </svg>
  );
}

export function LineChart({
  data,
  labelKey,
  valueKey,
  color = T.amber,
  valueFormatter = (value) => value,
}) {
  if (!Array.isArray(data) || !data.length) {
    return <EmptyChart message="No trend data available." />;
  }

  const width = 560;
  const height = 260;
  const left = 28;
  const right = 22;
  const top = 24;
  const bottom = 46;
  const values = data.map((item) => Number(item?.[valueKey]) || 0);
  const maxValue = Math.max(...values, 1);
  const minValue = Math.min(...values, 0);
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;

  const points = data.map((item, index) => {
    const x = left + (plotWidth * index) / Math.max(data.length - 1, 1);
    const normalized = maxValue === minValue ? 0.5 : (Number(item?.[valueKey]) - minValue) / (maxValue - minValue);
    const y = top + plotHeight - normalized * plotHeight;
    return { x, y, label: String(item?.[labelKey] || "—"), value: Number(item?.[valueKey]) || 0 };
  });

  return (
    <svg viewBox={`0 0 ${width} ${height}`} style={{ width: "100%", height: "auto", display: "block" }}>
      {[0, 1, 2, 3].map((line) => {
        const y = top + (plotHeight * line) / 3;
        return <line key={line} x1={left} y1={y} x2={width - right} y2={y} stroke={T.border} strokeDasharray="4 4" />;
      })}
      <polyline
        fill="none"
        stroke={color}
        strokeWidth="3"
        points={points.map((point) => `${point.x},${point.y}`).join(" ")}
      />
      {points.map((point, index) => (
        <g key={`${point.label}-${index}`}>
          <circle cx={point.x} cy={point.y} r="4.5" fill={color} />
          <text x={point.x} y={height - 14} textAnchor="middle" fill={T.muted} fontSize="11">
            {truncateLabel(point.label, 14)}
          </text>
          <title>{`${point.label}: ${valueFormatter(point.value)}`}</title>
        </g>
      ))}
    </svg>
  );
}

export function DonutChart({
  data,
  labelKey,
  valueKey,
  centerLabel = "Total",
}) {
  if (!Array.isArray(data) || !data.length) {
    return <EmptyChart message="No cluster distribution available." />;
  }

  const total = data.reduce((sum, item) => sum + (Number(item?.[valueKey]) || 0), 0);
  if (!total) {
    return <EmptyChart message="No cluster distribution available." />;
  }

  const radius = 76;
  const circumference = 2 * Math.PI * radius;
  const segments = data.map((item, index) => {
    const value = Number(item?.[valueKey]) || 0;
    const segment = (value / total) * circumference;
    const previous = data
      .slice(0, index)
      .reduce((sum, entry) => sum + ((Number(entry?.[valueKey]) || 0) / total) * circumference, 0);

    return {
      item,
      index,
      value,
      strokeDasharray: `${segment} ${circumference - segment}`,
      strokeDashoffset: -previous,
    };
  });

  return (
    <div style={{ display: "grid", gridTemplateColumns: "220px minmax(0, 1fr)", gap: 18, alignItems: "center" }}>
      <svg viewBox="0 0 220 220" style={{ width: "100%", maxWidth: 220, height: "auto", display: "block" }}>
        <g transform="translate(110 110) rotate(-90)">
          <circle cx="0" cy="0" r={radius} fill="none" stroke={T.navy3} strokeWidth="24" />
          {segments.map(({ item, index, value, strokeDasharray, strokeDashoffset }) => {
            return (
              <circle
                key={`${item?.[labelKey] || index}`}
                cx="0"
                cy="0"
                r={radius}
                fill="none"
                stroke={clusterColor(index)}
                strokeWidth="24"
                strokeLinecap="butt"
                strokeDasharray={strokeDasharray}
                strokeDashoffset={strokeDashoffset}
              >
                <title>{`${item?.[labelKey]}: ${value}`}</title>
              </circle>
            );
          })}
        </g>
        <text x="110" y="102" textAnchor="middle" fill={T.muted} fontSize="11" letterSpacing="0.8">
          {centerLabel.toUpperCase()}
        </text>
        <text x="110" y="126" textAnchor="middle" fill={T.white} fontSize="28" fontFamily="'Syne', sans-serif" fontWeight="800">
          {total}
        </text>
      </svg>

      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        {data.map((item, index) => {
          const value = Number(item?.[valueKey]) || 0;
          const percentage = (value / total) * 100;
          return (
            <div key={`${item?.[labelKey] || index}`} style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
              <div style={{ display: "flex", gap: 8, minWidth: 0 }}>
                <span
                  style={{
                    width: 10,
                    height: 10,
                    borderRadius: "50%",
                    background: clusterColor(index),
                    marginTop: 4,
                    flexShrink: 0,
                  }}
                />
                <div style={{ color: T.text, fontSize: 13, lineHeight: 1.5, minWidth: 0 }}>
                  {String(item?.[labelKey] || "Unknown")}
                </div>
              </div>
              <div style={{ color: T.white, fontSize: 13, whiteSpace: "nowrap" }}>
                {value} ({percentage.toFixed(0)}%)
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export function HeatmapChart({ rows, columns }) {
  if (!Array.isArray(rows) || !rows.length || !Array.isArray(columns) || !columns.length) {
    return <EmptyChart message="No industry-to-technology matrix available." />;
  }

  const clamp = (value) => Math.max(0, Math.min(1, Number(value) || 0));
  const labelWidth = 178;
  const columnWidth = 56;
  const minGridWidth = labelWidth + columns.length * columnWidth;
  const gridColumns = `${labelWidth}px repeat(${columns.length}, minmax(${columnWidth - 8}px, 1fr))`;

  const rowScores = rows.map((row) => {
    const values = Array.isArray(row.values) ? row.values : [];
    if (!values.length) return 0;
    return values.reduce((sum, value) => sum + clamp(value), 0) / values.length;
  });

  const columnScores = columns.map((_, columnIndex) => {
    const values = rows.map((row) => clamp(Array.isArray(row.values) ? row.values[columnIndex] : 0));
    if (!values.length) return 0;
    return values.reduce((sum, value) => sum + value, 0) / values.length;
  });

  const hottestColumnIndex = columnScores.reduce(
    (best, score, index) => (score > columnScores[best] ? index : best),
    0
  );
  const hottestColumn = columns[hottestColumnIndex] || "—";

  const toneFor = (value) => {
    const intensity = clamp(value);
    if (!intensity) {
      return {
        background: T.navy3,
        border: T.border,
        glow: "none",
        text: T.muted,
      };
    }
    const alpha = (0.22 + intensity * 0.62).toFixed(3);
    return {
      background: `linear-gradient(155deg, rgba(0, 212, 255, ${alpha}), rgba(14, 32, 59, 0.95))`,
      border: `rgba(0, 212, 255, ${(0.24 + intensity * 0.56).toFixed(3)})`,
      glow: `0 0 ${(6 + intensity * 10).toFixed(0)}px rgba(0, 212, 255, ${(0.10 + intensity * 0.24).toFixed(3)})`,
      text: intensity >= 0.58 ? T.white : T.text,
    };
  };

  return (
    <div style={{ display: "grid", gap: 12 }}>
      <style>{`
        .insights-heatmap-grid {
          display: grid;
          gap: 8px 8px;
          align-items: center;
        }
        .insights-heatmap-cell {
          transition: transform 140ms ease, box-shadow 180ms ease, border-color 180ms ease;
        }
        .insights-heatmap-cell:hover {
          transform: translateY(-1px) scale(1.03);
          border-color: rgba(0, 212, 255, 0.82) !important;
          box-shadow: 0 0 0 1px rgba(0, 212, 255, 0.36), 0 8px 18px rgba(0, 0, 0, 0.24) !important;
        }
      `}</style>

      <div style={{ display: "flex", justifyContent: "space-between", gap: 10, flexWrap: "wrap" }}>
        <div style={{ display: "inline-flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
          {[0.2, 0.45, 0.7, 0.95].map((level) => (
            <span
              key={level}
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 6,
                color: T.muted,
                fontSize: 10,
                letterSpacing: "0.6px",
                textTransform: "uppercase",
              }}
            >
              <span
                style={{
                  width: 10,
                  height: 10,
                  borderRadius: 4,
                  background: toneFor(level).background,
                  border: `1px solid ${toneFor(level).border}`,
                  boxShadow: toneFor(level).glow,
                }}
              />
              {Math.round(level * 100)}%
            </span>
          ))}
        </div>

        <div
          style={{
            color: T.text,
            fontSize: 11,
            letterSpacing: "0.3px",
            background: `${T.navy3}aa`,
            border: `1px solid ${T.border}`,
            borderRadius: 999,
            padding: "5px 10px",
            whiteSpace: "nowrap",
          }}
        >
          Strongest tech signal: <span style={{ color: T.cyan }}>{truncateLabel(hottestColumn, 20)}</span>
        </div>
      </div>

      <div style={{ overflowX: "auto", overflowY: "hidden", paddingBottom: 2 }}>
        <div className="insights-heatmap-grid" style={{ gridTemplateColumns: gridColumns, minWidth: minGridWidth }}>
          <div />
          {columns.map((column) => (
            <div
              key={column}
              style={{
                color: T.muted,
                fontSize: 10,
                textAlign: "center",
                letterSpacing: "0.2px",
                lineHeight: 1.3,
                minWidth: 0,
                padding: "0 2px",
              }}
              title={column}
            >
              {truncateLabel(column, 12)}
            </div>
          ))}

          {rows.flatMap((row, rowIndex) => {
            const rowScore = rowScores[rowIndex] || 0;
            const rowLabelNode = (
              <div key={`row-label-${row.label}-${rowIndex}`} style={{ minWidth: 0, paddingRight: 8 }}>
                <div style={{ color: T.text, fontSize: 11, lineHeight: 1.2 }} title={row.label}>
                  {truncateLabel(row.label, 24)}
                </div>
                <div
                  style={{
                    marginTop: 4,
                    height: 4,
                    borderRadius: 999,
                    background: T.navy3,
                    border: `1px solid ${T.border}`,
                    overflow: "hidden",
                  }}
                >
                  <div
                    style={{
                      width: `${Math.round(rowScore * 100)}%`,
                      height: "100%",
                      background: `linear-gradient(90deg, ${T.cyan}, ${T.green})`,
                    }}
                  />
                </div>
              </div>
            );

            const cells = columns.map((column, columnIndex) => {
              const value = clamp(Array.isArray(row.values) ? row.values[columnIndex] : 0);
              const tone = toneFor(value);
              return (
                <div
                  key={`cell-${row.label}-${column}-${columnIndex}`}
                  className="insights-heatmap-cell"
                  style={{
                    height: 34,
                    borderRadius: 10,
                    border: `1px solid ${tone.border}`,
                    background: tone.background,
                    boxShadow: tone.glow,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    color: tone.text,
                    fontSize: value >= 0.5 ? 10 : 0,
                    fontWeight: 700,
                    letterSpacing: "0.2px",
                    cursor: "default",
                    userSelect: "none",
                  }}
                  title={`${row.label} × ${column}: ${value ? `${Math.round(value * 100)}% signal` : "No signal"}`}
                >
                  {value >= 0.5 ? `${Math.round(value * 100)}` : ""}
                </div>
              );
            });

            return [rowLabelNode, ...cells];
          })}

          <div
            style={{
              color: T.muted,
              fontSize: 10,
              textTransform: "uppercase",
              letterSpacing: "0.7px",
              paddingRight: 8,
            }}
          >
            Tech Momentum
          </div>
          {columnScores.map((score, index) => (
            <div key={`column-momentum-${columns[index]}-${index}`} style={{ display: "flex", justifyContent: "center" }}>
              <div
                style={{
                  width: 28,
                  height: 6,
                  borderRadius: 999,
                  background: T.navy3,
                  border: `1px solid ${T.border}`,
                  overflow: "hidden",
                }}
                title={`${columns[index]} momentum: ${Math.round(score * 100)}%`}
              >
                <div
                  style={{
                    width: `${Math.round(score * 100)}%`,
                    height: "100%",
                    background: `linear-gradient(90deg, ${T.cyan}, ${T.amber})`,
                  }}
                />
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export function ClusterScatterChart({
  points,
  clusterLabels,
  selectedClusterId,
  onSelect,
}) {
  if (!Array.isArray(points) || !points.length) {
    return <EmptyChart message="No cluster map available." />;
  }

  const width = 560;
  const height = 300;
  const padding = 28;
  const xs = points.map((point) => Number(point?.x) || 0);
  const ys = points.map((point) => Number(point?.y) || 0);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);

  const projectX = (value) => {
    if (maxX === minX) return width / 2;
    return padding + ((Number(value) - minX) / (maxX - minX)) * (width - padding * 2);
  };

  const projectY = (value) => {
    if (maxY === minY) return height / 2;
    return height - padding - ((Number(value) - minY) / (maxY - minY)) * (height - padding * 2);
  };

  const legendIds = [...new Set(points.map((point) => Number(point.cluster_id)))].sort((left, right) => left - right);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <svg viewBox={`0 0 ${width} ${height}`} style={{ width: "100%", height: "auto", display: "block" }}>
        {[0, 1, 2, 3].map((line) => {
          const x = padding + ((width - padding * 2) * line) / 3;
          const y = padding + ((height - padding * 2) * line) / 3;
          return (
            <g key={line}>
              <line x1={x} y1={padding} x2={x} y2={height - padding} stroke={T.border} strokeDasharray="4 4" />
              <line x1={padding} y1={y} x2={width - padding} y2={y} stroke={T.border} strokeDasharray="4 4" />
            </g>
          );
        })}

        {points.map((point, index) => {
          const clusterId = Number(point.cluster_id);
          const active = selectedClusterId === null || selectedClusterId === clusterId;
          const color = clusterColor(clusterId);
          const x = projectX(point.x);
          const y = projectY(point.y);

          return (
            <g key={`${point.company_id || point.company_name || index}`} onClick={() => onSelect?.(clusterId)} style={{ cursor: "pointer" }}>
              <circle
                cx={x}
                cy={y}
                r={active ? 6 : 4}
                fill={color}
                opacity={active ? 0.95 : 0.45}
                stroke={selectedClusterId === clusterId ? T.white : "transparent"}
                strokeWidth="1.5"
              />
              <title>{`${point.company_name} · ${clusterLabels?.[clusterId] || `Cluster ${clusterId + 1}`}`}</title>
            </g>
          );
        })}
      </svg>

      <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
        {legendIds.map((clusterId) => (
          <button
            key={clusterId}
            onClick={() => onSelect?.(clusterId)}
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 8,
              padding: "6px 10px",
              borderRadius: 999,
              border: `1px solid ${selectedClusterId === clusterId ? `${clusterColor(clusterId)}60` : T.border}`,
              background: selectedClusterId === clusterId ? `${clusterColor(clusterId)}14` : T.navy3,
              color: T.text,
              cursor: "pointer",
              fontSize: 12,
            }}
          >
            <span style={{ width: 9, height: 9, borderRadius: "50%", background: clusterColor(clusterId), flexShrink: 0 }} />
            {clusterLabels?.[clusterId] || (clusterId >= 0 ? `Cluster ${clusterId + 1}` : "Noise")}
          </button>
        ))}
      </div>
    </div>
  );
}

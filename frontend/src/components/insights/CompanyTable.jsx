import { useMemo, useState } from "react";
import { T } from "../../theme";

function getColumnValue(column, row) {
  if (typeof column.value === "function") return column.value(row);
  return row?.[column.key];
}

function compareValues(left, right) {
  const leftNumber = Number(left);
  const rightNumber = Number(right);

  if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
    return leftNumber - rightNumber;
  }

  return String(left ?? "").localeCompare(String(right ?? ""), undefined, { sensitivity: "base" });
}

export default function CompanyTable({
  rows,
  columns,
  initialSortKey = "",
  initialSortDirection = "desc",
  emptyMessage = "No rows available for this selection.",
}) {
  const [sortKey, setSortKey] = useState(initialSortKey || columns.find((column) => column.sortable !== false)?.key || "");
  const [sortDirection, setSortDirection] = useState(initialSortDirection);

  const sortedRows = useMemo(() => {
    if (!sortKey) return rows;
    const column = columns.find((item) => item.key === sortKey);
    if (!column) return rows;

    const next = [...rows].sort((left, right) => {
      const value = compareValues(getColumnValue(column, left), getColumnValue(column, right));
      return sortDirection === "asc" ? value : -value;
    });

    return next;
  }, [columns, rows, sortDirection, sortKey]);

  if (!rows.length) {
    return <div style={{ color: T.muted, fontSize: 13 }}>{emptyMessage}</div>;
  }

  return (
    <div style={{ overflow: "auto", border: `1px solid ${T.border}`, borderRadius: 12 }}>
      <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 760 }}>
        <thead>
          <tr style={{ background: T.navy3 }}>
            {columns.map((column) => {
              const active = sortKey === column.key;
              return (
                <th
                  key={column.key}
                  onClick={() => {
                    if (column.sortable === false) return;
                    if (sortKey === column.key) {
                      setSortDirection((current) => (current === "asc" ? "desc" : "asc"));
                      return;
                    }
                    setSortKey(column.key);
                    setSortDirection(column.defaultDirection || "desc");
                  }}
                  style={{
                    padding: "12px 14px",
                    textAlign: column.align || "left",
                    color: active ? T.amber : T.muted,
                    fontSize: 11,
                    letterSpacing: "0.8px",
                    textTransform: "uppercase",
                    borderBottom: `1px solid ${T.border}`,
                    cursor: column.sortable === false ? "default" : "pointer",
                    whiteSpace: "nowrap",
                  }}
                >
                  {column.label}
                  {active ? (sortDirection === "asc" ? " ↑" : " ↓") : ""}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row, index) => (
            <tr key={`${row.company_id || row.company_name || index}`} style={{ borderBottom: `1px solid ${T.border}` }}>
              {columns.map((column) => (
                <td
                  key={column.key}
                  style={{
                    padding: "13px 14px",
                    fontSize: 13,
                    color: T.text,
                    verticalAlign: "top",
                    textAlign: column.align || "left",
                  }}
                >
                  {typeof column.render === "function" ? column.render(row) : getColumnValue(column, row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}


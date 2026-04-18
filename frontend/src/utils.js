export function formatDateTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

export function formatTime(value) {
  if (!value) {
    return new Date().toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function truncateId(value, size = 8) {
  if (!value) return "—";
  return String(value).slice(0, size);
}

export function humanizeKey(key) {
  return String(key || "")
    .replace(/[_-]+/g, " ")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .trim()
    .replace(/\s+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

export function outputIdFromPath(path) {
  const match = String(path || "").match(
    /([^/]+?)_(golden_record|validation_report|pytest_report|semantic_chunks)_(\d{8}_\d{6})\.json$/i
  );
  if (!match) return "";
  return `${match[1]}_${match[3]}`;
}

export function stringifyValue(value) {
  if (value == null) return "—";
  if (typeof value === "string") return value.trim() || "—";
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (Array.isArray(value)) {
    if (value.length === 0) return "—";
    if (value.every((item) => item == null || ["string", "number", "boolean"].includes(typeof item))) {
      return value.join(", ");
    }
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export function countFields(payload) {
  if (Array.isArray(payload)) return payload.length;
  if (payload && typeof payload === "object") return Object.keys(payload).length;
  return 0;
}

export function normalizeGoldenRecord(payload) {
  if (Array.isArray(payload)) {
    return payload.map((row, index) => ({
      id: row?.ID || index + 1,
      field: row?.Parameter || row?.Field || row?.field || `Field ${index + 1}`,
      rawKey: row?.Parameter || row?.Field || row?.field || `field_${index + 1}`,
      value:
        row?.["Research Output / Data"] ??
        row?.Value ??
        row?.value ??
        row?.output ??
        stringifyValue(row),
      source: row?.Source || row?.source || row?.Model || row?.model || "—",
    }));
  }

  if (payload && typeof payload === "object") {
    return Object.entries(payload).map(([key, value], index) => ({
      id: index + 1,
      field: humanizeKey(key),
      rawKey: key,
      value: stringifyValue(value),
      source: "Golden record",
    }));
  }

  return [];
}

export function normalizeValidationResults(payload) {
  const rows = Array.isArray(payload?.results) ? payload.results : [];
  return rows.map((row, index) => ({
    id: row?.ID || index + 1,
    category: row?.Category || "—",
    parameter: row?.Parameter || "—",
    status: row?.status || "—",
    issue: row?.issue || "",
  }));
}

export function normalizeChunkRows(payload) {
  const rows = Array.isArray(payload)
    ? payload
    : Array.isArray(payload?.chunks)
      ? payload.chunks
      : Array.isArray(payload?.records)
        ? payload.records
        : [];

  return rows.map((row, index) => ({
    id: row?.chunk_id || row?.id || index + 1,
    title: row?.chunk_title || row?.title || row?.heading || `Chunk ${index + 1}`,
    text: row?.chunk_text || row?.text || row?.content || stringifyValue(row),
    score: row?.score,
    category: row?.category || row?.section || "—",
  }));
}

export function prettyJson(value) {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export function searchResultKey(result) {
  return (
    result?.company_id ||
    result?.company_name ||
    result?.id ||
    `${result?.score || 0}-${result?.category || "result"}`
  );
}

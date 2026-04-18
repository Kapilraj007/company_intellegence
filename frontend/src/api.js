/**
 * API helpers for the FastAPI backend.
 */

import { getAccessToken, isSupabaseConfigured, refreshAccessToken } from "./supabaseClient";

// In production (Docker/Nginx) the frontend and backend share the same origin
// via an `/api` reverse proxy, so default to `/api` to avoid CORS issues.
export const API_BASE =
  import.meta.env.VITE_API_BASE || (import.meta.env.PROD ? "/api" : "http://localhost:8000");

async function getAuthHeaders() {
  if (!isSupabaseConfigured()) {
    return {};
  }

  try {
    const token = await getAccessToken();
    return token ? { Authorization: `Bearer ${token}` } : {};
  } catch {
    return {};
  }
}

async function request(path, options = {}) {
  const authHeaders = await getAuthHeaders();
  const baseHeaders = {
    ...(options.body ? { "Content-Type": "application/json" } : {}),
    ...(options.headers || {}),
  };
  let headers = {
    ...baseHeaders,
    ...authHeaders,
  };
  let res = await fetch(`${API_BASE}${path}`, { ...options, headers });

  // Retry once with a freshly refreshed token to recover from stale/expired sessions.
  if (res.status === 401 && isSupabaseConfigured()) {
    try {
      const refreshedToken = await refreshAccessToken();
      if (refreshedToken) {
        headers = {
          ...baseHeaders,
          Authorization: `Bearer ${refreshedToken}`,
        };
        res = await fetch(`${API_BASE}${path}`, { ...options, headers });
      }
    } catch {
      // Keep original 401 response handling below.
    }
  }

  if (res.status === 204) return null;

  let data = null;
  try {
    data = await res.json();
  } catch {
    data = null;
  }

  if (res.status === 202) {
    return { notReady: true, ...(data || {}) };
  }

  if (!res.ok) {
    const detail = data?.detail || res.statusText || `HTTP ${res.status}`;
    throw new Error(detail);
  }

  return data;
}

export async function submitJob(company) {
  return request("/run", {
    method: "POST",
    body: JSON.stringify({ company }),
  });
}

export async function getStatus(taskId) {
  return request(`/status/${encodeURIComponent(taskId)}`);
}

export async function getResult(taskId) {
  return request(`/result/${encodeURIComponent(taskId)}`);
}

export async function getTasks() {
  return request("/tasks");
}

export async function getTaskEvents(taskId, since = 0, limit = 200) {
  return request(
    `/events/${encodeURIComponent(taskId)}?since=${encodeURIComponent(since)}&limit=${encodeURIComponent(limit)}`
  );
}

export async function getOutputs(limit = 200) {
  return request(`/outputs?limit=${encodeURIComponent(limit)}`);
}

export async function getFileJson(path) {
  return request(`/file?path=${encodeURIComponent(path)}`);
}

export async function searchSimilarCompanies({
  query,
  top_k = 5,
  top_k_chunks = 200,
  exclude_company = "",
  include_full_data = true,
  filters = null,
}) {
  return request("/search/similar", {
    method: "POST",
    body: JSON.stringify({
      query,
      top_k,
      top_k_chunks,
      exclude_company,
      include_full_data,
      filters,
    }),
  });
}

export async function searchCompanies({
  query,
  top_k = 5,
  top_k_chunks = 200,
  exclude_company = "",
  include_full_data = true,
  filters = null,
}) {
  return request("/search-companies", {
    method: "POST",
    body: JSON.stringify({
      query,
      top_k,
      top_k_chunks,
      exclude_company,
      include_full_data,
      filters,
    }),
  });
}

export async function getHealth() {
  return request("/health");
}

export async function getCurrentUser() {
  return request("/auth/me");
}

export async function healthCheck() {
  try {
    const data = await getHealth();
    return data?.status === "ok";
  } catch {
    return false;
  }
}

export async function getInnovationClusters({
  company_ids = null,
  company_names = null,
  limit = 24,
  algorithm = "auto",
  reduction = "auto",
  n_clusters = null,
  min_cluster_size = 2,
  include_noise = false,
}) {
  return request("/ml/innovation-clusters", {
    method: "POST",
    body: JSON.stringify({
      company_ids,
      company_names,
      limit,
      algorithm,
      reduction,
      n_clusters,
      min_cluster_size,
      include_noise,
    }),
  });
}

export async function getDescriptiveAnalytics({
  company_ids = null,
  company_names = null,
  limit = 40,
  top_n = 6,
}) {
  return request("/ml/analytics/descriptive", {
    method: "POST",
    body: JSON.stringify({
      company_ids,
      company_names,
      limit,
      top_n,
    }),
  });
}

export async function getPredictiveAnalytics({
  company_ids = null,
  company_names = null,
  limit = 40,
  top_n = 6,
  min_training_samples = 6,
}) {
  return request("/ml/analytics/predictive", {
    method: "POST",
    body: JSON.stringify({
      company_ids,
      company_names,
      limit,
      top_n,
      min_training_samples,
    }),
  });
}

// ─── Versioning & Caching API ──────────────────────────────────────────
export async function analyzeCompany(company, forceRefresh = false) {
  return request("/analyze", {
    method: "POST",
    body: JSON.stringify({ company, force_refresh: forceRefresh }),
  });
}

export async function getRunStatus(runId) {
  return request(`/run-status/${encodeURIComponent(runId)}`);
}

export async function getCompanyHistory(companyName) {
  return request(`/company-history/${encodeURIComponent(companyName)}`);
}

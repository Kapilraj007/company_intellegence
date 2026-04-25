/**
 * API helpers for the FastAPI backend.
 */

// In production (Docker/Nginx) the frontend and backend share the same origin
// via an `/api` reverse proxy, so default to `/api` to avoid CORS issues.
export const API_BASE =
  import.meta.env.VITE_API_BASE || (import.meta.env.PROD ? "/api" : "http://localhost:8000");

async function request(path, options = {}) {
  const headers = {
    ...(options.body ? { "Content-Type": "application/json" } : {}),
    ...(options.headers || {}),
  };
  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers,
    credentials: "include",
  });

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

export async function signup({ name, email, password }) {
  return request("/auth/signup", {
    method: "POST",
    body: JSON.stringify({ name, email, password }),
  });
}

export async function login({ email, password }) {
  return request("/auth/login", {
    method: "POST",
    body: JSON.stringify({ email, password }),
  });
}

export async function logout() {
  return request("/auth/logout", {
    method: "POST",
  });
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

export async function getPendingUsers() {
  return request("/auth/admin/users/pending");
}

export async function getAdminUsers({ approval_status = "", limit = 500 } = {}) {
  const params = new URLSearchParams();
  if (approval_status) params.set("approval_status", approval_status);
  params.set("limit", String(limit));
  return request(`/auth/admin/users?${params.toString()}`);
}

export async function approveUser(userId) {
  return request(`/auth/admin/users/${encodeURIComponent(userId)}/approve`, {
    method: "POST",
  });
}

export async function rejectUser(userId) {
  return request(`/auth/admin/users/${encodeURIComponent(userId)}/reject`, {
    method: "POST",
  });
}

export async function verifyUser(userId, { verified = true, note = "" } = {}) {
  return request(`/auth/admin/users/${encodeURIComponent(userId)}/verify`, {
    method: "POST",
    body: JSON.stringify({ verified, note }),
  });
}

export async function updateUserRole(userId, role) {
  return request(`/auth/admin/users/${encodeURIComponent(userId)}/role`, {
    method: "POST",
    body: JSON.stringify({ role }),
  });
}

export async function getAdminDashboard() {
  return request("/auth/admin/dashboard");
}

export async function getAdminPipelineRuns(limit = 300) {
  return request(`/auth/admin/pipelines/runs?limit=${encodeURIComponent(limit)}`);
}

export async function getAdminActivityLogs(limit = 300) {
  return request(`/auth/admin/activity-logs?limit=${encodeURIComponent(limit)}`);
}

export async function getAdminErrorLogs(limit = 200) {
  return request(`/auth/admin/error-logs?limit=${encodeURIComponent(limit)}`);
}

export async function getAdminDataVersions(limit = 300) {
  return request(`/auth/admin/data-versions?limit=${encodeURIComponent(limit)}`);
}

export async function getMyDataVersions({ limit = 200, company_id = "", company_name = "" } = {}) {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  if (company_id) params.set("company_id", company_id);
  if (company_name) params.set("company_name", company_name);
  return request(`/data/versions?${params.toString()}`);
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

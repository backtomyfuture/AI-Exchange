const API = "";

export async function request(path, options = {}) {
  const response = await fetch(`${API}${path}`, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.detail || "console_operation_failed");
  }
  return payload;
}

export function emailListParams(filters = {}) {
  const params = new URLSearchParams({ page: "1", page_size: "25" });
  const entries = {
    query: filters.query,
    sender: filters.sender,
    status: filters.status,
    route: filters.route,
    tier: filters.tier,
    received_from: filters.receivedFrom,
    received_to: filters.receivedTo
  };
  Object.entries(entries).forEach(([key, value]) => {
    if (value) params.set(key, value);
  });
  if (filters.requiresHuman === "true" || filters.requiresHuman === "false") {
    params.set("requires_human", filters.requiresHuman);
  }
  if (filters.hasAnomaly === "true" || filters.hasAnomaly === "false") {
    params.set("has_anomaly", filters.hasAnomaly);
  }
  return params;
}

export const listEmails = (filters) => request(`/api/emails?${emailListParams(filters)}`);
export const getTrace = (emailId) => request(`/api/emails/${encodeURIComponent(emailId)}/trace`);
export const listRules = () => request("/api/rules");

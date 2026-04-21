// api.js — Cliente REST hacia FastAPI (Serving Layer)

const BASE_URL = import.meta.env.VITE_API_URL ?? "";

function buildUrl(path, params = {}) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== null && v !== undefined) qs.set(k, String(v));
  });
  const query = qs.toString();
  return `${BASE_URL}${path}${query ? `?${query}` : ""}`;
}

async function apiFetch(path, params = {}) {
  const res = await fetch(buildUrl(path, params));
  if (!res.ok) throw new Error(`API error ${res.status} en ${path}`);
  return res.json();
}

async function apiPost(path, params = {}) {
  const res = await fetch(buildUrl(path, params), { method: "POST" });
  if (!res.ok) throw new Error(`API error ${res.status} en ${path}`);
  return res.json();
}

export const api = {
  getRealtime: (limit = 20) => apiFetch("/api/realtime", { limit }),
  getHistorical: (days = 7) => apiFetch("/api/historical", { days }),
  getInsights: (severity = null, limit = 20) =>
    apiFetch("/api/insights", severity ? { severity, limit } : { limit }),
  getKddSummary: () => apiFetch("/api/kdd/summary"),

  getRecommendations: (userId, limit = 10) =>
    apiFetch(`/api/recommendations/${encodeURIComponent(userId)}`, { limit }),
  getSimilarGames: (gameTitle, limit = 10) =>
    apiFetch(`/api/similar/${encodeURIComponent(gameTitle)}`, { limit }),
  getRecommenderRealtime: (limit = 20) =>
    apiFetch("/api/recommender/realtime", { limit }),
  getRecommenderInsights: (severity = null, limit = 20) =>
    apiFetch(
      "/api/recommender/insights",
      severity ? { severity, limit } : { limit }
    ),
  getModelMetrics: () => apiFetch("/api/metrics"),
  agentRecommend: (userId) => apiFetch(`/api/agent/recommend/${encodeURIComponent(userId)}`),
  agentMonitor: () => apiFetch("/api/agent/monitor"),
  recordInteraction: (params) => apiPost("/api/agent/interaction", params),
};

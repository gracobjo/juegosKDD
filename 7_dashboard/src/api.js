// api.js — Cliente REST hacia FastAPI (Serving Layer)
// Por defecto usa URL relativa ("") para que Vite proxee /api/* a FastAPI
// (ver vite.config.js). Esto funciona desde cualquier host (localhost,
// nodo1.home, IP remota, etc.) y evita problemas de CORS.
// Puedes sobreescribir con VITE_API_URL si el frontend se sirve sin proxy.

const BASE_URL = import.meta.env.VITE_API_URL ?? "";

function buildUrl(path, params = {}) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== null && v !== undefined) qs.set(k, v);
  });
  const query = qs.toString();
  return `${BASE_URL}${path}${query ? `?${query}` : ""}`;
}

async function apiFetch(path, params = {}) {
  const res = await fetch(buildUrl(path, params));
  if (!res.ok) throw new Error(`API error ${res.status} en ${path}`);
  return res.json();
}

export const api = {
  // Pipeline original (Steam live)
  getRealtime:   (limit = 20)                   => apiFetch("/api/realtime",   { limit }),
  getHistorical: (days = 7)                     => apiFetch("/api/historical", { days }),
  getInsights:   (severity = null, limit = 20, source = "both") =>
    apiFetch("/api/insights", { ...(severity ? { severity } : {}), limit, source }),
  getKddSummary: ()                             => apiFetch("/api/kdd/summary"),

  // Sistema de recomendacion hibrido
  getRecommendations: (userId, limit = 10)       => apiFetch(`/api/recommendations/${encodeURIComponent(userId)}`, { limit }),
  getPopular:         (limit = 10)               => apiFetch("/api/recommendations/popular", { limit }),
  getSimilar:         (game, limit = 10)         => apiFetch(`/api/similar/${encodeURIComponent(game)}`, { limit }),
  getModelMetrics:    (limit = 20)               => apiFetch("/api/recommender/metrics", { limit }),
  getRecommenderRealtime: (limit = 20)           => apiFetch("/api/recommender/realtime", { limit }),
  getRecommenderUsers: (limit = 50)              => apiFetch("/api/recommender/users", { limit }),

  // Agentes IA
  agentExplorer:   ()                            => apiFetch("/api/agent/explorer"),
  agentKdd:        ()                            => apiFetch("/api/agent/kdd"),
  agentRecommend:  (userId, topK = 5)            => apiFetch(`/api/agent/recommend/${encodeURIComponent(userId)}`, { top_k: topK }),
  agentMonitor:    (autoTrigger = false)         => apiFetch("/api/agent/monitor", { auto_trigger: autoTrigger }),

  // Ingesta de interacciones en tiempo real
  ingestInteraction: async (payload) => {
    const res = await fetch(buildUrl("/api/ingest/interaction"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error(`API error ${res.status} en ingest`);
    return res.json();
  },
};

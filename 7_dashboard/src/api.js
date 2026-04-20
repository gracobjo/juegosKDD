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
  getRealtime:   (limit = 20)                   => apiFetch("/api/realtime",   { limit }),
  getHistorical: (days = 7)                     => apiFetch("/api/historical", { days }),
  getInsights:   (severity = null, limit = 20)  =>
    apiFetch("/api/insights", severity ? { severity, limit } : { limit }),
  getKddSummary: ()                             => apiFetch("/api/kdd/summary"),
};

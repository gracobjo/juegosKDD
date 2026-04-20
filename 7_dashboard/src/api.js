// api.js — Cliente REST hacia FastAPI (Serving Layer)
// La URL base se puede sobreescribir con la variable de entorno VITE_API_URL.
// Por defecto apunta al proxy de Vite hacia http://localhost:8000.

const BASE_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

async function apiFetch(path, params = {}) {
  const url = new URL(`${BASE_URL}${path}`);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== null && v !== undefined) url.searchParams.set(k, v);
  });
  const res = await fetch(url.toString());
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

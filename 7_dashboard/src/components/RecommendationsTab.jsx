import { useState, useEffect, useCallback } from "react";
import { api } from "../api.js";

const tierColor = (score) => {
  if (score >= 0.75) return "#0891b2";
  if (score >= 0.50) return "#ca8a04";
  if (score >= 0.25) return "#db2777";
  return "#64748b";
};

export default function RecommendationsTab() {
  const [userId, setUserId] = useState("");
  const [gameQuery, setGameQuery] = useState("");
  const [recs, setRecs] = useState(null);
  const [popular, setPopular] = useState([]);
  const [similar, setSimilar] = useState([]);
  const [metrics, setMetrics] = useState([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);

  // Normaliza respuestas que pueden venir como array plano o envueltas en
  // { recommendations: [...] } (p.ej. si /recommendations/popular se come la
  // ruta dinámica /{user_id}). Así el componente es robusto a ambos casos.
  const toArray = (r) =>
    Array.isArray(r) ? r : Array.isArray(r?.recommendations) ? r.recommendations : [];

  const fetchPopular = useCallback(async () => {
    try {
      const p = await api.getPopular(15);
      setPopular(toArray(p));
    } catch (e) {
      setErr(e.message);
    }
  }, []);

  const fetchMetrics = useCallback(async () => {
    try {
      const m = await api.getModelMetrics(10);
      setMetrics(Array.isArray(m) ? m : []);
    } catch (e) {
      /* silencio: aun no hay metricas */
    }
  }, []);

  useEffect(() => {
    fetchPopular();
    fetchMetrics();
  }, [fetchPopular, fetchMetrics]);

  const search = async () => {
    if (!userId.trim()) return;
    setLoading(true); setErr(null); setRecs(null);
    try {
      const r = await api.getRecommendations(userId.trim(), 10);
      setRecs(r);
    } catch (e) {
      setErr(e.message);
    }
    setLoading(false);
  };

  const searchSimilar = async () => {
    if (!gameQuery.trim()) return;
    setLoading(true); setErr(null);
    try {
      const s = await api.getSimilar(gameQuery.trim().toLowerCase(), 10);
      setSimilar(s || []);
    } catch (e) {
      setErr(e.message);
    }
    setLoading(false);
  };

  const latestMetric = metrics[0] || {};

  return (
    <div>
      {/* Metricas del modelo */}
      <div className="section">
        <div className="section-title">Metricas del Modelo Hibrido</div>
        <div className="panel-grid" style={{ gridTemplateColumns: "repeat(4,1fr)" }}>
          {[
            ["RMSE", latestMetric.rmse?.toFixed(4) || "—"],
            [`Precision@${latestMetric.k || 10}`, latestMetric.precision_at_k?.toFixed(4) || "—"],
            [`Recall@${latestMetric.k || 10}`, latestMetric.recall_at_k?.toFixed(4) || "—"],
            ["Alpha (CF peso)", latestMetric.alpha?.toFixed(2) || "—"],
          ].map(([k, v]) => (
            <div className="stat-card" key={k}>
              <div className="stat-label">{k}</div>
              <div className="stat-value" style={{ color: "var(--purple)" }}>{v}</div>
            </div>
          ))}
        </div>
        {metrics.length > 0 && (
          <div style={{ marginTop: 10, fontFamily: "var(--mono)", fontSize: 10, color: "var(--muted)" }}>
            Ultima ejecucion: {new Date(latestMetric.evaluated_at).toLocaleString()} ·
            {" "}{latestMetric.users_with_recs?.toLocaleString() || 0} usuarios con recs ·
            {" "}{latestMetric.games_covered?.toLocaleString() || 0} juegos cubiertos
          </div>
        )}
      </div>

      {/* Busqueda por usuario */}
      <div className="section">
        <div className="section-title">Recomendaciones por Usuario</div>
        <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
          <input
            className="ai-input"
            placeholder="user_id (ej: 151603712)"
            value={userId}
            onChange={(e) => setUserId(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && search()}
          />
          <button className="ai-btn" onClick={search} disabled={loading}>
            {loading ? "BUSCANDO..." : "RECOMENDAR"}
          </button>
        </div>

        {err && (
          <div style={{ color: "var(--red)", fontFamily: "var(--mono)", fontSize: 11 }}>
            Error: {err}
          </div>
        )}

        {recs && recs.cold_start && (
          <div className="insight-item" style={{ borderColor: "var(--yellow)", color: "var(--yellow)", background: "rgba(202,138,4,0.08)" }}>
            Usuario sin historial -&gt; mostrando top global (cold-start).
          </div>
        )}

        {recs && recs.recommendations?.length > 0 && (
          <div className="bar-chart">
            {recs.recommendations.map((r, i) => {
              const score = r.hybrid_score ?? 0;
              return (
                <div key={`${r.recommended_game || r.game}-${i}`} className="bar-row" style={{ alignItems: "flex-start" }}>
                  <div className="bar-key" title={r.recommended_game || r.game}>
                    {(r.recommended_game || r.game)}
                  </div>
                  <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 4 }}>
                    <div className="bar-track">
                      <div className="bar-fill" style={{ width: `${Math.min(score * 100, 100)}%`, background: tierColor(score) }} />
                    </div>
                    <div style={{ fontSize: 10, color: "var(--muted)", fontFamily: "var(--mono)" }}>
                      CF={r.cf_component?.toFixed(3) ?? "-"} · CB={r.cb_component?.toFixed(3) ?? "-"} · alpha={r.alpha_used?.toFixed(2) ?? "-"} · {r.source || "batch"}
                    </div>
                  </div>
                  <div className="bar-val">{score.toFixed(3)}</div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Juegos similares */}
      <div className="section border-top">
        <div className="section-title">Juegos Similares (TF-IDF content-based)</div>
        <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
          <input
            className="ai-input"
            placeholder="game_title (ej: dota 2)"
            value={gameQuery}
            onChange={(e) => setGameQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && searchSimilar()}
          />
          <button className="ai-btn" onClick={searchSimilar}>BUSCAR SIMILARES</button>
        </div>
        {similar.length > 0 && (
          <div className="bar-chart">
            {similar.map((s, i) => (
              <div key={i} className="bar-row">
                <div className="bar-key">{s.game}</div>
                <div className="bar-track">
                  <div className="bar-fill" style={{ width: `${s.similarity * 100}%`, background: "#7c3aed" }} />
                </div>
                <div className="bar-val">{s.similarity.toFixed(3)}</div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Popular / cold-start */}
      <div className="section border-top">
        <div className="section-title">Top Global (Cold-Start Fallback)</div>
        <div className="bar-chart">
          {popular.map((p, i) => (
            <div key={i} className="bar-row">
              <div className="bar-key">{p.recommended_game}</div>
              <div className="bar-track">
                <div className="bar-fill" style={{ width: `${Math.min(100, (p.hybrid_score || 0) * 100)}%`, background: "#0891b2" }} />
              </div>
              <div className="bar-val">
                {p.n_players?.toLocaleString() || 0} jug · {Math.round(p.total_hours || 0)}h
              </div>
            </div>
          ))}
          {popular.length === 0 && (
            <div style={{ color: "var(--muted)", fontFamily: "var(--mono)", fontSize: 11 }}>
              Aun no hay juegos populares en Cassandra. Ejecuta el pipeline KDD para poblarlos.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

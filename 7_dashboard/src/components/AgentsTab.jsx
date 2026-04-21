import { useState, useEffect, useCallback } from "react";
import { api } from "../api.js";

function AgentCard({ title, color, children, loading, onRefresh }) {
  return (
    <div className="ai-panel" style={{ borderColor: `${color}55` }}>
      <div className="ai-header" style={{ justifyContent: "space-between" }}>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <div className="ai-dot" style={{ background: color }} />
          <div className="ai-label" style={{ color }}>{title}</div>
        </div>
        {onRefresh && (
          <button className="ctrl-btn" onClick={onRefresh} disabled={loading}>
            {loading ? "..." : "↻"}
          </button>
        )}
      </div>
      <div className="ai-response">{children}</div>
    </div>
  );
}

export default function AgentsTab() {
  const [explorer, setExplorer] = useState(null);
  const [kdd, setKdd] = useState(null);
  const [monitor, setMonitor] = useState(null);
  const [recUserId, setRecUserId] = useState("");
  const [rec, setRec] = useState(null);
  const [loadingAll, setLoadingAll] = useState(false);
  const [loadingRec, setLoadingRec] = useState(false);

  const refreshAll = useCallback(async () => {
    setLoadingAll(true);
    try {
      const [e, k, m] = await Promise.all([
        api.agentExplorer().catch((err) => ({ error: err.message })),
        api.agentKdd().catch((err) => ({ error: err.message })),
        api.agentMonitor(false).catch((err) => ({ error: err.message })),
      ]);
      setExplorer(e);
      setKdd(k);
      setMonitor(m);
    } finally {
      setLoadingAll(false);
    }
  }, []);

  const askRec = async () => {
    if (!recUserId.trim()) return;
    setLoadingRec(true);
    try {
      const r = await api.agentRecommend(recUserId.trim(), 5);
      setRec(r);
    } catch (e) {
      setRec({ error: e.message });
    }
    setLoadingRec(false);
  };

  useEffect(() => { refreshAll(); }, [refreshAll]);

  return (
    <div className="section">
      <div className="section-title" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span>Agentes IA</span>
        <button className="ctrl-btn" onClick={refreshAll} disabled={loadingAll}>
          {loadingAll ? "ACTUALIZANDO..." : "↻ REFRESH ALL"}
        </button>
      </div>

      <div className="two-col" style={{ gridTemplateColumns: "1fr 1fr", gap: 16, background: "transparent" }}>
        {/* Explorer */}
        <AgentCard title="Agente Explorador" color="#0891b2" loading={loadingAll}>
          {explorer?.error ? (
            <span style={{ color: "var(--red)" }}>{explorer.error}</span>
          ) : explorer ? (
            <>
              <div>{explorer.diagnosis}</div>
              <div style={{ marginTop: 10, fontFamily: "var(--mono)", fontSize: 10, color: "var(--muted)" }}>
                {Object.entries(explorer.datasets || {}).map(([name, info]) => (
                  <div key={name}>
                    {info.exists ? "OK" : "--"} {name}: {info.size_mb || 0} MB · {info.rows_approx?.toLocaleString() || 0} filas
                  </div>
                ))}
                {explorer.is_synthetic && (
                  <div style={{ color: "var(--yellow)", marginTop: 4 }}>
                    (datos sinteticos - configura ~/.kaggle/kaggle.json para datos reales)
                  </div>
                )}
              </div>
            </>
          ) : (
            <span className="ai-loading">cargando...</span>
          )}
        </AgentCard>

        {/* KDD */}
        <AgentCard title="Agente KDD" color="#7c3aed" loading={loadingAll}>
          {kdd?.error ? (
            <span style={{ color: "var(--red)" }}>{kdd.error}</span>
          ) : kdd ? (
            <>
              <div>{kdd.summary}</div>
              <div style={{ marginTop: 10, fontFamily: "var(--mono)", fontSize: 10, color: "var(--muted)" }}>
                {Object.entries(kdd.cassandra_counts || {}).map(([k, v]) => (
                  <div key={k}>{k}: {v.toLocaleString()}</div>
                ))}
              </div>
            </>
          ) : (
            <span className="ai-loading">cargando...</span>
          )}
        </AgentCard>

        {/* Monitor */}
        <AgentCard title="Agente Monitor" color="#ca8a04" loading={loadingAll}>
          {monitor?.error ? (
            <span style={{ color: "var(--red)" }}>{monitor.error}</span>
          ) : monitor ? (
            <>
              <div>{monitor.diagnosis}</div>
              <div style={{ marginTop: 10, fontFamily: "var(--mono)", fontSize: 10 }}>
                <span style={{ color: monitor.drift?.drift_detected ? "var(--red)" : "var(--cyan)" }}>
                  {monitor.drift?.drift_detected ? "DRIFT" : "OK"}
                </span>
                <span style={{ marginLeft: 8, color: "var(--muted)" }}>
                  (RMSE actual: {monitor.drift?.latest_rmse?.toFixed(4) || "—"},
                  baseline: {monitor.drift?.baseline_rmse?.toFixed(4) || "—"})
                </span>
              </div>
              {monitor.drift?.reasons?.map((r, i) => (
                <div key={i} style={{ fontSize: 10, color: "var(--yellow)", fontFamily: "var(--mono)", marginTop: 4 }}>
                  · {r}
                </div>
              ))}
              {!monitor.llm_available && (
                <div style={{ marginTop: 6, fontSize: 10, color: "var(--muted)" }}>
                  (Claude API deshabilitado: define ANTHROPIC_API_KEY en .env para diagnosticos IA)
                </div>
              )}
            </>
          ) : (
            <span className="ai-loading">cargando...</span>
          )}
        </AgentCard>

        {/* Recomendador personalizado */}
        <AgentCard title="Agente Recomendador" color="#db2777">
          <div style={{ display: "flex", gap: 6, marginBottom: 10 }}>
            <input
              className="ai-input"
              placeholder="user_id"
              value={recUserId}
              onChange={(e) => setRecUserId(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && askRec()}
            />
            <button className="ai-btn" onClick={askRec} disabled={loadingRec}>
              {loadingRec ? "..." : "EXPLICAR"}
            </button>
          </div>
          {rec?.error && <span style={{ color: "var(--red)" }}>{rec.error}</span>}
          {rec?.cold_start && (
            <div style={{ color: "var(--yellow)", fontFamily: "var(--mono)", fontSize: 11, marginBottom: 8 }}>
              COLD-START: {rec.message}
            </div>
          )}
          {rec?.recommendations?.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              {rec.recommendations.map((r, i) => (
                <div key={i} style={{ padding: 10, background: "rgba(219,39,119,0.06)", borderLeft: "2px solid var(--pink)", borderRadius: "0 4px 4px 0" }}>
                  <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 4 }}>
                    {r.game || r.recommended_game} <span style={{ fontSize: 10, color: "var(--muted)", fontFamily: "var(--mono)" }}>(score {(r.hybrid_score || 0).toFixed(3)})</span>
                  </div>
                  <div style={{ fontSize: 12, lineHeight: 1.5 }}>{r.explanation}</div>
                </div>
              ))}
            </div>
          )}
          {!rec && (
            <span className="ai-loading">Introduce un user_id para recibir recomendaciones explicadas.</span>
          )}
        </AgentCard>
      </div>
    </div>
  );
}

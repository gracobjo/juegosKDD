/**
 * KDDλGAMING — Dashboard React
 * -----------------------------------------------------------------------------
 * Consume datos reales desde FastAPI (Cassandra) en lugar de datos sintéticos.
 * Mantiene la misma estética cyberpunk/terminal del prototipo original.
 *
 * CAMBIOS RESPECTO AL PROTOTIPO SINTÉTICO:
 * - Los datos vienen de api.js → FastAPI → Cassandra
 * - Los juegos son reales (CS2, Dota 2, Elden Ring, etc.)
 * - Las métricas son player counts, review scores y health scores reales
 * - El panel IA recibe contexto real de los datos de Cassandra
 * - Se añade indicador de estado de conexión API
 */

import { useState, useEffect, useRef, useCallback } from "react";
import { api } from "./api.js";

// ── KDD Stages ───────────────────────────────────────────────────────────────
const KDD_STAGES = [
  { id: "selection",      label: "Selection",     color: "#0891b2" },
  { id: "preprocessing",  label: "Preprocessing", color: "#ca8a04" },
  { id: "transformation", label: "Transform",     color: "#db2777" },
  { id: "mining",         label: "Mining",        color: "#7c3aed" },
  { id: "evaluation",     label: "Evaluation",    color: "#2563eb" },
];

// ── Helpers ──────────────────────────────────────────────────────────────────
const tierColor = (tier) =>
  ({ massive: "#0891b2", popular: "#ca8a04", active: "#db2777", niche: "#64748b" }[tier] || "#64748b");

const severityColor = (s) =>
  ({ info: "#2563eb", warning: "#ca8a04", alert: "#db2777", critical: "#dc2626" }[s] || "#64748b");

// ── CSS (misma estética que el prototipo) ───────────────────────────────────
const css = `
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow+Condensed:wght@300;400;600;800&display=swap');
* { box-sizing:border-box; margin:0; padding:0; }
:root {
  --bg0:#f1f5f9; --bg1:#ffffff; --border:rgba(15,23,42,0.10);
  --cyan:#0891b2; --yellow:#ca8a04; --pink:#db2777; --purple:#7c3aed;
  --blue:#2563eb; --red:#dc2626; --text:#0f172a; --muted:#64748b;
  --mono:'Share Tech Mono',monospace; --display:'Barlow Condensed',sans-serif;
}
body { background:var(--bg0); color:var(--text); font-family:var(--display); }
.app { min-height:100vh; position:relative; }
.app::before { content:''; position:fixed; inset:0; background:repeating-linear-gradient(0deg,transparent,transparent 3px,rgba(15,23,42,0.02) 3px,rgba(15,23,42,0.02) 4px); pointer-events:none; z-index:9999; }
.header { display:flex; align-items:center; justify-content:space-between; padding:12px 24px; background:linear-gradient(90deg,rgba(8,145,178,0.10),rgba(255,255,255,0.85) 60%); border-bottom:1px solid var(--border); position:sticky; top:0; z-index:100; backdrop-filter:blur(10px); }
.logo { font-size:22px; font-weight:800; letter-spacing:3px; color:var(--cyan); text-transform:uppercase; }
.logo span { color:var(--yellow); }
.badge { font-family:var(--mono); font-size:10px; color:var(--muted); border:1px solid var(--border); padding:3px 8px; border-radius:2px; }
.live-dot { width:8px; height:8px; border-radius:50%; background:var(--cyan); animation:pulse 1.5s ease-in-out infinite; }
.live-dot.error { background:var(--red); }
@keyframes pulse { 0%,100%{opacity:1;box-shadow:0 0 0 0 rgba(8,145,178,0.5)} 50%{opacity:.7;box-shadow:0 0 0 6px rgba(8,145,178,0)} }
.tabs { display:flex; border-bottom:1px solid var(--border); padding:0 24px; background:var(--bg1); }
.tab { padding:10px 20px; font-family:var(--display); font-size:12px; font-weight:600; letter-spacing:2px; text-transform:uppercase; cursor:pointer; border:none; background:none; color:var(--muted); border-bottom:2px solid transparent; transition:all .2s; }
.tab.active { color:var(--cyan); border-bottom-color:var(--cyan); }
.kdd-pipeline { display:flex; align-items:center; gap:0; padding:16px 24px; background:var(--bg1); border-bottom:1px solid var(--border); overflow-x:auto; }
.kdd-stage { display:flex; flex-direction:column; align-items:center; gap:4px; min-width:90px; position:relative; }
.kdd-stage:not(:last-child)::after { content:'→'; position:absolute; right:-12px; top:12px; color:var(--muted); font-size:14px; }
.stage-hex { width:36px; height:36px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:16px; border:2px solid; transition:all .3s; }
.stage-label { font-size:9px; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; color:var(--muted); }
.stage-count { font-family:var(--mono); font-size:11px; color:var(--text); }
.section { padding:20px 24px; }
.section-title { font-size:10px; font-weight:600; letter-spacing:3px; text-transform:uppercase; color:var(--muted); margin-bottom:16px; display:flex; align-items:center; gap:8px; }
.section-title::after { content:''; flex:1; height:1px; background:var(--border); }
.two-col { display:grid; grid-template-columns:1fr 1fr; gap:1px; background:var(--border); }
.two-col>* { background:var(--bg1); }
.panel-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:1px; background:var(--border); }
.panel-grid>* { background:var(--bg1); }
.border-top { border-top:1px solid var(--border); }
.stat-card { padding:20px; border-right:1px solid var(--border); }
.stat-card:last-child { border-right:none; }
.stat-label { font-size:9px; font-weight:600; letter-spacing:2px; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }
.stat-value { font-family:var(--mono); font-size:28px; line-height:1; }
.stat-unit { font-size:12px; color:var(--muted); margin-left:4px; }
.bar-chart { display:flex; flex-direction:column; gap:8px; }
.bar-row { display:flex; align-items:center; gap:10px; }
.bar-key { font-family:var(--mono); font-size:10px; color:var(--muted); width:140px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; flex-shrink:0; }
.bar-track { flex:1; height:6px; background:rgba(15,23,42,0.08); border-radius:3px; overflow:hidden; }
.bar-fill { height:100%; border-radius:3px; transition:width .6s cubic-bezier(.16,1,.3,1); }
.bar-val { font-family:var(--mono); font-size:10px; color:var(--muted); width:50px; text-align:right; }
.feed-item { display:grid; grid-template-columns:auto 1fr auto; gap:10px; align-items:center; padding:10px; background:rgba(15,23,42,0.03); border-left:2px solid; border-radius:0 4px 4px 0; font-family:var(--mono); font-size:10px; margin-bottom:6px; animation:slideIn .3s ease; }
@keyframes slideIn { from{opacity:0;transform:translateX(-8px)} to{opacity:1;transform:translateX(0)} }
.feed-game { color:var(--text); font-size:12px; font-weight:600; }
.feed-meta { color:var(--muted); font-size:10px; }
.insight-item { display:flex; align-items:flex-start; gap:10px; padding:10px 12px; border-radius:4px; font-size:12px; line-height:1.4; margin-bottom:8px; border:1px solid; }
.ai-panel { background:linear-gradient(135deg,rgba(124,58,237,.06),rgba(8,145,178,.05)); border:1px solid rgba(124,58,237,.22); border-radius:6px; padding:16px; }
.ai-header { display:flex; align-items:center; gap:8px; margin-bottom:12px; }
.ai-dot { width:8px; height:8px; border-radius:50%; background:var(--purple); animation:pulse 2s ease-in-out infinite; }
.ai-label { font-size:10px; font-weight:700; letter-spacing:3px; text-transform:uppercase; color:var(--purple); }
.ai-response { font-size:12px; line-height:1.6; color:var(--text); min-height:60px; white-space:pre-wrap; }
.ai-loading { color:var(--muted); font-family:var(--mono); font-size:11px; animation:blink 1s step-end infinite; }
@keyframes blink { 50%{opacity:0} }
.ai-prompt { display:flex; gap:8px; margin-top:12px; }
.ai-input { flex:1; background:rgba(15,23,42,0.04); border:1px solid var(--border); border-radius:4px; padding:8px 12px; font-family:var(--mono); font-size:11px; color:var(--text); outline:none; }
.ai-input:focus { border-color:var(--purple); }
.ai-btn { padding:8px 16px; background:rgba(124,58,237,.12); border:1px solid rgba(124,58,237,.40); border-radius:4px; color:var(--purple); font-family:var(--display); font-size:11px; font-weight:600; letter-spacing:1px; text-transform:uppercase; cursor:pointer; transition:all .2s; }
.ai-btn:disabled { opacity:.4; cursor:not-allowed; }
.ctrl-btn { padding:6px 14px; font-family:var(--display); font-size:10px; font-weight:600; letter-spacing:2px; text-transform:uppercase; border:1px solid var(--border); border-radius:2px; background:none; color:var(--muted); cursor:pointer; transition:all .2s; margin-left:8px; }
.ctrl-btn:hover { color:var(--text); border-color:var(--cyan); }
.ctrl-btn.active { color:var(--cyan); border-color:var(--cyan); background:rgba(8,145,178,0.10); }
.error-banner { padding:10px 24px; background:rgba(220,38,38,0.08); border-bottom:1px solid rgba(220,38,38,0.25); font-family:var(--mono); font-size:11px; color:var(--red); }
`;

// ── Sparkline SVG ────────────────────────────────────────────────────────────
function Sparkline({ data, color = "#0891b2" }) {
  if (!data || data.length < 2) return null;
  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = max - min || 1;
  const w = 200;
  const h = 48;
  const pts = data.map((v, i) => {
    const x = (i / (data.length - 1)) * w;
    const y = h - ((v - min) / range) * (h - 8) - 4;
    return `${x},${y}`;
  });
  const gradId = `g${color.slice(1)}`;
  return (
    <svg viewBox={`0 0 ${w} ${h}`} style={{ width: "100%", height: 48 }} preserveAspectRatio="none">
      <defs>
        <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity=".3" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={`M${pts[0]}L${pts.join("L")}L${w},${h}L0,${h}Z`} fill={`url(#${gradId})`} />
      <path d={`M${pts.join("L")}`} fill="none" stroke={color} strokeWidth="1.5" />
    </svg>
  );
}

// ── App ──────────────────────────────────────────────────────────────────────
export default function App() {
  const [tab, setTab] = useState("realtime");
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [realtimeData, setRealtime] = useState([]);
  const [historicalData, setHistorical] = useState([]);
  const [insights, setInsights] = useState([]);
  const [kddSummary, setKddSummary] = useState(null);
  const [apiError, setApiError] = useState(null);
  const [loading, setLoading] = useState(true);
  const [history, setHistory] = useState({ players: [], health: [] });
  const [kddStageActive, setKddStageActive] = useState(0);
  const [aiResponse, setAiResponse] = useState(
    "Pulsa «Análisis sistema» para leer el agente monitor (sin LLM externo)."
  );
  const [aiLoading, setAiLoading] = useState(false);
  const [aiPrompt, setAiPrompt] = useState("");
  const [recUserId, setRecUserId] = useState("151603717");
  const [recAgent, setRecAgent] = useState(null);
  const [recLoading, setRecLoading] = useState(false);
  const [monitorJson, setMonitorJson] = useState(null);
  const intervalRef = useRef(null);

  // Animación de fase KDD activa
  useEffect(() => {
    const t = setInterval(() => setKddStageActive((s) => (s + 1) % KDD_STAGES.length), 800);
    return () => clearInterval(t);
  }, []);

  // Fetch de todos los endpoints
  const fetchAll = useCallback(async () => {
    try {
      const [rt, hist, ins, kdd] = await Promise.all([
        api.getRealtime(20),
        api.getHistorical(7),
        api.getInsights(null, 15),
        api.getKddSummary(),
      ]);
      setRealtime(rt);
      setHistorical(hist);
      setInsights(ins);
      setKddSummary(kdd);
      setApiError(null);

      if (rt.length > 0) {
        const totalPlayers = rt.reduce((s, r) => s + (r.avg_players || 0), 0);
        const avgHealth =
          rt.reduce((s, r) => s + (r.avg_health_score || 0), 0) / (rt.length || 1);
        setHistory((h) => ({
          players: [...h.players.slice(-40), Math.round(totalPlayers)],
          health: [...h.health.slice(-40), Math.round(avgHealth)],
        }));
      }
    } catch (e) {
      setApiError(
        `Error conectando con API: ${e.message}. ¿Está corriendo FastAPI en :8000?`
      );
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchAll();
  }, [fetchAll]);

  useEffect(() => {
    if (!autoRefresh) {
      clearInterval(intervalRef.current);
      return;
    }
    intervalRef.current = setInterval(fetchAll, 15000);
    return () => clearInterval(intervalRef.current);
  }, [autoRefresh, fetchAll]);

  const runSystemAnalysis = useCallback(async () => {
    setAiLoading(true);
    try {
      const topGame = [...realtimeData].sort(
        (a, b) => (b.avg_players || 0) - (a.avg_players || 0)
      )[0];
      const avgHealth = realtimeData.length
        ? (
            realtimeData.reduce((s, r) => s + (r.avg_health_score || 0), 0) /
            realtimeData.length
          ).toFixed(1)
        : "N/A";
      const critInsights = insights.filter(
        (i) => i.severity === "critical" || i.severity === "alert"
      ).length;

      const mon = await api.agentMonitor();
      setMonitorJson(mon);
      const drift = mon.drift_detected ? "Sí" : "No";
      setAiResponse(
        `Steam (Speed/Batch): juego top ${topGame?.game || "N/D"} · health medio ${avgHealth}/100 · alertas ${critInsights}.\n` +
          `Recomendador: drift=${drift}. ${mon.diagnosis || ""}\n` +
          (mon.retraining_triggered
            ? "Se intentó disparar reentrenamiento vía Airflow."
            : "")
      );
    } catch (err) {
      setAiResponse(`No se pudo leer /api/agent/monitor: ${err.message}`);
    }
    setAiLoading(false);
  }, [realtimeData, insights]);

  // ── Métricas agregadas ────────────────────────────────────────────────────
  const totalPlayers = realtimeData.reduce((s, r) => s + (r.avg_players || 0), 0);
  const avgHealth = realtimeData.length
    ? realtimeData.reduce((s, r) => s + (r.avg_health_score || 0), 0) / realtimeData.length
    : 0;
  const avgReview = realtimeData.length
    ? realtimeData.reduce((s, r) => s + (r.avg_review_score || 0), 0) / realtimeData.length
    : 0;
  const alertCount = insights.filter(
    (i) => i.severity === "alert" || i.severity === "critical"
  ).length;

  const now = new Date().toLocaleTimeString("es-ES");

  const stageCount = (i) => {
    if (!kddSummary) return "—";
    const speed = kddSummary.layers?.speed?.records || 0;
    const batch = kddSummary.layers?.batch?.records || 0;
    const serving = kddSummary.layers?.serving?.insights || 0;
    return [speed, speed, batch, batch, serving][i] ?? "—";
  };

  const sendCustomPrompt = () => {
    if (!aiPrompt.trim()) return;
    setAiResponse(
      `Nota libre registrada: «${aiPrompt}». ` +
        `Las explicaciones detalladas por juego están en la pestaña Recomendaciones (API heurística).`
    );
    setAiPrompt("");
  };

  const loadRecommendations = useCallback(async () => {
    setRecLoading(true);
    try {
      const data = await api.agentRecommend(recUserId.trim() || "151603717");
      setRecAgent(data);
    } catch (e) {
      setRecAgent({ error: e.message });
    }
    setRecLoading(false);
  }, [recUserId]);

  return (
    <>
      <style>{css}</style>
      <div className="app">
        {/* HEADER */}
        <div className="header">
          <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
            <div className={`live-dot${apiError ? " error" : ""}`} />
            <div className="logo">
              KDD<span>λ</span>GAMING
            </div>
            <div className="badge">REAL DATA · CASSANDRA</div>
            {kddSummary && (
              <div
                className="badge"
                style={{ color: "var(--purple)", borderColor: "rgba(124,58,237,0.30)" }}
              >
                {(kddSummary.layers?.speed?.records || 0).toLocaleString()} WINDOWS
              </div>
            )}
          </div>
          <div style={{ display: "flex", alignItems: "center" }}>
            <button
              className={`ctrl-btn ${autoRefresh ? "active" : ""}`}
              onClick={() => setAutoRefresh((r) => !r)}
            >
              {autoRefresh ? "⏸ AUTO" : "▶ MANUAL"}
            </button>
            <button className="ctrl-btn" onClick={fetchAll}>
              ↻ REFRESH
            </button>
            <button className="ctrl-btn" onClick={() => runSystemAnalysis()}>
              ⚡ Análisis sistema
            </button>
          </div>
          <div
            style={{
              fontFamily: "var(--mono)",
              fontSize: 11,
              color: "var(--muted)",
              display: "flex",
              gap: 16,
            }}
          >
            <span>
              JUEGOS: <span style={{ color: "var(--cyan)" }}>{realtimeData.length}</span>
            </span>
            <span>{now}</span>
          </div>
        </div>

        {/* ERROR BANNER */}
        {apiError && <div className="error-banner">⚠ {apiError}</div>}

        {/* KDD PIPELINE */}
        <div className="kdd-pipeline">
          <div
            style={{
              fontFamily: "var(--mono)",
              fontSize: "10px",
              color: "var(--muted)",
              marginRight: 20,
              flexShrink: 0,
            }}
          >
            KDD PIPELINE
          </div>
          {KDD_STAGES.map((s, i) => (
            <div key={s.id} className="kdd-stage">
              <div
                className="stage-hex"
                style={{
                  color: s.color,
                  borderColor: s.color,
                  background: i === kddStageActive ? `${s.color}22` : "transparent",
                }}
              >
                {i === kddStageActive ? "◉" : "○"}
              </div>
              <div className="stage-label">{s.label}</div>
              <div className="stage-count" style={{ color: s.color }}>
                {stageCount(i)}
              </div>
            </div>
          ))}
          <div style={{ marginLeft: "auto", display: "flex", gap: 16, flexShrink: 0 }}>
            {[
              ["BATCH", "var(--purple)", "HISTÓRICO"],
              ["SPEED", "var(--cyan)", "TIEMPO REAL"],
              ["SERVING", "var(--yellow)", "CASSANDRA"],
            ].map(([l, c, s]) => (
              <div key={l} style={{ textAlign: "center" }}>
                <div style={{ fontFamily: "var(--mono)", fontSize: 9, color: "var(--muted)" }}>
                  {l} LAYER
                </div>
                <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: c }}>{s}</div>
              </div>
            ))}
          </div>
        </div>

        {/* TABS */}
        <div className="tabs">
          {[
            ["realtime", "⚡ Tiempo Real"],
            ["historical", "📊 Histórico"],
            ["insights", "🔬 KDD Insights"],
            ["recommendations", "🎮 Recomendaciones"],
            ["system", "🤖 Sistema"],
            ["lambda", "λ Arquitectura"],
          ].map(([id, label]) => (
            <button
              key={id}
              className={`tab ${tab === id ? "active" : ""}`}
              onClick={() => setTab(id)}
            >
              {label}
            </button>
          ))}
        </div>

        {/* ── TAB: REALTIME ── */}
        {tab === "realtime" && (
          <div>
            <div className="panel-grid" style={{ gridTemplateColumns: "repeat(4,1fr)" }}>
              {[
                {
                  label: "JUGADORES TOTALES",
                  value: totalPlayers.toLocaleString(),
                  color: "var(--cyan)",
                },
                {
                  label: "HEALTH SCORE AVG",
                  value: avgHealth.toFixed(1),
                  unit: "/100",
                  color:
                    avgHealth < 50
                      ? "var(--red)"
                      : avgHealth < 70
                      ? "var(--yellow)"
                      : "var(--cyan)",
                },
                {
                  label: "REVIEW SCORE AVG",
                  value: avgReview.toFixed(1),
                  unit: "%",
                  color:
                    avgReview < 50
                      ? "var(--red)"
                      : avgReview < 70
                      ? "var(--yellow)"
                      : "var(--cyan)",
                },
                {
                  label: "ALERTAS KDD",
                  value: alertCount,
                  color: alertCount > 0 ? "var(--red)" : "var(--cyan)",
                },
              ].map((s) => (
                <div key={s.label} className="stat-card">
                  <div className="stat-label">{s.label}</div>
                  <div className="stat-value" style={{ color: s.color }}>
                    {s.value}
                    <span className="stat-unit">{s.unit || ""}</span>
                  </div>
                </div>
              ))}
            </div>

            <div className="two-col border-top">
              <div className="section">
                <div className="section-title">
                  Speed Layer — Ventanas Activas (Cassandra)
                </div>
                <div style={{ maxHeight: 300, overflowY: "auto" }}>
                  {loading && (
                    <div
                      style={{
                        color: "var(--muted)",
                        fontFamily: "var(--mono)",
                        fontSize: 11,
                      }}
                    >
                      Cargando desde Cassandra...
                    </div>
                  )}
                  {[...realtimeData]
                    .sort((a, b) => (b.avg_players || 0) - (a.avg_players || 0))
                    .map((r, i) => (
                      <div
                        key={i}
                        className="feed-item"
                        style={{ borderLeftColor: tierColor(r.player_tier) }}
                      >
                        <span style={{ color: tierColor(r.player_tier), fontSize: 14 }}>
                          ▸
                        </span>
                        <div>
                          <div className="feed-game">{r.game}</div>
                          <div className="feed-meta">
                            {r.player_tier?.toUpperCase()} · review{" "}
                            {r.avg_review_score?.toFixed(1)}%
                          </div>
                        </div>
                        <div style={{ textAlign: "right" }}>
                          <div
                            style={{
                              color: tierColor(r.player_tier),
                              fontFamily: "var(--mono)",
                              fontSize: 12,
                            }}
                          >
                            {(r.avg_players || 0).toLocaleString()}
                          </div>
                          <div className="feed-meta">
                            health {r.avg_health_score?.toFixed(1)}
                          </div>
                        </div>
                      </div>
                    ))}
                </div>
              </div>

              <div className="section">
                <div className="section-title">Distribución de Jugadores por Juego</div>
                <div className="bar-chart">
                  {[...realtimeData]
                    .sort((a, b) => (b.avg_players || 0) - (a.avg_players || 0))
                    .map((r, i) => {
                      const maxP =
                        Math.max(...realtimeData.map((x) => x.avg_players || 0)) || 1;
                      const colors = [
                        "#0891b2",
                        "#ca8a04",
                        "#db2777",
                        "#7c3aed",
                        "#2563eb",
                        "#dc2626",
                        "#db2777",
                        "#0891b2",
                      ];
                      return (
                        <div key={i} className="bar-row">
                          <div className="bar-key">{r.game}</div>
                          <div className="bar-track">
                            <div
                              className="bar-fill"
                              style={{
                                width: `${((r.avg_players || 0) / maxP) * 100}%`,
                                background: colors[i % 8],
                              }}
                            />
                          </div>
                          <div className="bar-val" style={{ color: colors[i % 8] }}>
                            {((r.avg_players || 0) / 1000).toFixed(0)}K
                          </div>
                        </div>
                      );
                    })}
                </div>
              </div>
            </div>

            <div className="section border-top">
              <div className="ai-panel">
                <div className="ai-header">
                  <div className="ai-dot" />
                  <div className="ai-label">
                    KDD Intelligence — Monitor heurístico (FastAPI)
                  </div>
                </div>
                <div className="ai-response">
                  {aiLoading ? (
                    <span className="ai-loading">Consultando métricas del recomendador▋</span>
                  ) : (
                    aiResponse
                  )}
                </div>
                <div className="ai-prompt">
                  <input
                    className="ai-input"
                    placeholder="Nota / contexto (no llama a LLM externo)..."
                    value={aiPrompt}
                    onChange={(e) => setAiPrompt(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") sendCustomPrompt();
                    }}
                  />
                  <button
                    className="ai-btn"
                    disabled={aiLoading || !aiPrompt.trim()}
                    onClick={sendCustomPrompt}
                  >
                    ENVIAR
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ── TAB: HISTORICAL ── */}
        {tab === "historical" && (
          <div>
            <div className="two-col border-top">
              <div className="section">
                <div className="section-title">
                  Batch Layer — Evolución Total Jugadores
                </div>
                <Sparkline data={history.players} color="#0891b2" />
                <div
                  style={{
                    fontFamily: "var(--mono)",
                    fontSize: 10,
                    color: "var(--muted)",
                    marginTop: 8,
                  }}
                >
                  Últimas {history.players.length} lecturas · actualización cada 15s
                </div>
              </div>
              <div className="section">
                <div className="section-title">Batch Layer — Health Score Promedio</div>
                <Sparkline data={history.health} color="#7c3aed" />
                <div
                  style={{
                    fontFamily: "var(--mono)",
                    fontSize: 10,
                    color: "var(--muted)",
                    marginTop: 8,
                  }}
                >
                  Health = (reviews_positivas / total) × 0.7 + (players / 50K) × 0.3
                </div>
              </div>
            </div>

            <div className="section border-top">
              <div className="section-title">
                Resumen Diario — game_stats_daily (Cassandra)
              </div>
              <div className="bar-chart">
                {historicalData
                  .filter((v, i, a) => a.findIndex((x) => x.appid === v.appid) === i)
                  .sort((a, b) => (b.avg_players || 0) - (a.avg_players || 0))
                  .map((r, i) => {
                    const maxP =
                      Math.max(...historicalData.map((x) => x.avg_players || 0)) || 1;
                    const colors = [
                      "#0891b2",
                      "#ca8a04",
                      "#db2777",
                      "#7c3aed",
                      "#2563eb",
                      "#dc2626",
                      "#db2777",
                      "#0891b2",
                    ];
                    const growthColor =
                      (r.growth_rate || 0) >= 0 ? "var(--cyan)" : "var(--red)";
                    return (
                      <div key={i} className="bar-row">
                        <div className="bar-key">{r.game}</div>
                        <div className="bar-track">
                          <div
                            className="bar-fill"
                            style={{
                              width: `${((r.avg_players || 0) / maxP) * 100}%`,
                              background: colors[i % 8],
                            }}
                          />
                        </div>
                        <div className="bar-val" style={{ color: colors[i % 8] }}>
                          {((r.avg_players || 0) / 1000).toFixed(0)}K
                        </div>
                        <div
                          style={{
                            fontFamily: "var(--mono)",
                            fontSize: 9,
                            color: growthColor,
                            width: 40,
                            textAlign: "right",
                          }}
                        >
                          {(r.growth_rate || 0) >= 0 ? "+" : ""}
                          {(r.growth_rate || 0).toFixed(1)}%
                        </div>
                      </div>
                    );
                  })}
              </div>
            </div>
          </div>
        )}

        {/* ── TAB: KDD INSIGHTS ── */}
        {tab === "insights" && (
          <div className="section border-top">
            <div className="section-title">
              KDD Evaluation — Insights Generados por el Pipeline
            </div>
            {insights.length === 0 && (
              <div
                style={{
                  color: "var(--muted)",
                  fontFamily: "var(--mono)",
                  fontSize: 12,
                }}
              >
                Sin insights todavía. El pipeline genera insights cuando detecta
                anomalías en los datos de Steam.
              </div>
            )}
            {insights.map((ins, i) => (
              <div
                key={i}
                className="insight-item"
                style={{
                  borderColor: `${severityColor(ins.severity)}44`,
                  background: `${severityColor(ins.severity)}11`,
                  color: "var(--text)",
                }}
              >
                <span style={{ fontSize: 16 }}>
                  {{ info: "ℹ️", warning: "⚠️", alert: "🔴", critical: "🚨" }[
                    ins.severity
                  ] || "ℹ️"}
                </span>
                <div>
                  <div style={{ marginBottom: 4 }}>{ins.message}</div>
                  <div
                    style={{
                      fontFamily: "var(--mono)",
                      fontSize: 9,
                      color: "var(--muted)",
                    }}
                  >
                    {ins.layer?.toUpperCase()} LAYER · {ins.insight_type} ·{" "}
                    {ins.metric_name}={ins.metric_value?.toFixed(1)} ·{" "}
                    {ins.created_at?.slice(0, 19)}
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}

        {tab === "recommendations" && (
          <div className="section border-top">
            <div className="section-title">Recomendador híbrido (ALS + TF-IDF)</div>
            <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
              <input
                className="ai-input"
                style={{ maxWidth: 260 }}
                value={recUserId}
                onChange={(e) => setRecUserId(e.target.value)}
                placeholder="user_id (Kaggle Steam behaviors)"
              />
              <button
                className="ai-btn"
                disabled={recLoading}
                type="button"
                onClick={() => loadRecommendations()}
              >
                Cargar explicaciones
              </button>
            </div>
            {recLoading && (
              <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--muted)" }}>
                Consultando /api/agent/recommend …
              </div>
            )}
            {recAgent?.error && (
              <div style={{ color: "var(--red)", fontFamily: "var(--mono)", fontSize: 12 }}>
                {recAgent.error}
              </div>
            )}
            {recAgent && !recAgent.error && (
              <div style={{ display: "grid", gap: 10 }}>
                {(recAgent.recommendations || []).map((r, idx) => (
                  <div
                    key={idx}
                    className="insight-item"
                    style={{
                      borderColor: "rgba(8,145,178,0.35)",
                      background: "rgba(8,145,178,0.06)",
                    }}
                  >
                    <div style={{ fontWeight: 700, marginBottom: 6 }}>{r.game}</div>
                    <div
                      style={{
                        fontFamily: "var(--mono)",
                        fontSize: 10,
                        color: "var(--muted)",
                        marginBottom: 6,
                      }}
                    >
                      score {Number(r.hybrid_score || 0).toFixed(3)} · CF {Number(
                        r.cf_component || 0
                      ).toFixed(2)} · CB {Number(r.cb_component || 0).toFixed(2)}
                    </div>
                    <div style={{ fontSize: 12, lineHeight: 1.5 }}>{r.explanation}</div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {tab === "system" && (
          <div className="section border-top">
            <div className="section-title">Métricas del modelo (Cassandra)</div>
            <button className="ctrl-btn active" type="button" onClick={() => runSystemAnalysis()}>
              Refrescar monitor
            </button>
            <div style={{ marginTop: 16, fontFamily: "var(--mono)", fontSize: 11, color: "var(--text)" }}>
              {monitorJson ? (
                <pre style={{ whiteSpace: "pre-wrap", lineHeight: 1.4 }}>
                  {JSON.stringify(monitorJson, null, 2)}
                </pre>
              ) : (
                <span style={{ color: "var(--muted)" }}>
                  Sin datos todavía. Ejecuta el pipeline `kdd_pipeline.py` y vuelve a pulsar.
                </span>
              )}
            </div>
          </div>
        )}

        {/* ── TAB: LAMBDA ARCH ── */}
        {tab === "lambda" && (
          <div className="section border-top">
            <div className="section-title">Arquitectura Lambda — Tu Infraestructura Real</div>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr",
                gap: 12,
                maxWidth: 700,
              }}
            >
              {[
                {
                  cls: "batch",
                  title: "λ Batch Layer — Apache Spark + Hive",
                  desc:
                    "Job diario (Airflow 02:00 UTC) sobre datos históricos en Hive (Parquet). Calcula growth_rate, rankings, stddev. KDD completo: 5 fases sobre todo el histórico.",
                  tech: [
                    "Apache Spark",
                    "Apache Hive",
                    "Apache Airflow",
                    "Parquet/SNAPPY",
                    "spark_batch_kdd.py",
                    "gaming_kdd_batch DAG",
                  ],
                  color: "var(--purple)",
                },
                {
                  cls: "speed",
                  title: "⚡ Speed Layer — Spark Streaming + NiFi",
                  desc:
                    "Micro-batches de 30s desde Kafka (gaming.events.raw). NiFi hace polling a Steam API/SteamSpy cada 60s. KDD online con ventanas de 5 minutos.",
                  tech: [
                    "Apache NiFi",
                    "Apache Kafka",
                    "Spark Streaming",
                    "spark_streaming_kdd.py",
                    "InvokeHTTP processor",
                    "PublishKafka processor",
                  ],
                  color: "var(--cyan)",
                },
                {
                  cls: "serving",
                  title: "◈ Serving Layer — Apache Cassandra + FastAPI",
                  desc:
                    "Cassandra: gaming_kdd (Steam) + gaming_recommender (ALS/TF-IDF). FastAPI :8000 expone métricas, similares y agentes heurísticos.",
                  tech: [
                    "Apache Cassandra",
                    "FastAPI",
                    "gaming_kdd + gaming_recommender",
                    "gaming.user.interactions",
                    "kdd_pipeline.py",
                  ],
                  color: "var(--yellow)",
                },
              ].map((layer) => (
                <div
                  key={layer.cls}
                  style={{
                    border: `1px solid ${layer.color}44`,
                    background: `${layer.color}08`,
                    borderRadius: 6,
                    padding: 16,
                  }}
                >
                  <div
                    style={{
                      fontSize: 10,
                      fontWeight: 700,
                      letterSpacing: 2,
                      textTransform: "uppercase",
                      color: layer.color,
                      marginBottom: 8,
                    }}
                  >
                    {layer.title}
                  </div>
                  <div
                    style={{
                      fontSize: 12,
                      color: "var(--muted)",
                      marginBottom: 12,
                      lineHeight: 1.6,
                    }}
                  >
                    {layer.desc}
                  </div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                    {layer.tech.map((t) => (
                      <div
                        key={t}
                        style={{
                          fontFamily: "var(--mono)",
                          fontSize: 9,
                          padding: "3px 8px",
                          borderRadius: 2,
                          background: "rgba(15,23,42,0.05)",
                          color: "var(--muted)",
                        }}
                      >
                        {t}
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>

            {kddSummary && (
              <div style={{ marginTop: 24 }}>
                <div className="section-title">Estado Actual del Pipeline</div>
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(3,1fr)",
                    gap: 12,
                    maxWidth: 700,
                  }}
                >
                  {Object.entries(kddSummary.layers || {}).map(([key, val]) => (
                    <div
                      key={key}
                      style={{
                        padding: 12,
                        background: "rgba(15,23,42,0.03)",
                        border: "1px solid var(--border)",
                        borderRadius: 4,
                      }}
                    >
                      <div
                        style={{
                          fontSize: 9,
                          fontWeight: 700,
                          letterSpacing: 2,
                          textTransform: "uppercase",
                          color: "var(--muted)",
                          marginBottom: 8,
                        }}
                      >
                        {key} LAYER
                      </div>
                      <div
                        style={{
                          fontFamily: "var(--mono)",
                          fontSize: 20,
                          color: "var(--cyan)",
                        }}
                      >
                        {(Object.values(val)[0] || 0).toLocaleString()}
                      </div>
                      <div
                        style={{
                          fontFamily: "var(--mono)",
                          fontSize: 9,
                          color: "var(--muted)",
                          marginTop: 4,
                        }}
                      >
                        {val.table}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </>
  );
}

import time
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="GPU Cluster Health Monitor",
    page_icon="🖥️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .block-container { padding-top: 1rem; padding-bottom: 0rem; }
    .metric-card {
        background: #1e1e2e;
        border-radius: 10px;
        padding: 16px;
        border: 1px solid #313244;
    }
    h1 { color: #cdd6f4; }
    .stMetric label { color: #a6adc8 !important; }
</style>
""", unsafe_allow_html=True)

# ── DB connection ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    return create_engine(
        "postgresql+psycopg2://gpuadmin:gpupass123@postgres:5432/gpu_monitor",
        pool_pre_ping=True,
    )

def query(sql, params=None):
    with get_engine().connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)

# ── Data loaders ──────────────────────────────────────────────────────────────
def load_latest_per_gpu():
    return query("""
        SELECT DISTINCT ON (gpu_id)
            gpu_id, node, gpu_model,
            avg_util, avg_temp, max_temp,
            avg_power, avg_mem_bw, avg_mem_util,
            efficiency_score, health_status,
            thermal_throttle, underutilized, anomaly_flag,
            created_at
        FROM gpu_analytics
        ORDER BY gpu_id, created_at DESC
    """)

def load_alerts(limit=50):
    return query("""
        SELECT gpu_id, node, alert_type, avg_temp, avg_util, max_temp, created_at
        FROM gpu_alerts
        ORDER BY created_at DESC
        LIMIT :lim
    """, {"lim": limit})

def load_timeseries(minutes=5):
    return query("""
        SELECT event_time, gpu_id, utilization_pct, temperature_c, power_draw_w
        FROM gpu_raw_telemetry
        WHERE event_time >= NOW() - INTERVAL '5 minutes'
        ORDER BY event_time ASC
    """)

def load_cluster_summary():
    return query("""
        SELECT
            COUNT(DISTINCT gpu_id)                                         AS total_gpus,
            ROUND(AVG(avg_util)::numeric, 1)                               AS cluster_util,
            ROUND(AVG(avg_temp)::numeric, 1)                               AS cluster_temp,
            ROUND(AVG(avg_power)::numeric, 1)                              AS cluster_power,
            SUM(CASE WHEN health_status = 'CRITICAL' THEN 1 ELSE 0 END)   AS critical_count,
            SUM(CASE WHEN health_status = 'WARNING'  THEN 1 ELSE 0 END)   AS warning_count,
            SUM(CASE WHEN health_status = 'LOW_UTIL' THEN 1 ELSE 0 END)   AS low_util_count,
            SUM(CASE WHEN health_status = 'HEALTHY'  THEN 1 ELSE 0 END)   AS healthy_count
        FROM (
            SELECT DISTINCT ON (gpu_id) *
            FROM gpu_analytics
            ORDER BY gpu_id, created_at DESC
        ) latest
    """)

# ── Color maps ────────────────────────────────────────────────────────────────
STATUS_COLOR = {
    "HEALTHY" : "#a6e3a1",
    "LOW_UTIL": "#f9e2af",
    "WARNING" : "#fab387",
    "CRITICAL": "#f38ba8",
}
STATUS_EMOJI = {
    "HEALTHY" : "✅",
    "LOW_UTIL": "🟡",
    "WARNING" : "⚠️",
    "CRITICAL": "🔴",
}

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🖥️  GPU Cluster Health & Utilization Monitor")
st.caption("Live PySpark Structured Streaming  •  Kafka  •  PostgreSQL  •  Streamlit")

refresh = st.sidebar.slider("Auto-refresh (seconds)", 5, 60, 10)
st.sidebar.markdown("---")
st.sidebar.markdown("**Stack**")
st.sidebar.markdown("- Apache Kafka\n- PySpark Structured Streaming\n- PostgreSQL\n- Streamlit + Plotly")

# ── Main loop ─────────────────────────────────────────────────────────────────
placeholder = st.empty()

while True:
    try:
        gpu_df     = load_latest_per_gpu()
        alerts_df  = load_alerts()
        ts_df      = load_timeseries()
        summary_df = load_cluster_summary()

        with placeholder.container():

            # ── Row 1: Cluster KPIs ───────────────────────────────────────
            if not summary_df.empty:
                s = summary_df.iloc[0]
                c1, c2, c3, c4, c5, c6 = st.columns(6)
                c1.metric("Total GPUs",       int(s.get("total_gpus",   0)))
                c2.metric("Cluster Util %",   f"{s.get('cluster_util',  0):.1f}%")
                c3.metric("Avg Temp °C",       f"{s.get('cluster_temp',  0):.1f}")
                c4.metric("Avg Power W",       f"{s.get('cluster_power', 0):.1f}")
                c5.metric("🔴 Critical",       int(s.get("critical_count", 0)))
                c6.metric("⚠️  Warnings",      int(s.get("warning_count",  0)))

            st.divider()

            # ── Row 2: GPU Health Map + Efficiency Chart ──────────────────
            col_left, col_right = st.columns([1, 1])

            with col_left:
                st.subheader("GPU Health Map")
                if not gpu_df.empty:
                    cols = st.columns(4)
                    for i, row in gpu_df.iterrows():
                        status = row.get("health_status", "HEALTHY")
                        color  = STATUS_COLOR.get(status, "#cdd6f4")
                        emoji  = STATUS_EMOJI.get(status, "✅")
                        with cols[i % 4]:
                            st.markdown(f"""
                            <div style="background:{color};border-radius:8px;
                                        padding:10px;margin-bottom:8px;color:#1e1e2e;">
                                <b>{row['gpu_id']}</b><br/>
                                {emoji} {status}<br/>
                                🌡 {row['avg_temp']:.1f}°C<br/>
                                ⚡ {row['avg_util']:.1f}%<br/>
                                🔋 {row['avg_power']:.0f}W
                            </div>
                            """, unsafe_allow_html=True)
                else:
                    st.info("Waiting for GPU data...")

            with col_right:
                st.subheader("Efficiency Scores (Ranked)")
                if not gpu_df.empty:
                    eff = gpu_df[["gpu_id", "efficiency_score", "health_status"]].sort_values(
                        "efficiency_score", ascending=True
                    )
                    eff["color"] = eff["health_status"].map(STATUS_COLOR).fillna("#89b4fa")
                    fig = go.Figure(go.Bar(
                        x=eff["efficiency_score"],
                        y=eff["gpu_id"],
                        orientation="h",
                        marker_color=eff["color"],
                        text=eff["efficiency_score"].apply(lambda x: f"{x:.1f}"),
                        textposition="outside",
                    ))
                    fig.update_layout(
                        margin=dict(l=0, r=40, t=10, b=0),
                        height=350,
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        xaxis=dict(title="Score (0-100)", color="#cdd6f4", gridcolor="#313244"),
                        yaxis=dict(color="#cdd6f4"),
                        font=dict(color="#cdd6f4"),
                    )
                    st.plotly_chart(fig, use_container_width=True)

            st.divider()

            # ── Row 3: Time Series + Alerts ───────────────────────────────
            col_ts, col_alerts = st.columns([2, 1])

            with col_ts:
                st.subheader("Rolling Temperature — Last 5 Minutes")
                if not ts_df.empty:
                    fig2 = px.line(
                        ts_df, x="event_time", y="temperature_c",
                        color="gpu_id", labels={"temperature_c": "Temp °C", "event_time": ""},
                    )
                    fig2.add_hline(y=83, line_dash="dash", line_color="#f38ba8",
                                   annotation_text="Throttle threshold (83°C)")
                    fig2.update_layout(
                        height=300,
                        margin=dict(l=0, r=0, t=10, b=0),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        xaxis=dict(color="#cdd6f4", gridcolor="#313244"),
                        yaxis=dict(color="#cdd6f4", gridcolor="#313244"),
                        font=dict(color="#cdd6f4"),
                        legend=dict(font=dict(size=9)),
                    )
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.info("Collecting time series data...")

            with col_alerts:
                st.subheader("Recent Alerts")
                if not alerts_df.empty:
                    for _, row in alerts_df.head(10).iterrows():
                        atype = row["alert_type"]
                        color = STATUS_COLOR.get(atype, "#cdd6f4")
                        emoji = STATUS_EMOJI.get(atype, "⚠️")
                        st.markdown(f"""
                        <div style="background:{color};border-radius:6px;
                                    padding:8px;margin-bottom:6px;color:#1e1e2e;font-size:0.85rem;">
                            {emoji} <b>{row['gpu_id']}</b> ({row['node']})<br/>
                            {atype} — {row['avg_temp']:.1f}°C / {row['avg_util']:.1f}%<br/>
                            <small>{str(row['created_at'])[:19]}</small>
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    st.success("✅ No active alerts")

            # ── Row 4: Utilization heatmap across all GPUs ────────────────
            st.divider()
            st.subheader("Live Utilization % — All GPUs")
            if not gpu_df.empty:
                util_sorted = gpu_df.sort_values("avg_util", ascending=False)
                fig3 = go.Figure(go.Bar(
                    x=util_sorted["gpu_id"],
                    y=util_sorted["avg_util"],
                    marker_color=[STATUS_COLOR.get(s, "#89b4fa") for s in util_sorted["health_status"]],
                    text=util_sorted["avg_util"].apply(lambda x: f"{x:.1f}%"),
                    textposition="outside",
                ))
                fig3.add_hline(y=25, line_dash="dot", line_color="#f9e2af",
                               annotation_text="Underutil threshold")
                fig3.add_hline(y=95, line_dash="dot", line_color="#f38ba8",
                               annotation_text="Overload threshold")
                fig3.update_layout(
                    height=250,
                    margin=dict(l=0, r=0, t=10, b=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(color="#cdd6f4", gridcolor="#313244"),
                    yaxis=dict(color="#cdd6f4", gridcolor="#313244", range=[0, 110]),
                    font=dict(color="#cdd6f4"),
                )
                st.plotly_chart(fig3, use_container_width=True)

            st.caption(f"Last refreshed every {refresh}s  •  Data via PySpark Structured Streaming")

    except Exception as e:
        st.error(f"DB connection error: {e}")

    time.sleep(refresh)
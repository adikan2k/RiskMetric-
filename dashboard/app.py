"""
RiskMetric — Executive Watchdog Dashboard
Real-time fraud intelligence and risk attribution interface.
Powered by Streamlit + DuckDB + Plotly.
"""

import os
import sys
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# Page Config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="RiskMetric — Fraud Watchdog",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .main .block-container { padding-top: 1rem; }
    .metric-card {
        background: linear-gradient(135deg, #1e1e2f 0%, #2d2d44 100%);
        border-radius: 12px;
        padding: 20px;
        color: white;
        text-align: center;
        border: 1px solid rgba(255,255,255,0.1);
    }
    .metric-card h2 { margin: 0; font-size: 2rem; }
    .metric-card p { margin: 0; opacity: 0.7; font-size: 0.85rem; }
    .critical { border-left: 4px solid #ff4b4b; }
    .high { border-left: 4px solid #ffa726; }
    .medium { border-left: 4px solid #ffee58; }
    .low { border-left: 4px solid #66bb6a; }
    div[data-testid="stMetric"] {
        background-color: #0e1117;
        border: 1px solid #262730;
        border-radius: 8px;
        padding: 12px 16px;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Database Connection
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "riskmetric.duckdb")
PARQUET_PATH = os.path.join(PROJECT_ROOT, "data", "raw_transactions.parquet")


@st.cache_resource
def get_connection():
    """Get a read-only DuckDB connection."""
    if os.path.exists(DB_PATH):
        return duckdb.connect(DB_PATH, read_only=True)
    return None


def query_df(sql: str) -> pd.DataFrame:
    """Execute a SQL query and return a DataFrame."""
    conn = get_connection()
    if conn is None:
        st.error("Database not found. Run the pipeline first: `python run_pipeline.py`")
        st.stop()
    try:
        return conn.execute(sql).fetchdf()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/shield.png", width=60)
    st.title("RiskMetric")
    st.caption("Algorithmic Fraud Detection & Spatial-Temporal Risk Modeling")
    st.divider()

    page = st.radio(
        "Navigation",
        ["Overview", "Impossible Travel", "Velocity Spikes", "Behavioral Drift",
         "User Profiles", "Model Evaluation", "Triage Queue", "Raw Explorer"],
        index=0,
    )

    st.divider()
    st.markdown("**Pipeline Info**")
    if os.path.exists(DB_PATH):
        db_size = os.path.getsize(DB_PATH) / 1e6
        st.metric("DB Size", f"{db_size:.1f} MB")
    if os.path.exists(PARQUET_PATH):
        pq_size = os.path.getsize(PARQUET_PATH) / 1e6
        st.metric("Parquet Size", f"{pq_size:.1f} MB")

    st.divider()
    st.markdown(
        "Built with **DuckDB** · **dbt** · **Streamlit**",
    )


# ===========================================================================
# PAGE: Overview
# ===========================================================================
if page == "Overview":
    st.title("🛡️ RiskMetric — Executive Overview")
    st.caption("Real-time fraud intelligence across 1M+ transactions")

    # KPI Row
    col1, col2, col3, col4, col5 = st.columns(5)

    total_txns = query_df("SELECT COUNT(*) AS cnt FROM gold_risk_scores")
    flagged = query_df("SELECT COUNT(*) AS cnt FROM gold_risk_scores WHERE detected_fraud = TRUE")
    critical = query_df("SELECT COUNT(*) AS cnt FROM gold_risk_scores WHERE risk_tier = 'CRITICAL'")
    high = query_df("SELECT COUNT(*) AS cnt FROM gold_risk_scores WHERE risk_tier = 'HIGH'")
    avg_score = query_df("SELECT ROUND(AVG(risk_score), 2) AS avg FROM gold_risk_scores WHERE detected_fraud = TRUE")

    col1.metric("Total Transactions", f"{total_txns['cnt'].iloc[0]:,.0f}")
    col2.metric("Flagged Fraud", f"{flagged['cnt'].iloc[0]:,.0f}")
    col3.metric("Critical Alerts", f"{critical['cnt'].iloc[0]:,.0f}", delta="Immediate Review")
    col4.metric("High Risk", f"{high['cnt'].iloc[0]:,.0f}")
    col5.metric("Avg Risk Score (Flagged)", f"{avg_score['avg'].iloc[0]:.1f}")

    st.divider()

    # Charts Row 1
    left, right = st.columns(2)

    with left:
        st.subheader("Fraud by Archetype")
        archetype_df = query_df("""
            SELECT primary_fraud_attribution, COUNT(*) AS count
            FROM gold_fraud_attribution
            GROUP BY primary_fraud_attribution
            ORDER BY count DESC
        """)
        if not archetype_df.empty:
            fig = px.pie(
                archetype_df,
                names="primary_fraud_attribution",
                values="count",
                color_discrete_sequence=px.colors.qualitative.Set2,
                hole=0.45,
            )
            fig.update_layout(
                template="plotly_dark",
                margin=dict(t=20, b=20, l=20, r=20),
                height=350,
            )
            st.plotly_chart(fig, width="stretch")

    with right:
        st.subheader("Risk Tier Distribution")
        tier_df = query_df("""
            SELECT risk_tier, COUNT(*) AS count
            FROM gold_risk_scores
            WHERE detected_fraud = TRUE
            GROUP BY risk_tier
            ORDER BY
                CASE risk_tier
                    WHEN 'CRITICAL' THEN 1
                    WHEN 'HIGH' THEN 2
                    WHEN 'MEDIUM' THEN 3
                    WHEN 'LOW' THEN 4
                END
        """)
        if not tier_df.empty:
            colors = {"CRITICAL": "#ff4b4b", "HIGH": "#ffa726", "MEDIUM": "#ffee58", "LOW": "#66bb6a"}
            fig = px.bar(
                tier_df,
                x="risk_tier",
                y="count",
                color="risk_tier",
                color_discrete_map=colors,
            )
            fig.update_layout(
                template="plotly_dark",
                showlegend=False,
                margin=dict(t=20, b=20, l=20, r=20),
                height=350,
                xaxis_title="Risk Tier",
                yaxis_title="Flagged Transactions",
            )
            st.plotly_chart(fig, width="stretch")

    # Charts Row 2
    left2, right2 = st.columns(2)

    with left2:
        st.subheader("Fraud Timeline (Monthly)")
        timeline_df = query_df("""
            SELECT
                DATE_TRUNC('month', transaction_timestamp) AS month,
                primary_fraud_attribution AS archetype,
                COUNT(*) AS count
            FROM gold_fraud_attribution
            GROUP BY month, archetype
            ORDER BY month
        """)
        if not timeline_df.empty:
            fig = px.area(
                timeline_df,
                x="month",
                y="count",
                color="archetype",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_layout(
                template="plotly_dark",
                margin=dict(t=20, b=20, l=20, r=20),
                height=350,
                xaxis_title="",
                yaxis_title="Flagged Transactions",
            )
            st.plotly_chart(fig, width="stretch")

    with right2:
        st.subheader("Detection Accuracy")
        accuracy_df = query_df("""
            SELECT detection_accuracy, COUNT(*) AS count
            FROM gold_fraud_attribution
            GROUP BY detection_accuracy
        """)
        if not accuracy_df.empty:
            colors_acc = {"TRUE_POSITIVE": "#66bb6a", "FALSE_POSITIVE": "#ff4b4b", "UNKNOWN": "#888"}
            fig = px.pie(
                accuracy_df,
                names="detection_accuracy",
                values="count",
                color="detection_accuracy",
                color_discrete_map=colors_acc,
                hole=0.45,
            )
            fig.update_layout(
                template="plotly_dark",
                margin=dict(t=20, b=20, l=20, r=20),
                height=350,
            )
            st.plotly_chart(fig, width="stretch")

    # Top Fraud Cities
    st.subheader("🌍 Top 10 Cities by Fraud Volume")
    city_df = query_df("""
        SELECT city, country, COUNT(*) AS fraud_count, ROUND(AVG(risk_score), 1) AS avg_risk
        FROM gold_fraud_attribution
        GROUP BY city, country
        ORDER BY fraud_count DESC
        LIMIT 10
    """)
    if not city_df.empty:
        st.dataframe(city_df, width="stretch", hide_index=True)


# ===========================================================================
# PAGE: Impossible Travel
# ===========================================================================
elif page == "Impossible Travel":
    st.title("✈️ Impossible Travel Detection")
    st.caption("Spatial-temporal analysis flagging transactions exceeding 500 mph ground speed")

    col1, col2, col3 = st.columns(3)
    stats = query_df("""
        SELECT
            COUNT(*) AS total_flags,
            ROUND(AVG(ground_speed_mph), 0) AS avg_speed,
            ROUND(MAX(ground_speed_mph), 0) AS max_speed,
            ROUND(AVG(distance_miles), 0) AS avg_distance
        FROM gold_fraud_attribution
        WHERE flag_impossible_travel = TRUE
    """)
    if not stats.empty:
        col1.metric("Impossible Travel Flags", f"{stats['total_flags'].iloc[0]:,.0f}")
        col2.metric("Avg Ground Speed", f"{stats['avg_speed'].iloc[0]:,.0f} mph")
        col3.metric("Max Ground Speed", f"{stats['max_speed'].iloc[0]:,.0f} mph")

    st.divider()

    travel_df = query_df("""
        SELECT
            transaction_id, user_id, transaction_timestamp,
            amount, city, prev_city,
            ROUND(distance_miles, 1) AS distance_miles,
            ROUND(ground_speed_mph, 0) AS ground_speed_mph,
            ROUND(time_gap_hours * 60, 1) AS time_gap_minutes,
            risk_score
        FROM gold_fraud_attribution
        WHERE flag_impossible_travel = TRUE
        ORDER BY ground_speed_mph DESC
        LIMIT 500
    """)

    if not travel_df.empty:
        left, right = st.columns(2)
        with left:
            st.subheader("Speed Distribution")
            fig = px.histogram(
                travel_df, x="ground_speed_mph", nbins=50,
                color_discrete_sequence=["#ff4b4b"],
            )
            fig.update_layout(template="plotly_dark", height=350,
                              margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, width="stretch")

        with right:
            st.subheader("Distance vs Time Gap")
            fig = px.scatter(
                travel_df, x="time_gap_minutes", y="distance_miles",
                size="risk_score", color="ground_speed_mph",
                color_continuous_scale="Reds",
            )
            fig.update_layout(template="plotly_dark", height=350,
                              margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, width="stretch")

        st.subheader("Flagged Transactions")
        st.dataframe(travel_df, width="stretch", hide_index=True, height=400)

    # Map visualization
    st.subheader("🗺️ Fraud Hotspot Map")
    map_df = query_df("""
        SELECT latitude, longitude, city, risk_score, amount
        FROM gold_fraud_attribution
        WHERE flag_impossible_travel = TRUE
        AND latitude IS NOT NULL AND longitude IS NOT NULL
        LIMIT 2000
    """)
    if not map_df.empty:
        fig = px.scatter_geo(
            map_df, lat="latitude", lon="longitude",
            size="amount", color="risk_score",
            color_continuous_scale="Reds",
            projection="natural earth",
            hover_data=["city", "amount", "risk_score"],
        )
        fig.update_layout(
            template="plotly_dark", height=450,
            margin=dict(t=20, b=20, l=20, r=20),
            geo=dict(bgcolor="#0e1117", lakecolor="#0e1117"),
        )
        st.plotly_chart(fig, width="stretch")


# ===========================================================================
# PAGE: Velocity Spikes
# ===========================================================================
elif page == "Velocity Spikes":
    st.title("⚡ Velocity Spike Detection")
    st.caption("Card-testing behavior: 10+ micro-transactions within 60 seconds")

    col1, col2, col3 = st.columns(3)
    stats = query_df("""
        SELECT
            COUNT(*) AS total_flags,
            ROUND(AVG(txn_count_60s), 1) AS avg_burst_size,
            ROUND(AVG(amount), 2) AS avg_amount
        FROM gold_fraud_attribution
        WHERE flag_velocity_spike = TRUE
    """)
    if not stats.empty:
        col1.metric("Velocity Spike Flags", f"{stats['total_flags'].iloc[0]:,.0f}")
        col2.metric("Avg Burst Size", f"{stats['avg_burst_size'].iloc[0]:.0f} txns/60s")
        col3.metric("Avg Transaction Amount", f"${stats['avg_amount'].iloc[0]:.2f}")

    st.divider()

    velocity_df = query_df("""
        SELECT
            user_id,
            MIN(transaction_timestamp) AS burst_start,
            COUNT(*) AS txn_count,
            ROUND(SUM(amount), 2) AS total_amount,
            ROUND(AVG(amount), 2) AS avg_amount,
            city,
            MAX(risk_score) AS max_risk_score
        FROM gold_fraud_attribution
        WHERE flag_velocity_spike = TRUE
        GROUP BY user_id, city, DATE_TRUNC('minute', transaction_timestamp)
        ORDER BY txn_count DESC
        LIMIT 200
    """)

    if not velocity_df.empty:
        left, right = st.columns(2)
        with left:
            st.subheader("Burst Size Distribution")
            fig = px.histogram(
                velocity_df, x="txn_count", nbins=30,
                color_discrete_sequence=["#ffa726"],
            )
            fig.update_layout(template="plotly_dark", height=350,
                              margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, width="stretch")

        with right:
            st.subheader("Amount per Burst")
            fig = px.scatter(
                velocity_df, x="txn_count", y="total_amount",
                color="max_risk_score", size="txn_count",
                color_continuous_scale="OrRd",
            )
            fig.update_layout(template="plotly_dark", height=350,
                              margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, width="stretch")

        st.subheader("Velocity Spike Events")
        st.dataframe(velocity_df, width="stretch", hide_index=True, height=400)


# ===========================================================================
# PAGE: Behavioral Drift
# ===========================================================================
elif page == "Behavioral Drift":
    st.title("📈 Behavioral Drift Detection")
    st.caption("Z-Score analysis: transactions > 3σ from 30-day rolling average")

    col1, col2, col3 = st.columns(3)
    stats = query_df("""
        SELECT
            COUNT(*) AS total_flags,
            ROUND(AVG(z_score), 2) AS avg_z_score,
            ROUND(MAX(z_score), 2) AS max_z_score
        FROM gold_fraud_attribution
        WHERE flag_behavioral_drift = TRUE
    """)
    if not stats.empty:
        col1.metric("Behavioral Drift Flags", f"{stats['total_flags'].iloc[0]:,.0f}")
        col2.metric("Avg Z-Score", f"{stats['avg_z_score'].iloc[0]:.2f}σ")
        col3.metric("Max Z-Score", f"{stats['max_z_score'].iloc[0]:.2f}σ")

    st.divider()

    drift_df = query_df("""
        SELECT
            transaction_id, user_id, transaction_timestamp,
            ROUND(amount, 2) AS amount,
            ROUND(rolling_avg_30d, 2) AS rolling_avg_30d,
            ROUND(z_score, 2) AS z_score,
            merchant_category, city, risk_score
        FROM gold_fraud_attribution
        WHERE flag_behavioral_drift = TRUE
        ORDER BY z_score DESC
        LIMIT 500
    """)

    if not drift_df.empty:
        left, right = st.columns(2)
        with left:
            st.subheader("Z-Score Distribution")
            fig = px.histogram(
                drift_df, x="z_score", nbins=50,
                color_discrete_sequence=["#ab47bc"],
            )
            fig.add_vline(x=3, line_dash="dash", line_color="red", annotation_text="3σ threshold")
            fig.update_layout(template="plotly_dark", height=350,
                              margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, width="stretch")

        with right:
            st.subheader("Amount vs Rolling Average")
            fig = px.scatter(
                drift_df, x="rolling_avg_30d", y="amount",
                color="z_score", size="risk_score",
                color_continuous_scale="Purples",
            )
            fig.update_layout(template="plotly_dark", height=350,
                              margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, width="stretch")

        st.subheader("Flagged Drift Transactions")
        st.dataframe(drift_df, width="stretch", hide_index=True, height=400)


# ===========================================================================
# PAGE: User Profiles
# ===========================================================================
elif page == "User Profiles":
    st.title("👤 User Risk Profiles")
    st.caption("Holistic risk view per user aggregated across all fraud signals")

    col1, col2, col3, col4 = st.columns(4)
    tier_counts = query_df("""
        SELECT user_risk_tier, COUNT(*) AS cnt
        FROM gold_user_risk_profiles
        GROUP BY user_risk_tier
        ORDER BY
            CASE user_risk_tier
                WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2
                WHEN 'MEDIUM' THEN 3 WHEN 'LOW' THEN 4
            END
    """)
    tiers = dict(zip(tier_counts["user_risk_tier"], tier_counts["cnt"])) if not tier_counts.empty else {}
    col1.metric("Critical Users", f"{tiers.get('CRITICAL', 0):,}")
    col2.metric("High Risk Users", f"{tiers.get('HIGH', 0):,}")
    col3.metric("Medium Risk Users", f"{tiers.get('MEDIUM', 0):,}")
    col4.metric("Low Risk Users", f"{tiers.get('LOW', 0):,}")

    st.divider()

    # Risk tier filter
    selected_tier = st.selectbox("Filter by Risk Tier", ["ALL", "CRITICAL", "HIGH", "MEDIUM", "LOW"])
    tier_filter = f"WHERE user_risk_tier = '{selected_tier}'" if selected_tier != "ALL" else ""

    profiles_df = query_df(f"""
        SELECT
            user_id, home_city, home_country, user_risk_tier,
            total_transactions, ROUND(total_spend, 2) AS total_spend,
            impossible_travel_count, velocity_spike_count, behavioral_drift_count,
            total_flags, ROUND(avg_risk_score, 1) AS avg_risk_score,
            max_risk_score, fraud_rate_pct
        FROM gold_user_risk_profiles
        {tier_filter}
        ORDER BY max_risk_score DESC, total_flags DESC
        LIMIT 500
    """)

    if not profiles_df.empty:
        left, right = st.columns(2)
        with left:
            st.subheader("User Risk Score Distribution")
            fig = px.histogram(
                profiles_df, x="avg_risk_score", nbins=50,
                color_discrete_sequence=["#26c6da"],
            )
            fig.update_layout(template="plotly_dark", height=350,
                              margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, width="stretch")

        with right:
            st.subheader("Flags by Type")
            flag_sums = pd.DataFrame({
                "Archetype": ["Impossible Travel", "Velocity Spike", "Behavioral Drift"],
                "Count": [
                    profiles_df["impossible_travel_count"].sum(),
                    profiles_df["velocity_spike_count"].sum(),
                    profiles_df["behavioral_drift_count"].sum(),
                ],
            })
            fig = px.bar(
                flag_sums, x="Archetype", y="Count",
                color="Archetype",
                color_discrete_sequence=["#ff4b4b", "#ffa726", "#ab47bc"],
            )
            fig.update_layout(template="plotly_dark", showlegend=False, height=350,
                              margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, width="stretch")

        st.subheader("User Profiles Table")
        st.dataframe(profiles_df, width="stretch", hide_index=True, height=500)


# ===========================================================================
# PAGE: Model Evaluation
# ===========================================================================
elif page == "Model Evaluation":
    st.title("📊 Model Evaluation & Calibration")
    st.caption("Precision, Recall, F1-Score, and confusion matrix against ground-truth labels")

    # Evaluation metrics
    eval_df = query_df("SELECT * FROM gold_model_evaluation ORDER BY archetype")

    if not eval_df.empty:
        # KPI row for OVERALL
        overall = eval_df[eval_df["archetype"] == "OVERALL"]
        if not overall.empty:
            o = overall.iloc[0]
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Precision", f"{o['precision_score']:.4f}")
            col2.metric("Recall", f"{o['recall_score']:.4f}")
            col3.metric("F1-Score", f"{o['f1_score']:.4f}")
            col4.metric("Accuracy", f"{o['accuracy']:.6f}")
            col5.metric("False Positive Rate", f"{o['false_positive_rate']:.6f}")

        st.divider()

        # Per-archetype metrics
        left, right = st.columns(2)
        with left:
            st.subheader("Classification Metrics by Archetype")
            archetypes = eval_df[eval_df["archetype"] != "OVERALL"]
            if not archetypes.empty:
                metrics_melted = archetypes.melt(
                    id_vars=["archetype"],
                    value_vars=["precision_score", "recall_score", "f1_score"],
                    var_name="metric",
                    value_name="value",
                )
                metrics_melted["metric"] = metrics_melted["metric"].str.replace("_score", "").str.title()
                fig = px.bar(
                    metrics_melted, x="archetype", y="value", color="metric",
                    barmode="group",
                    color_discrete_sequence=["#66bb6a", "#42a5f5", "#ab47bc"],
                )
                fig.update_layout(
                    template="plotly_dark", height=400,
                    margin=dict(t=20, b=20, l=20, r=20),
                    yaxis_title="Score", xaxis_title="",
                    yaxis=dict(range=[0, 1.05]),
                )
                st.plotly_chart(fig, width="stretch")

        with right:
            st.subheader("Confusion Matrix (Overall)")
            if not overall.empty:
                o = overall.iloc[0]
                cm = [[int(o["true_positives"]), int(o["false_negatives"])],
                      [int(o["false_positives"]), int(o["true_negatives"])]]
                fig = go.Figure(data=go.Heatmap(
                    z=cm, x=["Predicted Fraud", "Predicted Legit"],
                    y=["Actual Fraud", "Actual Legit"],
                    text=[[f"{cm[0][0]:,}", f"{cm[0][1]:,}"],
                          [f"{cm[1][0]:,}", f"{cm[1][1]:,}"]],
                    texttemplate="%{text}",
                    colorscale=[[0, "#1a1a2e"], [0.5, "#16213e"], [1, "#e94560"]],
                    showscale=False,
                ))
                fig.update_layout(
                    template="plotly_dark", height=400,
                    margin=dict(t=20, b=20, l=20, r=20),
                    xaxis_title="Predicted", yaxis_title="Actual",
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig, width="stretch")

        # Full metrics table
        st.subheader("Detailed Metrics Table")
        display_cols = ["archetype", "true_positives", "false_positives", "false_negatives",
                        "true_negatives", "precision_score", "recall_score", "f1_score",
                        "accuracy", "false_positive_rate"]
        st.dataframe(eval_df[display_cols], width="stretch", hide_index=True)

        st.divider()

        # Threshold calibration curves
        st.subheader("Threshold Calibration Analysis")
        st.caption("Precision-Recall tradeoff across different detection thresholds")

        calib_df = query_df("SELECT * FROM gold_threshold_calibration ORDER BY archetype, threshold_value")

        if not calib_df.empty:
            for archetype in calib_df["archetype"].unique():
                arch_df = calib_df[calib_df["archetype"] == archetype]
                unit = arch_df["threshold_unit"].iloc[0]

                st.markdown(f"**{archetype.replace('_', ' ').title()}** (threshold unit: `{unit}`)")
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=arch_df["threshold_value"], y=arch_df["precision_score"],
                    name="Precision", mode="lines+markers", line=dict(color="#66bb6a"),
                ))
                fig.add_trace(go.Scatter(
                    x=arch_df["threshold_value"], y=arch_df["recall_score"],
                    name="Recall", mode="lines+markers", line=dict(color="#42a5f5"),
                ))
                fig.add_trace(go.Scatter(
                    x=arch_df["threshold_value"], y=arch_df["f1_score"],
                    name="F1", mode="lines+markers", line=dict(color="#ab47bc", width=3),
                ))
                fig.update_layout(
                    template="plotly_dark", height=300,
                    margin=dict(t=20, b=20, l=20, r=20),
                    xaxis_title=f"Threshold ({unit})", yaxis_title="Score",
                    yaxis=dict(range=[0, 1.05]),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig, width="stretch")


# ===========================================================================
# PAGE: Triage Queue
# ===========================================================================
elif page == "Triage Queue":
    st.title("🚨 Triage Queue — Alert Case Management")
    st.caption("Operational workflow for reviewing and dispositioning flagged transactions")

    # Initialize session state for triage decisions
    if "triage_decisions" not in st.session_state:
        st.session_state.triage_decisions = {}

    # Filters
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        triage_tier = st.selectbox("Risk Tier", ["CRITICAL", "HIGH", "MEDIUM", "ALL"], index=0)
    with filter_col2:
        triage_archetype = st.selectbox(
            "Fraud Type",
            ["ALL", "Impossible Travel", "Velocity Spike", "Behavioral Drift"],
        )
    with filter_col3:
        triage_sort = st.selectbox("Sort By", ["risk_score DESC", "amount DESC", "transaction_timestamp DESC"])

    tier_where = f"AND risk_tier = '{triage_tier}'" if triage_tier != "ALL" else ""
    arch_where = ""
    if triage_archetype == "Impossible Travel":
        arch_where = "AND flag_impossible_travel = TRUE"
    elif triage_archetype == "Velocity Spike":
        arch_where = "AND flag_velocity_spike = TRUE"
    elif triage_archetype == "Behavioral Drift":
        arch_where = "AND flag_behavioral_drift = TRUE"

    queue_df = query_df(f"""
        SELECT
            transaction_id, user_id, transaction_timestamp,
            ROUND(amount, 2) AS amount, merchant_name, merchant_category,
            city, country, risk_score, risk_tier,
            primary_fraud_attribution,
            ROUND(ground_speed_mph, 0) AS ground_speed_mph,
            txn_count_60s,
            ROUND(z_score, 2) AS z_score,
            detection_accuracy
        FROM gold_fraud_attribution
        WHERE 1=1 {tier_where} {arch_where}
        ORDER BY {triage_sort}
        LIMIT 100
    """)

    if queue_df.empty:
        st.info("No alerts match the selected filters.")
    else:
        # Summary bar
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Alerts in Queue", f"{len(queue_df)}")
        reviewed = sum(1 for v in st.session_state.triage_decisions.values() if v != "NEW")
        col2.metric("Reviewed", f"{reviewed}")
        confirmed = sum(1 for v in st.session_state.triage_decisions.values() if v == "CONFIRMED_FRAUD")
        col3.metric("Confirmed Fraud", f"{confirmed}")
        false_pos = sum(1 for v in st.session_state.triage_decisions.values() if v == "FALSE_POSITIVE")
        col4.metric("False Positives", f"{false_pos}")

        st.divider()

        # Alert cards
        for idx, row in queue_df.iterrows():
            txn_id = row["transaction_id"]
            current_status = st.session_state.triage_decisions.get(txn_id, "NEW")

            # Status color coding
            status_colors = {
                "NEW": "🔴", "INVESTIGATING": "🟡",
                "CONFIRMED_FRAUD": "🟢", "FALSE_POSITIVE": "⚪",
                "ESCALATED": "🟠",
            }
            status_icon = status_colors.get(current_status, "⚪")

            with st.expander(
                f"{status_icon} **{row['risk_tier']}** | {row['primary_fraud_attribution']} | "
                f"${row['amount']:,.2f} | {row['city']}, {row['country']} | "
                f"Score: {row['risk_score']}",
                expanded=(current_status == "NEW" and idx < 3),
            ):
                detail_left, detail_right = st.columns([2, 1])

                with detail_left:
                    st.markdown(f"""
| Field | Value |
|---|---|
| **Transaction ID** | `{txn_id[:16]}...` |
| **User ID** | `{row['user_id']}` |
| **Timestamp** | {row['transaction_timestamp']} |
| **Amount** | ${row['amount']:,.2f} |
| **Merchant** | {row['merchant_name']} ({row['merchant_category']}) |
| **Location** | {row['city']}, {row['country']} |
| **Risk Score** | {row['risk_score']} ({row['risk_tier']}) |
| **Attribution** | {row['primary_fraud_attribution']} |
| **Ground Truth** | {row['detection_accuracy']} |
""")
                    # Evidence summary
                    evidence = []
                    if row.get("ground_speed_mph") and pd.notna(row["ground_speed_mph"]):
                        evidence.append(f"Ground Speed: {row['ground_speed_mph']:,.0f} mph")
                    if row.get("txn_count_60s") and pd.notna(row["txn_count_60s"]):
                        evidence.append(f"Burst: {int(row['txn_count_60s'])} txns/60s")
                    if row.get("z_score") and pd.notna(row["z_score"]):
                        evidence.append(f"Z-Score: {row['z_score']:.2f}σ")
                    if evidence:
                        st.markdown("**Evidence:** " + " · ".join(evidence))

                with detail_right:
                    st.markdown("**Disposition**")
                    new_status = st.selectbox(
                        "Status",
                        ["NEW", "INVESTIGATING", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "ESCALATED"],
                        index=["NEW", "INVESTIGATING", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "ESCALATED"].index(current_status),
                        key=f"status_{txn_id}",
                    )
                    analyst_notes = st.text_area(
                        "Analyst Notes",
                        placeholder="Add investigation notes...",
                        height=80,
                        key=f"notes_{txn_id}",
                    )
                    if st.button("Save Decision", key=f"save_{txn_id}", type="primary"):
                        st.session_state.triage_decisions[txn_id] = new_status
                        st.success(f"Saved: {new_status}")

        # Export triage decisions
        st.divider()
        if st.session_state.triage_decisions:
            st.subheader("Triage Summary")
            triage_summary = pd.DataFrame([
                {"transaction_id": k, "disposition": v}
                for k, v in st.session_state.triage_decisions.items()
            ])
            disposition_counts = triage_summary["disposition"].value_counts().reset_index()
            disposition_counts.columns = ["Disposition", "Count"]

            col_l, col_r = st.columns(2)
            with col_l:
                st.dataframe(disposition_counts, width="stretch", hide_index=True)
            with col_r:
                colors_disp = {
                    "NEW": "#ff4b4b", "INVESTIGATING": "#ffa726",
                    "CONFIRMED_FRAUD": "#66bb6a", "FALSE_POSITIVE": "#888",
                    "ESCALATED": "#ff7043",
                }
                fig = px.pie(
                    disposition_counts, names="Disposition", values="Count",
                    color="Disposition", color_discrete_map=colors_disp,
                    hole=0.45,
                )
                fig.update_layout(
                    template="plotly_dark", height=250,
                    margin=dict(t=20, b=20, l=20, r=20),
                )
                st.plotly_chart(fig, width="stretch")

            st.download_button(
                "Download Triage Decisions (CSV)",
                data=triage_summary.to_csv(index=False),
                file_name="triage_decisions.csv",
                mime="text/csv",
            )


# ===========================================================================
# PAGE: Raw Explorer
# ===========================================================================
elif page == "Raw Explorer":
    st.title("🔍 Raw Data Explorer")
    st.caption("Query the DuckDB warehouse directly")

    default_query = """SELECT *
FROM gold_risk_scores
WHERE detected_fraud = TRUE
ORDER BY risk_score DESC
LIMIT 100"""

    query = st.text_area("SQL Query", value=default_query, height=150)

    if st.button("Run Query", type="primary"):
        with st.spinner("Executing..."):
            result = query_df(query)
            st.success(f"Returned {len(result):,} rows")
            st.dataframe(result, width="stretch", hide_index=True, height=500)

"""
Reddit Community Collapse Analysis Dashboard
============================================
A high-performance, beautifully styled Streamlit analytics dashboard exploring
early-warning signals of online community decline using temporal graph analytics.

Big Data Architecture: HDFS -> Apache Pig -> Apache Hive -> Apache Spark GraphX -> Streamlit
"""

import os
import math
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import networkx as nx

# -----------------------------------------------------------------------------
# 1. PAGE CONFIGURATION & THEME STYLING
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Reddit Community Collapse Analytics",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom High-End Modern CSS (Dark/Glassmorphic Palette)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    /* Gradient Headers */
    .hero-title {
        font-size: 2.35rem;
        font-weight: 800;
        background: linear-gradient(135deg, #60A5FA 0%, #A78BFA 50%, #F472B6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.25rem;
        letter-spacing: -0.02em;
    }

    .hero-subtitle {
        font-size: 1.05rem;
        color: #94A3B8;
        margin-bottom: 1.5rem;
        font-weight: 400;
        line-height: 1.5;
    }

    /* Glassmorphic Metric Cards */
    .metric-card {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.7) 0%, rgba(15, 23, 42, 0.8) 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 1.25rem 1.4rem;
        box-shadow: 0 8px 24px -4px rgba(0, 0, 0, 0.35);
        backdrop-filter: blur(12px);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }

    .metric-card:hover {
        transform: translateY(-2px);
        border-color: rgba(99, 102, 241, 0.4);
    }

    .metric-label {
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94A3B8;
        font-weight: 600;
        margin-bottom: 0.35rem;
    }

    .metric-value {
        font-size: 1.85rem;
        font-weight: 700;
        color: #F8FAFC;
        letter-spacing: -0.02em;
    }

    .metric-delta {
        font-size: 0.85rem;
        margin-top: 0.35rem;
        display: flex;
        align-items: center;
        gap: 0.35rem;
        font-weight: 500;
    }

    .delta-declining {
        color: #F87171;
    }

    .delta-growing {
        color: #34D399;
    }

    .delta-neutral {
        color: #818CF8;
    }

    /* Badges */
    .badge {
        display: inline-block;
        padding: 0.25rem 0.65rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }

    .badge-declining {
        background-color: rgba(239, 68, 68, 0.15);
        color: #F87171;
        border: 1px solid rgba(239, 68, 68, 0.3);
    }

    .badge-stable {
        background-color: rgba(16, 185, 129, 0.15);
        color: #34D399;
        border: 1px solid rgba(16, 185, 129, 0.3);
    }

    /* Callout Card */
    .callout-box {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.5) 0%, rgba(51, 65, 85, 0.3) 100%);
        border-left: 4px solid #6366F1;
        border-radius: 0 10px 10px 0;
        padding: 1rem 1.25rem;
        margin: 1rem 0;
        color: #E2E8F0;
        font-size: 0.95rem;
        line-height: 1.6;
    }

    .callout-highlight {
        color: #A5B4FC;
        font-weight: 600;
    }

    /* Architecture Pipeline Flow */
    .pipeline-step {
        background: rgba(30, 41, 59, 0.6);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 10px;
        padding: 1rem;
        text-align: center;
    }

    .pipeline-step h4 {
        margin: 0 0 0.4rem 0;
        color: #38BDF8;
        font-size: 1rem;
    }

    .pipeline-step p {
        margin: 0;
        font-size: 0.8rem;
        color: #94A3B8;
    }

    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    ::-webkit-scrollbar-track {
        background: #0f172a;
    }
    ::-webkit-scrollbar-thumb {
        background: #334155;
        border-radius: 4px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: #475569;
    }
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# 2. DATA LOADING & CACHING BACKEND
# -----------------------------------------------------------------------------
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

@st.cache_data(show_spinner=False)
def load_cohort_comparison():
    path = os.path.join(DATA_DIR, "cohort_comparison.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()

@st.cache_data(show_spinner=False)
def load_degree_decline():
    path = os.path.join(DATA_DIR, "degree_decline.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()

@st.cache_data(show_spinner=False)
def load_community_metrics():
    path = os.path.join(DATA_DIR, "community_metrics.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
        # Ensure correct numeric types
        df['early_activity'] = pd.to_numeric(df['early_activity'], errors='coerce')
        df['late_activity'] = pd.to_numeric(df['late_activity'], errors='coerce')
        df['decline_percentage'] = pd.to_numeric(df['decline_percentage'], errors='coerce')
        df['early_degree'] = pd.to_numeric(df['early_degree'], errors='coerce')
        df['early_pagerank'] = pd.to_numeric(df['early_pagerank'], errors='coerce')
        df['early_component'] = pd.to_numeric(df['early_component'], errors='coerce')
        return df
    return pd.DataFrame()

@st.cache_data(show_spinner=False)
def load_monthly_activity():
    path = os.path.join(DATA_DIR, "monthly_activity.csv")
    if os.path.exists(path):
        df = pd.read_csv(
            path,
            dtype={'source_subreddit': 'category', 'year': 'str', 'month': 'str', 'interactions': 'int32'}
        )
        df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2) + '-01')
        return df
    return pd.DataFrame()

@st.cache_data(show_spinner=False)
def load_network_edges():
    path = os.path.join(DATA_DIR, "network_edges.csv")
    if os.path.exists(path):
        df = pd.read_csv(path, dtype={'source_subreddit': 'category', 'target_subreddit': 'category', 'interaction_count': 'int32'})
        return df
    return pd.DataFrame()

# Load main datasets
df_cohort = load_cohort_comparison()
df_degree_decline = load_degree_decline()
df_metrics = load_community_metrics()
df_monthly = load_monthly_activity()
df_edges = load_network_edges()


# -----------------------------------------------------------------------------
# 3. HELPER FUNCTIONS & ANALYTICS UTILS
# -----------------------------------------------------------------------------
PALETTE = {
    "declining": "#EF4444",
    "declining_light": "rgba(239, 68, 68, 0.2)",
    "stable": "#10B981",
    "stable_light": "rgba(16, 185, 129, 0.2)",
    "primary": "#6366F1",
    "accent": "#38BDF8",
    "warning": "#F59E0B",
    "bg_dark": "#0F172A",
    "card_dark": "#1E293B",
    "text_muted": "#94A3B8"
}

def render_metric_card(label, value, delta_text="", delta_type="neutral"):
    delta_class = f"delta-{delta_type}"
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {f'<div class="metric-delta {delta_class}">{delta_text}</div>' if delta_text else ''}
    </div>
    """, unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# 4. SIDEBAR NAVIGATION & FILTERS
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 0.75rem; margin-bottom: 1.25rem;">
        <span style="font-size: 2rem;">🌐</span>
        <div>
            <div style="font-weight: 800; font-size: 1.15rem; color: #F8FAFC;">Reddit Collapse</div>
            <div style="font-size: 0.75rem; color: #94A3B8; letter-spacing: 0.05em; text-transform: uppercase;">Big Data Analytics</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        [
            "🌟 Executive Overview",
            "🔍 Community Explorer",
            "📉 Decline Analysis",
            "🕸️ Network Topology",
            "🛠️ Big Data Pipeline"
        ],
        label_visibility="collapsed"
    )

    st.markdown("---")

    st.markdown("""
    <div style="padding: 0.75rem; background: rgba(30, 41, 59, 0.4); border-radius: 8px; font-size: 0.8rem; color: #94A3B8; line-height: 1.5;">
        <strong style="color: #E2E8F0;">Dataset Scope</strong><br>
        • Interaction Period: Dec 2013 – Apr 2017<br>
        • Early Period: Dec 2013 – Aug 2015<br>
        • Late Period: Sep 2015 – Apr 2017<br>
        • Cohort Cutoff: ≥ 50 Early Interactions<br>
        • Analyzed Communities: <strong>387</strong>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.caption("Stanford Network Analysis Project (SNAP)")
    st.caption("Pipeline: HDFS → Pig → Hive → GraphX")


# =============================================================================
# PAGE 1: EXECUTIVE OVERVIEW
# =============================================================================
if page == "🌟 Executive Overview":
    st.markdown('<div class="hero-title">When Online Communities Collapse</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero-subtitle">Early warning signals of online community activity decline using temporal graph analytics on the Reddit Hyperlink Network.</div>', unsafe_allow_html=True)

    # Top KPI Metrics Row
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        render_metric_card("Analyzed Cohort", "387", "Threshold ≥ 50 calls", "neutral")
    with c2:
        render_metric_card("Declining Cohort", "215", "55.6% of communities", "declining")
    with c3:
        render_metric_card("Stable / Growing", "172", "44.4% of communities", "growing")
    with c4:
        render_metric_card("Early Vertices", "19,747", "Active subreddits", "neutral")
    with c5:
        render_metric_card("Early Directed Edges", "65,696", "Cross-hyperlinks", "neutral")
    with c6:
        render_metric_card("Early Components", "339", "Connected clusters", "neutral")

    st.markdown("""
    <div class="callout-box">
        💡 <strong>Key Research Finding:</strong> Subreddits that subsequently suffered severe activity decline exhibited 
        <span class="callout-highlight">lower average early degree (106.9 vs 120.4)</span> and 
        <span class="callout-highlight">lower early PageRank (10.86 vs 12.37)</span> compared to stable or growing communities. 
        However, weak correlation coefficients (<em>r = -0.0933</em> for degree, <em>r = -0.1054</em> for PageRank) reveal that 
        <strong>early graph centrality alone is not an absolute predictor</strong> of individual survival.
    </div>
    """, unsafe_allow_html=True)

    st.markdown("### 📊 Cohort Comparison: Declining vs Stable / Growing")

    col_chart1, col_chart2 = st.columns([3, 2])

    with col_chart1:
        # Grouped bar chart comparing key metrics
        comp_metrics = [
            {"Metric": "Early Activity", "DECLINING": 154.95, "STABLE_OR_GROWING": 144.00},
            {"Metric": "Late Activity", "DECLINING": 75.00, "STABLE_OR_GROWING": 242.05},
            {"Metric": "Early Degree", "DECLINING": 106.93, "STABLE_OR_GROWING": 120.41},
            {"Metric": "Early PageRank (x10)", "DECLINING": 108.61, "STABLE_OR_GROWING": 123.72},
        ]
        df_comp = pd.DataFrame(comp_metrics)

        fig_comp = go.Figure()
        fig_comp.add_trace(go.Bar(
            x=df_comp["Metric"],
            y=df_comp["DECLINING"],
            name="Declining (N=215)",
            marker_color=PALETTE["declining"],
            text=df_comp["DECLINING"].apply(lambda v: f"{v:.1f}"),
            textposition="auto"
        ))
        fig_comp.add_trace(go.Bar(
            x=df_comp["Metric"],
            y=df_comp["STABLE_OR_GROWING"],
            name="Stable / Growing (N=172)",
            marker_color=PALETTE["stable"],
            text=df_comp["STABLE_OR_GROWING"].apply(lambda v: f"{v:.1f}"),
            textposition="auto"
        ))

        fig_comp.update_layout(
            barmode="group",
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(30,41,59,0.3)",
            height=360,
            margin=dict(l=20, r=20, t=30, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            yaxis_title="Average Metric Value"
        )
        st.plotly_chart(fig_comp, use_container_width=True)

    with col_chart2:
        # Cohort Donut + Avg Decline
        labels = ['Declining Communities', 'Stable / Growing']
        values = [215, 172]
        colors = [PALETTE["declining"], PALETTE["stable"]]

        fig_donut = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            hole=.6,
            marker_colors=colors,
            textinfo='label+percent',
            hoverinfo='label+value+percent'
        )])
        fig_donut.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            height=360,
            margin=dict(l=20, r=20, t=30, b=20),
            showlegend=False,
            annotations=[dict(text="387<br><span style='font-size:12px;color:#94A3B8'>Communities</span>", x=0.5, y=0.5, font_size=20, showarrow=False)]
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    st.markdown("---")

    # Big Data Architecture Flow
    st.markdown("### 🏗️ Big Data Processing Architecture")
    p1, p2, p3, p4, p5 = st.columns(5)
    with p1:
        st.markdown("""
        <div class="pipeline-step">
            <span style="font-size: 1.6rem;">🗄️</span>
            <h4>1. HDFS</h4>
            <p><strong>Storage Engine</strong><br>Distributed ingestion of SNAP Reddit Hyperlink TSV (~300MB, 286k records).</p>
        </div>
        """, unsafe_allow_html=True)
    with p2:
        st.markdown("""
        <div class="pipeline-step">
            <span style="font-size: 1.6rem;">🐷</span>
            <h4>2. Apache Pig</h4>
            <p><strong>Batch ETL</strong><br>Header stripping, lowercase normalization, date parsing, missing-value filtration.</p>
        </div>
        """, unsafe_allow_html=True)
    with p3:
        st.markdown("""
        <div class="pipeline-step">
            <span style="font-size: 1.6rem;">🐝</span>
            <h4>3. Apache Hive</h4>
            <p><strong>Temporal Analytics</strong><br>Aggregating monthly counts, establishing Early/Late cohorts & decline % metrics.</p>
        </div>
        """, unsafe_allow_html=True)
    with p4:
        st.markdown("""
        <div class="pipeline-step">
            <span style="font-size: 1.6rem;">⚡</span>
            <h4>4. Spark GraphX</h4>
            <p><strong>Graph Processing</strong><br>Directed graph building, PageRank, Degree centrality, and Connected Components.</p>
        </div>
        """, unsafe_allow_html=True)
    with p5:
        st.markdown("""
        <div class="pipeline-step">
            <span style="font-size: 1.6rem;">📈</span>
            <h4>5. Streamlit</h4>
            <p><strong>Interactive UI</strong><br>Ego-network visualizer, longitudinal activity drill-down & hypothesis validation.</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Aggregate Cohort Table
    st.markdown("### 📋 Cohort Benchmark Summary")
    st.dataframe(
        df_cohort.rename(columns={
            "outcome": "Outcome Cohort",
            "communities": "Community Count",
            "avg_early_activity": "Avg Early Activity",
            "avg_late_activity": "Avg Late Activity",
            "avg_decline_percentage": "Avg Decline %",
            "avg_early_degree": "Avg Early Degree",
            "avg_early_pagerank": "Avg Early PageRank"
        }),
        use_container_width=True,
        hide_index=True
    )


# =============================================================================
# PAGE 2: COMMUNITY EXPLORER
# =============================================================================
elif page == "🔍 Community Explorer":
    st.markdown('<div class="hero-title">Community Explorer & Longitudinal Drilldown</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero-subtitle">Inspect individual subreddits, compare early vs late lifecycle dynamics, and inspect their immediate hyperlink network.</div>', unsafe_allow_html=True)

    # Community Selector with Quick Filters
    col_filter1, col_filter2, col_filter3 = st.columns([2, 1, 1])

    with col_filter2:
        outcome_filter = st.selectbox("Filter Cohort", ["All Communities", "DECLINING Only", "STABLE_OR_GROWING Only"])

    with col_filter3:
        sort_by = st.selectbox("Sort Order", ["Decline % (Highest first)", "Early Activity", "Early Degree", "Early PageRank", "Alphabetical"])

    filtered_subreddits = df_metrics.copy()
    if outcome_filter == "DECLINING Only":
        filtered_subreddits = filtered_subreddits[filtered_subreddits['outcome'] == 'DECLINING']
    elif outcome_filter == "STABLE_OR_GROWING Only":
        filtered_subreddits = filtered_subreddits[filtered_subreddits['outcome'] == 'STABLE_OR_GROWING']

    if sort_by == "Decline % (Highest first)":
        filtered_subreddits = filtered_subreddits.sort_values(by="decline_percentage", ascending=False)
    elif sort_by == "Early Activity":
        filtered_subreddits = filtered_subreddits.sort_values(by="early_activity", ascending=False)
    elif sort_by == "Early Degree":
        filtered_subreddits = filtered_subreddits.sort_values(by="early_degree", ascending=False)
    elif sort_by == "Early PageRank":
        filtered_subreddits = filtered_subreddits.sort_values(by="early_pagerank", ascending=False)
    else:
        filtered_subreddits = filtered_subreddits.sort_values(by="source_subreddit", ascending=True)

    subreddit_list = filtered_subreddits['source_subreddit'].tolist()

    with col_filter1:
        # Default to a high-profile interesting community
        default_index = 0
        if "3ds" in subreddit_list:
            default_index = subreddit_list.index("3ds")
        elif len(subreddit_list) > 0:
            default_index = 0

        selected_sub = st.selectbox(
            "Select Subreddit to Inspect",
            subreddit_list,
            index=default_index,
            help="Choose from the 387 analyzed communities with >= 50 early interactions"
        )

    if selected_sub:
        sub_row = df_metrics[df_metrics['source_subreddit'] == selected_sub].iloc[0]

        is_declining = (sub_row['outcome'] == 'DECLINING')
        badge_class = "badge-declining" if is_declining else "badge-stable"
        badge_text = "🔴 DECLINING COMMUNITY" if is_declining else "🟢 STABLE OR GROWING"

        st.markdown(f"""
        <div style="display: flex; align-items: center; gap: 1rem; margin: 1rem 0;">
            <h2 style="margin: 0; font-size: 1.8rem; color: #F8FAFC;">r/{selected_sub}</h2>
            <span class="badge {badge_class}">{badge_text}</span>
            <span style="font-size: 0.85rem; color: #94A3B8;">Component ID: #{int(sub_row['early_component'])}</span>
        </div>
        """, unsafe_allow_html=True)

        # 4 Metric Cards for Selected Subreddit
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            render_metric_card(
                "Early Activity",
                f"{int(sub_row['early_activity'])}",
                "Dec 2013 – Aug 2015",
                "neutral"
            )
        with m2:
            pct_change = -sub_row['decline_percentage'] if is_declining else ((sub_row['late_activity'] - sub_row['early_activity'])/sub_row['early_activity']*100)
            render_metric_card(
                "Late Activity",
                f"{int(sub_row['late_activity'])}",
                f"{pct_change:+.1f}% vs Early",
                "declining" if is_declining else "growing"
            )
        with m3:
            cohort_avg_degree = 106.93 if is_declining else 120.41
            degree_diff = sub_row['early_degree'] - cohort_avg_degree
            render_metric_card(
                "Early Degree",
                f"{int(sub_row['early_degree'])}",
                f"{degree_diff:+.1f} vs Cohort Avg",
                "growing" if degree_diff >= 0 else "declining"
            )
        with m4:
            render_metric_card(
                "Early PageRank",
                f"{sub_row['early_pagerank']:.4f}",
                "Network importance score",
                "neutral"
            )

        st.markdown("### 📈 Monthly Activity Timeline (Dec 2013 – Apr 2017)")

        # Monthly Activity Time-Series
        sub_monthly = df_monthly[df_monthly['source_subreddit'] == selected_sub].sort_values(by="date")

        if not sub_monthly.empty:
            fig_time = go.Figure()

            # Shaded background zones for Early and Late periods
            fig_time.add_vrect(
                x0="2013-12-01", x1="2015-08-31",
                fillcolor="rgba(56, 189, 248, 0.08)", opacity=1,
                layer="below", line_width=0,
                annotation_text="Early Period (Graph Formulation)", annotation_position="top left",
                annotation_font=dict(size=11, color="#38BDF8")
            )
            fig_time.add_vrect(
                x0="2015-09-01", x1="2017-04-30",
                fillcolor="rgba(167, 139, 250, 0.08)", opacity=1,
                layer="below", line_width=0,
                annotation_text="Late Period (Outcome Measurement)", annotation_position="top right",
                annotation_font=dict(size=11, color="#A78BFA")
            )

            # Dividing line
            fig_time.add_vline(
                x="2015-08-31",
                line_width=1.5,
                line_dash="dash",
                line_color="#E2E8F0"
            )

            # Line and Area
            fig_time.add_trace(go.Scatter(
                x=sub_monthly['date'],
                y=sub_monthly['interactions'],
                mode='lines+markers',
                name='Monthly Hyperlinks',
                line=dict(color=PALETTE["declining"] if is_declining else PALETTE["stable"], width=3),
                marker=dict(size=6, color="#FFFFFF", line=dict(width=2, color=PALETTE["declining"] if is_declining else PALETTE["stable"])),
                fill='tozeroy',
                fillcolor=PALETTE["declining_light"] if is_declining else PALETTE["stable_light"]
            ))

            fig_time.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(30,41,59,0.3)",
                height=350,
                margin=dict(l=20, r=20, t=30, b=20),
                xaxis_title="Time",
                yaxis_title="Monthly Hyperlink Interactions",
                hovermode="x unified"
            )

            st.plotly_chart(fig_time, use_container_width=True)
        else:
            st.info(f"No granular monthly activity records found for r/{selected_sub} in monthly_activity.csv")

        # Subreddit Ego-Network & Connections
        st.markdown("### 🕸️ Local Interaction Ego-Network")
        col_net1, col_net2 = st.columns([1, 1])

        sub_outbound = df_edges[df_edges['source_subreddit'] == selected_sub].sort_values(by="interaction_count", ascending=False)
        sub_inbound = df_edges[df_edges['target_subreddit'] == selected_sub].sort_values(by="interaction_count", ascending=False)

        with col_net1:
            st.markdown(f"**Top Outgoing Hyperlinks From r/{selected_sub}** ({len(sub_outbound)} total targets)")
            if not sub_outbound.empty:
                st.dataframe(
                    sub_outbound.head(10).rename(columns={
                        "target_subreddit": "Target Community",
                        "interaction_count": "Hyperlink Count"
                    })[['Target Community', 'Hyperlink Count']],
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.write("No outgoing early hyperlink edges.")

        with col_net2:
            st.markdown(f"**Top Incoming Hyperlinks Into r/{selected_sub}** ({len(sub_inbound)} total sources)")
            if not sub_inbound.empty:
                st.dataframe(
                    sub_inbound.head(10).rename(columns={
                        "source_subreddit": "Source Community",
                        "interaction_count": "Hyperlink Count"
                    })[['Source Community', 'Hyperlink Count']],
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.write("No incoming early hyperlink edges.")


# =============================================================================
# PAGE 3: DECLINE ANALYSIS & HYPOTHESIS TESTING
# =============================================================================
elif page == "📉 Decline Analysis":
    st.markdown('<div class="hero-title">Decline Analysis & Hypothesis Testing</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero-subtitle">Evaluating the relationship between early network connectivity and subsequent activity decline among online communities.</div>', unsafe_allow_html=True)

    # Degree Binned Analysis Section
    st.markdown("### 1. Degree-Binned Decline Dynamics")
    st.markdown("""
    Declining communities were partitioned into five early-degree tiers to test whether higher early connectivity 
    buffers communities against activity loss.
    """)

    col_bin1, col_bin2 = st.columns([3, 2])

    with col_bin1:
        fig_binned = make_subplots(specs=[[{"secondary_y": True}]])

        fig_binned.add_trace(
            go.Bar(
                x=df_degree_decline['degree_group'],
                y=df_degree_decline['avg_decline_percentage'],
                name="Avg Decline %",
                marker_color="#EF4444",
                text=df_degree_decline['avg_decline_percentage'].apply(lambda x: f"{x:.1f}%"),
                textposition="auto"
            ),
            secondary_y=False
        )

        fig_binned.add_trace(
            go.Scatter(
                x=df_degree_decline['degree_group'],
                y=df_degree_decline['avg_pagerank'],
                name="Avg PageRank",
                mode="lines+markers+text",
                text=df_degree_decline['avg_pagerank'].apply(lambda x: f"{x:.1f}"),
                textposition="top center",
                line=dict(color="#38BDF8", width=3),
                marker=dict(size=8, color="#FFFFFF")
            ),
            secondary_y=True
        )

        fig_binned.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(30,41,59,0.3)",
            height=360,
            margin=dict(l=20, r=20, t=30, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        fig_binned.update_yaxes(title_text="Average Decline Percentage (%)", secondary_y=False, range=[0, 85])
        fig_binned.update_yaxes(title_text="Average PageRank", secondary_y=True)

        st.plotly_chart(fig_binned, use_container_width=True)

    with col_bin2:
        st.dataframe(
            df_degree_decline.rename(columns={
                "degree_group": "Degree Tier",
                "communities": "Communities (N)",
                "avg_decline_percentage": "Avg Decline %",
                "avg_pagerank": "Avg PageRank"
            }),
            use_container_width=True,
            hide_index=True
        )

        st.markdown("""
        <div class="callout-box" style="margin-top: 0.5rem; font-size: 0.85rem;">
            <strong>Key Insight:</strong> The lowest connectivity tier (<code>0–24</code>) experienced 
            <span class="callout-highlight">74.06% average decline</span>, compared to 
            <span class="callout-highlight">40.16%</span> in the highest tier (<code>200+</code>). 
            This represents a <strong>33.90 percentage-point gap</strong>. However, the plateau between 100-199 (40.10%) and 200+ (40.16%) shows the effect is not strictly monotonic.
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # Correlation Scatter Plots
    st.markdown("### 2. Statistical Correlation & Centrality Distributions")
    st.markdown("""
    Evaluating Pearson correlation among declining communities (N=215). Both degree (*r = -0.0933*) and PageRank (*r = -0.1054*)
    display negative associations with decline percentage, but the relationship is weak.
    """)

    scatter_col1, scatter_col2 = st.columns(2)
    df_declining = df_metrics[df_metrics['outcome'] == 'DECLINING'].copy()

    with scatter_col1:
        log_scale_deg = st.checkbox("Log scale for Degree", value=True)
        fig_scat1 = px.scatter(
            df_declining,
            x="early_degree",
            y="decline_percentage",
            hover_name="source_subreddit",
            hover_data={"early_activity": True, "late_activity": True, "early_pagerank": True},
            color_discrete_sequence=["#F87171"],
            trendline="ols",
            log_x=log_scale_deg,
            labels={"early_degree": "Early Degree Centrality", "decline_percentage": "Decline Percentage (%)"},
            title="Early Degree vs. Activity Decline (r = -0.0933)"
        )
        fig_scat1.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(30,41,59,0.3)",
            height=380
        )
        st.plotly_chart(fig_scat1, use_container_width=True)

    with scatter_col2:
        log_scale_pr = st.checkbox("Log scale for PageRank", value=True)
        fig_scat2 = px.scatter(
            df_declining,
            x="early_pagerank",
            y="decline_percentage",
            hover_name="source_subreddit",
            hover_data={"early_activity": True, "late_activity": True, "early_degree": True},
            color_discrete_sequence=["#38BDF8"],
            trendline="ols",
            log_x=log_scale_pr,
            labels={"early_pagerank": "Early PageRank", "decline_percentage": "Decline Percentage (%)"},
            title="Early PageRank vs. Activity Decline (r = -0.1054)"
        )
        fig_scat2.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(30,41,59,0.3)",
            height=380
        )
        st.plotly_chart(fig_scat2, use_container_width=True)

    st.markdown("---")

    # High-Degree Collapse Case Studies (Counter-examples)
    st.markdown("### 3. Highly Connected Collapses (Why High Degree ≠ Immunity)")
    st.markdown("""
    The table below highlights subreddits with **strong early connectivity (Degree ≥ 100)** that nevertheless suffered 
    **extreme decline (≥ 60%)**. These counter-examples prove that early centrality does not guarantee resilience.
    """)

    anomalies = df_declining[(df_declining['early_degree'] >= 100) & (df_declining['decline_percentage'] >= 60.0)].sort_values(
        by="decline_percentage", ascending=False
    )

    st.dataframe(
        anomalies[['source_subreddit', 'early_activity', 'late_activity', 'decline_percentage', 'early_degree', 'early_pagerank']].rename(columns={
            "source_subreddit": "Subreddit",
            "early_activity": "Early Activity",
            "late_activity": "Late Activity",
            "decline_percentage": "Decline %",
            "early_degree": "Early Degree",
            "early_pagerank": "Early PageRank"
        }),
        use_container_width=True,
        hide_index=True
    )


# =============================================================================
# PAGE 4: NETWORK TOPOLOGY & GRAPH EXPLORER
# =============================================================================
elif page == "🕸️ Network Topology":
    st.markdown('<div class="hero-title">Early Network Topology & Hub Analysis</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero-subtitle">Explore the structure of the early-period hyperlink network (19,747 vertices, 65,696 unique directed edges).</div>', unsafe_allow_html=True)

    # Leaderboards
    col_lead1, col_lead2, col_lead3 = st.columns(3)

    # Compute top out-degree from df_edges
    top_outbound = df_edges.groupby('source_subreddit', observed=True)['interaction_count'].agg(['count', 'sum']).reset_index()
    top_outbound.columns = ['subreddit', 'unique_targets', 'total_hyperlinks']
    top_outbound = top_outbound.sort_values(by='unique_targets', ascending=False).head(10)

    # Compute top in-degree
    top_inbound = df_edges.groupby('target_subreddit', observed=True)['interaction_count'].agg(['count', 'sum']).reset_index()
    top_inbound.columns = ['subreddit', 'unique_sources', 'total_references']
    top_inbound = top_inbound.sort_values(by='unique_sources', ascending=False).head(10)

    # Compute top dyads
    top_dyads = df_edges.sort_values(by='interaction_count', ascending=False).head(10)

    with col_lead1:
        st.markdown("**Top Source Hubs (Out-Degree)**")
        st.dataframe(
            top_outbound.rename(columns={
                "subreddit": "Subreddit",
                "unique_targets": "Out-Degree",
                "total_hyperlinks": "Hyperlinks"
            }),
            use_container_width=True,
            hide_index=True
        )

    with col_lead2:
        st.markdown("**Top Target Hubs (In-Degree)**")
        st.dataframe(
            top_inbound.rename(columns={
                "subreddit": "Subreddit",
                "unique_sources": "In-Degree",
                "total_references": "References"
            }),
            use_container_width=True,
            hide_index=True
        )

    with col_lead3:
        st.markdown("**Top Interaction Dyads**")
        st.dataframe(
            top_dyads.rename(columns={
                "source_subreddit": "Source",
                "target_subreddit": "Target",
                "interaction_count": "Count"
            }),
            use_container_width=True,
            hide_index=True
        )

    st.markdown("---")

    # Interactive Subgraph Network Visualizer
    st.markdown("### 🕸️ Interactive Subgraph Visualizer")
    st.markdown("Select a seed subreddit to construct an interactive topological graph of its immediate neighborhood.")

    top_candidates = df_metrics.sort_values(by="early_degree", ascending=False)['source_subreddit'].head(30).tolist()
    net_seed = st.selectbox("Select Seed Subreddit for Local Graph", top_candidates, index=0)
    max_neighbors = st.slider("Max Neighbors to Display", min_value=5, max_value=30, value=15)

    # Build local graph
    sub_edges = df_edges[(df_edges['source_subreddit'] == net_seed) | (df_edges['target_subreddit'] == net_seed)].copy()
    sub_edges = sub_edges.sort_values(by="interaction_count", ascending=False).head(max_neighbors)

    if not sub_edges.empty:
        G = nx.DiGraph()
        for _, row in sub_edges.iterrows():
            G.add_edge(row['source_subreddit'], row['target_subreddit'], weight=row['interaction_count'])

        # Layout computation
        pos = nx.spring_layout(G, seed=42, k=0.6)

        edge_x = []
        edge_y = []
        for edge in G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])

        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=1.5, color="rgba(148, 163, 184, 0.4)"),
            hoverinfo='none',
            mode='lines'
        )

        node_x = []
        node_y = []
        node_text = []
        node_color = []
        node_size = []

        metrics_lookup = df_metrics.set_index('source_subreddit').to_dict('index')

        for node in G.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)

            if node == net_seed:
                node_text.append(f"<b>r/{node} (SEED)</b>")
                node_color.append("#6366F1")
                node_size.append(28)
            elif node in metrics_lookup:
                outcome = metrics_lookup[node]['outcome']
                color = PALETTE["declining"] if outcome == "DECLINING" else PALETTE["stable"]
                node_text.append(f"r/{node} ({outcome})<br>Deg: {metrics_lookup[node]['early_degree']}")
                node_color.append(color)
                node_size.append(18)
            else:
                node_text.append(f"r/{node}")
                node_color.append("#64748B")
                node_size.append(14)

        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            hoverinfo='text',
            text=[n if n == net_seed else "" for n in G.nodes()],
            textposition="top center",
            hovertext=node_text,
            marker=dict(
                color=node_color,
                size=node_size,
                line=dict(width=2, color="#FFFFFF")
            )
        )

        fig_net = go.Figure(
            data=[edge_trace, node_trace],
            layout=go.Layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(30,41,59,0.3)",
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20, l=5, r=5, t=20),
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                height=450
            )
        )

        st.plotly_chart(fig_net, use_container_width=True)
        st.caption(f"Graph shows {len(G.nodes())} vertices and {len(G.edges())} edges connected to r/{net_seed}. Purple: Seed; Red: Declining Cohort; Green: Stable Cohort; Gray: Other Subreddits.")


# =============================================================================
# PAGE 5: BIG DATA PIPELINE & SCRIPTS
# =============================================================================
elif page == "🛠️ Big Data Pipeline":
    st.markdown('<div class="hero-title">Big Data Pipeline & Implementation</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero-subtitle">Inspect the underlying Apache Pig, Apache Hive, and Spark GraphX code powering the data extraction and graph computations.</div>', unsafe_allow_html=True)

    tab_pig, tab_hive, tab_graph, tab_math = st.tabs([
        "🐷 Pig ETL Pipeline",
        "🐝 Hive Analytics Queries",
        "⚡ Spark GraphX (Scala)",
        "📐 Mathematical Formulations"
    ])

    with tab_pig:
        st.markdown("**Apache Pig ETL Script (`pig/reddit_etl.pig`)**")
        st.markdown("Normalizes raw TSV records, cleans missing fields, and derives year/month partition keys.")
        pig_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "pig", "reddit_etl.pig")
        if os.path.exists(pig_path):
            with open(pig_path, "r") as f:
                st.code(f.read(), language="pig")
        else:
            st.warning("Pig script not found at expected location.")

    with tab_hive:
        st.markdown("**Apache Hive Integration & Export Queries (`hive/08_integrated_analysis.sql` & `09_dashboard_exports.sql`)**")
        st.markdown("Joins temporal activity aggregates with GraphX PageRank, Degree, and Component IDs.")
        hive_path1 = os.path.join(os.path.dirname(os.path.dirname(__file__)), "hive", "08_integrated_analysis.sql")
        hive_path2 = os.path.join(os.path.dirname(os.path.dirname(__file__)), "hive", "09_dashboard_exports.sql")

        col_h1, col_h2 = st.columns(2)
        with col_h1:
            st.markdown("*08_integrated_analysis.sql*")
            if os.path.exists(hive_path1):
                with open(hive_path1, "r") as f:
                    st.code(f.read(), language="sql")
        with col_h2:
            st.markdown("*09_dashboard_exports.sql*")
            if os.path.exists(hive_path2):
                with open(hive_path2, "r") as f:
                    st.code(f.read(), language="sql")

    with tab_graph:
        st.markdown("**Spark GraphX Early Network Analysis (`graph/GraphAnalysis_early.scala`)**")
        st.markdown("Distributed Scala implementation computing Degree, PageRank (damping 0.85, tol 0.001), and Connected Components.")
        scala_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "graph", "GraphAnalysis_early.scala")
        if os.path.exists(scala_path):
            with open(scala_path, "r") as f:
                st.code(f.read()[:3000] + "\n\n// ... [truncated for dashboard preview] ...", language="scala")
        else:
            st.warning("Scala GraphX script not found.")

    with tab_math:
        st.markdown("### Analytical Formulations")
        
        st.markdown("#### 1. Activity Decline Metric")
        st.markdown("For communities with early observed activity $\\ge 50$:")
        st.latex(r"\text{Decline Percentage} = \frac{\text{early\_activity} - \text{late\_activity}}{\text{early\_activity}} \times 100")
        
        st.markdown("#### 2. Classification Logic")
        st.latex(r"\text{Outcome} = \begin{cases} \text{DECLINING} & \text{if } \text{late\_activity} < \text{early\_activity} \\ \text{STABLE\_OR\_GROWING} & \text{if } \text{late\_activity} \ge \text{early\_activity} \end{cases}")

        st.markdown("#### 3. PageRank in GraphX")
        st.markdown("PageRank measures structural importance via stochastic random walk:")
        st.latex(r"PR(u) = \frac{1-d}{N} + d \sum_{v \in B_u} \frac{PR(v)}{L(v)}")
        st.markdown(r"where $d = 0.85$ is the damping factor, $B_u$ is the set of neighbors linking to $u$, and $L(v)$ is the out-degree of $v$.")

# -----------------------------------------------------------------------------
# 5. FOOTER
# -----------------------------------------------------------------------------
st.markdown("---")
st.markdown("""
<div style="display: flex; justify-content: space-between; align-items: center; color: #64748B; font-size: 0.8rem;">
    <div>Big Data & Large-Scale Computing Project | <strong>Reddit Community Collapse Analytics</strong></div>
    <div>SNAP Dataset | HDFS • Pig • Hive • Spark GraphX • Streamlit</div>
</div>
""", unsafe_allow_html=True)

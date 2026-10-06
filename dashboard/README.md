# Community Collapse Visualization Dashboard

This directory contains the interactive Streamlit analytics dashboard for the **Community Collapse Analysis** project.

The dashboard visualizes the results of our Big Data pipeline (HDFS → Apache Pig → Apache Hive → Apache Spark GraphX), investigating whether early-stage temporal and topological graph features can serve as early-warning indicators for online community decline.

---

## Directory Structure

```text
dashboard/
├── app.py                     # Main Streamlit dashboard application
├── README.md                  # Dashboard documentation and guide
└── data/                      # Exported analytical CSV datasets from Hive
    ├── cohort_comparison.csv  # Declining vs Stable/Growing cohort benchmarks
    ├── community_metrics.csv  # 387 communities with degree, PageRank, outcomes
    ├── degree_decline.csv     # Binned analysis across 5 degree tiers
    ├── monthly_activity.csv   # Longitudinal monthly interaction time series
    └── network_edges.csv      # Early-period directed hyperlink interaction network
```

---

## Prerequisites & Installation

Ensure you have Python 3.9+ installed. Install the required dependencies:

```bash
pip install streamlit pandas plotly networkx
```

---

## Running the Dashboard

To launch the dashboard, execute:

```bash
cd dashboard
streamlit run app.py
```

Or from the repository root:

```bash
python -m streamlit run dashboard/app.py
```

By default, Streamlit will start a local server at:
```text
http://localhost:8501
```

---

## Dashboard Pages & Features

### 1. 🌟 Executive Overview
- **Key Metrics Tiles**: Total analyzed communities (387), Declining cohort (215), Stable/Growing cohort (172), Early network vertices (19,747), and Directed edges (65,696).
- **Core Research Findings**: Clear callout detailing degree and PageRank disparities between cohorts and weak standalone correlation coefficients.
- **Visual Cohort Benchmarks**: Side-by-side grouped comparisons for early activity, late activity, degree, and PageRank.
- **Pipeline Architecture Visualizer**: Interactive diagram outlining the flow from HDFS through Pig, Hive, Spark GraphX, to Streamlit.

### 2. 🔍 Community Explorer
- **Interactive Search & Autocomplete**: Search across all 387 analyzed communities.
- **Detailed Subreddit Profile**: Visual status badge (`DECLINING` vs `STABLE_OR_GROWING`), component ID, and metric deltas against cohort averages.
- **Longitudinal Monthly Activity Timeline**: Interactive Plotly time-series spanning December 2013 through April 2017, highlighting the Early formulation period vs Late outcome period.
- **Ego-Network Connections**: Top inbound and outbound hyperlink interactions for the selected community.

### 3. 📉 Decline Analysis & Hypothesis Testing
- **Degree-Binned Decline Dynamics**: Dual-axis bar and line chart examining decline rates across degree tiers (`0–24`, `25–49`, `50–99`, `100–199`, `200+`), showing the 33.9 percentage-point gap between low and high connectivity.
- **Correlation & Centrality Distribution**: Scatter plots with OLS regression lines for Early Degree vs Decline % (*r = -0.0933*) and Early PageRank vs Decline % (*r = -0.1054*).
- **Counter-example Analysis**: Table of high-degree communities that still suffered severe collapse, demonstrating why centrality is not a guarantee of resilience.

### 4. 🕸️ Early Network Topology
- **Network Hub Leaderboards**: Top source communities (out-degree), target communities (in-degree), and most active subreddit dyads.
- **Interactive Subgraph Visualizer**: Force-directed network diagram (spring layout) rendering local ego-networks with color-coded cohort outcomes and edge weights.

### 5. 🛠️ Big Data Pipeline & Code Inspector
- **Script Viewer**: Embedded, syntax-highlighted viewers for `pig/reddit_etl.pig`, `graph/GraphAnalysis_early.scala`, and Hive SQL queries (`08_integrated_analysis.sql`, `09_dashboard_exports.sql`).
- **Mathematical Formulations**: LaTeX-rendered formulas for activity decline percentage, cohort classification, and PageRank computation.

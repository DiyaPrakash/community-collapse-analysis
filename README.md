Each record represents a hyperlink interaction between two subreddits.

### Dataset Fields

| Field | Description |
|---|---|
| `SOURCE_SUBREDDIT` | Subreddit containing the post |
| `TARGET_SUBREDDIT` | Subreddit linked to |
| `POST_ID` | Reddit post identifier |
| `TIMESTAMP` | Timestamp of the interaction |
| `LINK_SENTIMENT` | Sentiment associated with the hyperlink |
| `PROPERTIES` | Additional dataset properties |

The dataset covers Reddit hyperlink interactions from approximately:

**December 2013 – April 2017**

The raw dataset is approximately 300 MB and is therefore **not stored in this GitHub repository**.

The dataset should be downloaded separately and placed under:

```text
data/raw/
```

---

## Big Data Architecture

The project follows a four-tool Big Data pipeline:

```text
                  SNAP Reddit Dataset
                           |
                           v
                         HDFS
                           |
                           v
                    Apache Pig
                  Cleaning + ETL
                           |
                           v
                    Apache Hive
              Temporal Activity Analysis
                           |
                  +--------+--------+
                  |                 |
                  v                 v
           Early Network       Activity
               Data            Cohorts
                  |                 |
                  v                 |
             Spark GraphX <---------+
                  |
                  v
          Early Graph Metrics
                  |
                  v
                Hive
         Integrated Analysis
                  |
                  v
          Processed CSV Data
                  |
                  v
              Streamlit
              Dashboard
```

The four Big Data technologies used are:

```text
HDFS → Pig → Hive → Spark GraphX
```

Streamlit is used only as the visualization/presentation layer and is not considered one of the Big Data processing tools.

---

## Why These Tools?

### HDFS

HDFS provides distributed storage for the Reddit hyperlink dataset and intermediate project data.

It is used as the storage foundation of the pipeline.

Main HDFS locations include:

```text
/community_collapse/raw
/community_collapse/pig_output
/community_collapse/hive
/community_collapse/graph
```

### Apache Pig

Apache Pig performs the initial ETL and preprocessing stage.

The Pig pipeline performs the following operations:

1. Loads the raw Reddit hyperlink TSV file.
2. Removes the dataset header.
3. Normalizes subreddit names using lowercase and trimming.
4. Removes records with missing required fields.
5. Extracts the year from the timestamp.
6. Extracts the month from the timestamp.
7. Stores the cleaned dataset in HDFS.

Main script:

```text
pig/reddit_etl.pig
```

Output:

```text
/community_collapse/pig_output/reddit_cleaned
```

The cleaned records have the following logical structure:

```text
source_subreddit
target_subreddit
post_id
timestamp
sentiment
year
month
```

### Apache Hive

Hive performs the main SQL-based analytical processing.

Hive is used for:

- Creating the Reddit interaction table
- Calculating monthly activity
- Separating early and late activity periods
- Constructing declining and stable/growing cohorts
- Aggregating subreddit-to-subreddit interactions
- Preparing graph edges
- Integrating GraphX results
- Producing final dashboard datasets

The Hive database used by the project is:

```text
community_collapse
```

The main interaction table is:

```text
reddit_interactions
```

### Apache Spark GraphX

Apache Spark GraphX is used to analyze the early-period subreddit interaction network.

The graph is constructed as a directed network:

```text
SOURCE_SUBREDDIT → TARGET_SUBREDDIT
```

Multiple interactions between the same pair of subreddits are aggregated into a single graph edge with an interaction count.

The early-period GraphX analysis calculates:

- Degree
- PageRank
- Connected components

The final early-period GraphX implementation is:

```text
graph/GraphAnalysis_early.scala
```

---

## Temporal Analysis

The project divides the dataset into two periods.

### Early Period

**December 2013 – August 2015**

This period is used to calculate early activity and early network structure.

### Late Period

**September 2015 – April 2017**

This period is used to measure subsequent activity.

This separation is important because the project is studying whether **early characteristics are associated with later outcomes**.

The graph metrics are therefore calculated using only the early-period network.

---

## Community Selection

The main analysis focuses on communities with sufficient observed activity during the early period.

A minimum threshold of:

```text
50 early interactions
```

is used for the main cohort analysis.

This avoids interpreting extremely sparse communities as meaningful cases of activity decline.

---

## Activity Outcome Definition

For each subreddit, two activity measures are calculated:

```text
early_activity
late_activity
```

where activity represents the number of observed hyperlink interactions sourced from that subreddit.

### Declining Communities

A community is classified as declining when:

```text
late_activity < early_activity
```

The percentage decline is calculated as:

```text
decline_percentage =
((early_activity - late_activity) / early_activity) × 100
```

### Stable or Growing Communities

A community is classified as stable/growing when:

```text
late_activity >= early_activity
```

These communities are represented as:

```text
STABLE_OR_GROWING
```

---

## Early Graph Construction

Only interactions occurring during the early period are included in the early graph:

**December 2013 – August 2015**

The resulting early network contains:

- **19,747 vertices**
- **65,696 unique directed edges**
- **339 connected components**

The network is therefore constructed before the late-period activity outcome is evaluated.

---

## Graph Metrics

### Degree

Degree represents the number of network connections associated with a subreddit in the early-period graph.

The analysis uses the GraphX degree information to represent early network connectivity.

Higher degree generally indicates that a subreddit participates in more observed subreddit-to-subreddit relationships.

### PageRank

PageRank measures the relative structural importance of a subreddit within the early hyperlink network.

A higher PageRank indicates greater structural importance within the network.

PageRank is calculated using the early-period graph only.

### Connected Components

Connected components identify groups of vertices connected through the network.

The component identifier is used as an additional structural characteristic of each subreddit.

---

## Final Network Statistics

| Metric | Value |
|---|---:|
| Total vertices | 35,776 |
| Total edges | 137,821 |
| Early-period vertices | 19,747 |
| Early-period unique edges | 65,696 |
| Early-period interaction records | 122,429 |
| Early connected components | 339 |

---

## Final Cohort Statistics

| Outcome | Communities | Avg. Early Activity | Avg. Late Activity | Avg. Decline | Avg. Early Degree | Avg. Early PageRank |
|---|---:|---:|---:|---:|---:|---:|
| DECLINING | 215 | 154.95 | 75.00 | 49.93% | 106.93 | 10.8607 |
| STABLE_OR_GROWING | 172 | 144.00 | 242.05 | 0.00% | 120.41 | 12.3722 |

Therefore:

- **Total analyzed communities:** 387
- **Declining communities:** 215
- **Stable/growing communities:** 172

The declining cohort had lower average early-period degree and PageRank than the stable/growing cohort.

---

## Degree-Binned Analysis

To further investigate the relationship between early connectivity and subsequent decline, declining communities were grouped according to their early-period degree.

| Early Degree | Communities | Average Decline | Average PageRank |
|---|---:|---:|---:|
| 0–24 | 34 | 74.06% | 1.1272 |
| 25–49 | 44 | 51.25% | 2.9029 |
| 50–99 | 61 | 47.75% | 6.1367 |
| 100–199 | 49 | 40.10% | 14.4689 |
| 200+ | 27 | 40.16% | 40.2104 |

The lowest-connectivity group experienced substantially greater average subsequent decline than the highest-connectivity group.

For example:

```text
0–24 degree     → 74.06% average decline
200+ degree     → 40.16% average decline
```

This represents a difference of approximately:

**33.90 percentage points**

However, the relationship is not perfectly monotonic.

Therefore, increasing degree cannot be interpreted as guaranteeing lower future decline.

---

## Correlation Analysis

Correlation analysis was performed among the declining communities.

### Early Degree vs. Decline

```text
Correlation = -0.0933
```

### Early PageRank vs. Decline

```text
Correlation = -0.1054
```

The negative signs are consistent with the degree-binned analysis.

However, both correlations are weak.

Therefore, early degree and PageRank should not be presented as strong standalone predictors of subsequent decline.

---

## Interpretation of Results

The results suggest that early network structure is **associated with** subsequent changes in observed hyperlink activity.

The main observations are:

1. Declining communities had lower average early degree than stable/growing communities.
2. Declining communities had lower average early PageRank than stable/growing communities.
3. Communities with very low early degree experienced larger average subsequent declines.
4. The degree-binned analysis shows a substantial difference between low-connectivity and high-connectivity communities.
5. The correlation coefficients are weak, indicating that early network centrality alone does not explain individual outcomes.
6. There is substantial variation between individual communities.

---

## Important Example

Not every highly connected community remains active.

The analysis identified communities that experienced very large subsequent declines despite having relatively strong early network characteristics.

This demonstrates why the project does not claim:

```text
High degree = guaranteed survival
```

or:

```text
Low degree = guaranteed decline
```

Instead, the results indicate an association between early network structure and subsequent observed activity decline.

---

## Key Finding

The central finding of this project is:

> **Communities that subsequently experienced declining observed hyperlink activity had lower average early-period degree and PageRank than stable or growing communities. However, the weak correlation coefficients indicate that early network centrality alone is not a strong explanatory or predictive signal for individual community outcomes.**

Therefore, the project demonstrates the usefulness of combining:

```text
Temporal Activity Analysis
+
Graph Analytics
```

rather than relying on a single network metric.

---

## Dashboard

The project includes processed datasets for a Streamlit-based visualization dashboard.

Dashboard data is stored under:

```text
dashboard/data/
```

The final datasets are:

```text
dashboard/data/
├── community_metrics.csv
├── monthly_activity.csv
├── network_edges.csv
├── degree_decline.csv
└── cohort_comparison.csv
```

---

## Dashboard Dataset Definitions

### community_metrics.csv

Contains one row for each analyzed community.

Columns:

```text
source_subreddit
early_activity
late_activity
decline_percentage
early_degree
early_pagerank
early_component
outcome
```

Example:

```text
3ds,76,32,57.89,73,11.242637462788569,0,DECLINING
```

### monthly_activity.csv

Contains monthly observed activity for subreddit sources.

Columns:

```text
source_subreddit
year
month
interactions
```

This dataset is used for temporal activity visualizations.

### network_edges.csv

Contains the early-period subreddit interaction network.

Columns:

```text
source_subreddit
target_subreddit
interaction_count
```

Each row represents an aggregated directed edge.

Example:

```text
0x10c,ixion,1
0x10c,trillek,1
```

### degree_decline.csv

Contains the degree-binned analysis of declining communities.

Columns:

```text
degree_group
communities
avg_decline_percentage
avg_pagerank
```

### cohort_comparison.csv

Contains aggregate statistics comparing the declining and stable/growing cohorts.

Columns:

```text
outcome
communities
avg_early_activity
avg_late_activity
avg_decline_percentage
avg_early_degree
avg_early_pagerank
```

---

## Planned Dashboard Pages

### 1. Overview

The overview page will display:

- Total analyzed communities
- Declining communities
- Stable/growing communities
- Total network vertices
- Total network edges
- Early network size
- Activity trends
- Cohort comparison

### 2. Community Explorer

Users can search for an individual subreddit and view:

- Early activity
- Late activity
- Percentage decline
- Early degree
- Early PageRank
- Early connected component
- Monthly activity history

### 3. Decline Analysis

This page will visualize:

- Declining vs stable/growing communities
- Average early degree
- Average early PageRank
- Degree-binned decline
- Distribution of decline percentages
- Most strongly declining communities

### 4. Network Explorer

This page will allow users to explore the early-period subreddit network.

Users can select a subreddit and inspect its observed connections to other communities.

The network visualization will use the processed:

```text
network_edges.csv
```

dataset rather than requiring Hadoop or Hive to be running.

---

## Repository Structure

```text
community_collapse_project/
│
├── data/
│   ├── raw/
│   └── cleaned/
│
├── dashboard/
│   └── data/
│       ├── community_metrics.csv
│       ├── monthly_activity.csv
│       ├── network_edges.csv
│       ├── degree_decline.csv
│       └── cohort_comparison.csv
│
├── docs/
│
├── graph/
│   ├── GraphAnalysis.scala
│   └── GraphAnalysis_early.scala
│
├── hdfs/
│
├── hive/
│
├── pig/
│   └── reddit_etl.pig
│
└── results/
```

Generated Hadoop, Hive, Spark, and temporary files are excluded from GitHub using `.gitignore`.

The raw Reddit dataset is also excluded because of its size.

---

## Reproducibility

The complete pipeline follows:

```text
1. Download SNAP Reddit Hyperlink Dataset
                    ↓
2. Store raw dataset in HDFS
                    ↓
3. Execute Pig ETL
                    ↓
4. Load cleaned data into Hive
                    ↓
5. Perform temporal activity analysis
                    ↓
6. Construct early-period graph edges
                    ↓
7. Run Spark GraphX
                    ↓
8. Calculate early degree/PageRank/components
                    ↓
9. Integrate GraphX metrics with Hive outcomes
                    ↓
10. Export dashboard datasets
                    ↓
11. Run Streamlit dashboard
```

---

## Raw Dataset Setup

After downloading the SNAP dataset, place:

```text
soc-redditHyperlinks-body.tsv
```

under:

```text
data/raw/
```

The raw dataset should not be committed to GitHub.

---

## HDFS Processing

The raw dataset is uploaded to:

```text
/community_collapse/raw/
```

The cleaned Pig output is stored at:

```text
/community_collapse/pig_output/reddit_cleaned/
```

Graph-related outputs are stored under:

```text
/community_collapse/graph/
```

---

## Pig Processing

Run:

```bash
pig -x mapreduce pig/reddit_etl.pig
```

The script performs the initial transformation and produces the cleaned interaction dataset.

---

## Hive Processing

Hive is used to create the Reddit interaction table and perform the temporal analysis.

The main database is:

```text
community_collapse
```

The main interaction table is:

```text
reddit_interactions
```

Important analytical objects include:

```text
community_monthly_activity
community_decline_analysis
decline_cohort
stable_cohort
early_network_outcome
```

---

## GraphX Processing

The early-period graph analysis is implemented in:

```text
graph/GraphAnalysis_early.scala
```

The graph is constructed only from interactions in the early period.

The resulting GraphX outputs include:

```text
/community_collapse/graph/results/early_degree
/community_collapse/graph/results/early_pagerank
/community_collapse/graph/results/early_components
```

These outputs are subsequently loaded into Hive for integrated analysis.

---

## Limitations

### 1. Observed activity is not identical to community health

The dataset measures observed subreddit-to-subreddit hyperlink interactions.

Therefore, a reduction in hyperlink activity does not necessarily mean that the underlying subreddit disappeared or became inactive in every possible sense.

### 2. Weak predictive signal

The correlation between early degree and later decline is:

```text
-0.0933
```

The correlation between early PageRank and later decline is:

```text
-0.1054
```

These are weak correlations.

Therefore, the current graph features should not be presented as a reliable standalone prediction model.

### 3. Cohort selection

The main analysis requires at least:

```text
50 early interactions
```

This means very low-activity communities are excluded from the main cohort comparison.

### 4. No causal inference

The project identifies associations between early network structure and later observed activity.

It does not establish that network structure causes community decline.

### 5. Limited graph features

The current analysis focuses primarily on:

- Degree
- PageRank
- Connected components

Additional temporal and graph features could provide more information.

---

## Future Work

Possible extensions include:

- Temporal graph snapshots
- Community detection
- Network density
- Clustering coefficients
- Reciprocity
- Centrality change over time
- Community-level structural evolution
- Supervised prediction models
- Survival analysis
- Temporal graph neural networks
- More advanced network visualization
- Additional activity and structural features

A future prediction model could combine:

```text
Activity Features
+
Degree
+
PageRank
+
Network Density
+
Reciprocity
+
Centrality Change
+
Temporal Trends
```

rather than relying on a single network metric.

---

## Team Contributions

The project is designed as a collaborative Big Data analytics project.

Possible contribution areas include:

### Data Engineering

- Dataset preparation
- HDFS setup
- Pig ETL
- Data cleaning

### Data Analytics

- Hive schema
- Temporal analysis
- Cohort construction
- Statistical analysis

### Graph Analytics

- Graph construction
- Spark GraphX
- Degree analysis
- PageRank
- Connected components

### Visualization

- Streamlit dashboard
- Interactive community explorer
- Network visualization
- Analytical charts

### Documentation

- Technical documentation
- Final report
- Presentation
- Demo preparation

Individual contribution percentages should be finalized by the project team before submission.

---

## Technologies Used

| Technology | Purpose |
|---|---|
| Hadoop HDFS | Distributed storage |
| Apache Pig | ETL and preprocessing |
| Apache Hive | SQL-based analytics |
| Apache Spark | Distributed graph processing |
| Spark GraphX | Graph analytics |
| Scala | GraphX implementation |
| Streamlit | Dashboard and visualization |
| Git/GitHub | Version control |

---

## Project Highlights

The project demonstrates:

- End-to-end Big Data processing
- Distributed storage using HDFS
- Batch ETL using Pig
- Large-scale SQL analytics using Hive
- Graph processing using Spark GraphX
- Temporal cohort analysis
- Network centrality analysis
- Integration of graph and temporal features
- Interactive data visualization

The key technical idea is the separation of:

```text
EARLY NETWORK STRUCTURE
          ↓
     GraphX Metrics
          ↓
     Later Activity
          ↓
       Outcome
```

This allows the analysis to examine whether network characteristics measured before the outcome are associated with subsequent changes in observed activity.

---

## Conclusion

This project presents an end-to-end Big Data analytics pipeline for studying the evolution of online communities.

Using:

```text
HDFS
  ↓
Apache Pig
  ↓
Apache Hive
  ↓
Apache Spark GraphX
```

the project combines large-scale data preparation, temporal activity analysis, and graph analytics.

The results show that communities experiencing subsequent declining observed hyperlink activity had, on average, somewhat weaker early-period network connectivity and PageRank than stable or growing communities.

The degree-binned analysis also shows substantially larger average declines among communities with very low early connectivity.

However, the weak correlation coefficients demonstrate that simple network centrality measures are not sufficient to explain individual community outcomes.

The final conclusion is therefore:

> **Early network structure is associated with subsequent declining observed hyperlink activity, but early network centrality alone is not sufficient to explain or reliably predict individual community outcomes.**

---

## Repository Status

The repository contains the finalized analytical datasets required by the visualization layer.

The final dashboard data consists of:

```text
community_metrics.csv
monthly_activity.csv
network_edges.csv
degree_decline.csv
cohort_comparison.csv
```

Large raw datasets and generated Big Data outputs are intentionally excluded from the repository.

---

## License and Dataset Attribution

This project uses the Reddit Hyperlink Network dataset provided by the **Stanford Network Analysis Project (SNAP)**.

The dataset should be obtained directly from its original source and used according to the applicable dataset terms and attribution requirements.

---

## Authors

**Big Data & Large-Scale Computing Project**

### Project Title

**When Communities Decline: Early Warning Signals of Online Community Activity Using Temporal Graph Analytics**

### Research Question

**Can early temporal graph characteristics provide signals associated with subsequent declining observed activity in online communities?**

### Big Data Pipeline

```text
HDFS → Pig → Hive → Spark GraphX
```

### Visualization

```text
Streamlit
```

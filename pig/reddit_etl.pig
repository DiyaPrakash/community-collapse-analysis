-- ============================================================
-- Community Collapse Project
-- Pig ETL Pipeline
-- Dataset: Reddit Hyperlink Network
-- ============================================================

-- 1. Load raw data from HDFS
raw_data = LOAD '/community_collapse/raw/soc-redditHyperlinks-body.tsv'
USING PigStorage('\t')
AS (
    source_subreddit:chararray,
    target_subreddit:chararray,
    post_id:chararray,
    timestamp:chararray,
    link_sentiment:int,
    properties:chararray
);

-- 2. Remove the header row
data_without_header = FILTER raw_data
    BY source_subreddit != 'SOURCE_SUBREDDIT';

-- 3. Select only fields needed for our project
selected_data = FOREACH data_without_header GENERATE
    LOWER(TRIM(source_subreddit)) AS source_subreddit:chararray,
    LOWER(TRIM(target_subreddit)) AS target_subreddit:chararray,
    post_id,
    timestamp,
    link_sentiment AS sentiment;

-- 4. Remove records with missing essential fields
clean_data = FILTER selected_data
    BY source_subreddit IS NOT NULL
    AND target_subreddit IS NOT NULL
    AND post_id IS NOT NULL
    AND timestamp IS NOT NULL;

-- 5. Extract year
with_year = FOREACH clean_data GENERATE
    source_subreddit,
    target_subreddit,
    post_id,
    timestamp,
    sentiment,
    SUBSTRING(timestamp, 0, 4) AS year:chararray;

-- 6. Extract month
with_year_month = FOREACH with_year GENERATE
    source_subreddit,
    target_subreddit,
    post_id,
    timestamp,
    sentiment,
    year,
    SUBSTRING(timestamp, 5, 7) AS month:chararray;

-- 7. Store the cleaned dataset in HDFS
STORE with_year_month
INTO '/community_collapse/pig_output/reddit_cleaned'
USING PigStorage('\t');

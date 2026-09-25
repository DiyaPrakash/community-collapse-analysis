CREATE DATABASE IF NOT EXISTS community_collapse;

USE community_collapse;

DROP TABLE IF EXISTS reddit_interactions;

CREATE EXTERNAL TABLE reddit_interactions (
    source_subreddit STRING,
    target_subreddit STRING,
    post_id STRING,
    interaction_timestamp STRING,
    sentiment INT,
    year STRING,
    month STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/community_collapse/pig_output/reddit_cleaned';

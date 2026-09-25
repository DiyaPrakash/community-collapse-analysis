USE community_collapse;

INSERT OVERWRITE DIRECTORY
'/community_collapse/graph/edges'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'

SELECT
    source_subreddit,
    target_subreddit,
    COUNT(*) AS interaction_count

FROM reddit_interactions

WHERE source_subreddit IS NOT NULL
  AND target_subreddit IS NOT NULL

GROUP BY
    source_subreddit,
    target_subreddit;

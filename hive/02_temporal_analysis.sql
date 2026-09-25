USE community_collapse;

DROP VIEW IF EXISTS community_monthly_activity;

CREATE VIEW community_monthly_activity AS
SELECT
    source_subreddit,
    year,
    month,
    COUNT(*) AS interactions
FROM reddit_interactions
GROUP BY
    source_subreddit,
    year,
    month;


DROP VIEW IF EXISTS community_decline_analysis;

CREATE VIEW community_decline_analysis AS
SELECT
    source_subreddit,

    SUM(
        CASE
            WHEN CAST(yr AS INT) < 2015
              OR (CAST(yr AS INT) = 2015 AND CAST(mo AS INT) <= 8)
            THEN 1
            ELSE 0
        END
    ) AS early_activity,

    SUM(
        CASE
            WHEN CAST(yr AS INT) > 2015
              OR (CAST(yr AS INT) = 2015 AND CAST(mo AS INT) > 8)
            THEN 1
            ELSE 0
        END
    ) AS late_activity

FROM (
    SELECT
        source_subreddit,
        year AS yr,
        month AS mo
    FROM reddit_interactions
) t

GROUP BY source_subreddit

HAVING COUNT(*) >= 20;

USE community_collapse;

-- ============================================================
-- 1. Community-level metrics
-- ============================================================

SELECT
    source_subreddit,
    early_activity,
    late_activity,
    decline_percentage,
    early_degree,
    early_pagerank,
    early_component,
    outcome
FROM early_network_outcome
ORDER BY source_subreddit;


-- ============================================================
-- 2. Monthly activity
-- ============================================================

SELECT
    source_subreddit,
    year,
    month,
    interactions
FROM community_monthly_activity
ORDER BY year, month, source_subreddit;


-- ============================================================
-- 3. Early network edges
-- ============================================================

SELECT
    source_subreddit,
    target_subreddit,
    COUNT(*) AS interaction_count
FROM reddit_interactions
WHERE
    CAST(year AS INT) < 2015
    OR (
        CAST(year AS INT) = 2015
        AND CAST(month AS INT) <= 8
    )
GROUP BY
    source_subreddit,
    target_subreddit
ORDER BY
    source_subreddit,
    target_subreddit;


-- ============================================================
-- 4. Cohort comparison
-- ============================================================

SELECT
    outcome,
    COUNT(*) AS communities,
    ROUND(AVG(early_activity), 2) AS avg_early_activity,
    ROUND(AVG(late_activity), 2) AS avg_late_activity,
    ROUND(AVG(decline_percentage), 2) AS avg_decline_percentage,
    ROUND(AVG(early_degree), 2) AS avg_early_degree,
    ROUND(AVG(early_pagerank), 4) AS avg_early_pagerank
FROM early_network_outcome
GROUP BY outcome
ORDER BY outcome;


-- ============================================================
-- 5. Degree-binned decline analysis
-- ============================================================

SELECT
    CASE
        WHEN early_degree < 25 THEN '0-24'
        WHEN early_degree < 50 THEN '25-49'
        WHEN early_degree < 100 THEN '50-99'
        WHEN early_degree < 200 THEN '100-199'
        ELSE '200+'
    END AS degree_group,

    COUNT(*) AS communities,

    ROUND(
        AVG(decline_percentage),
        2
    ) AS avg_decline_percentage,

    ROUND(
        AVG(early_pagerank),
        4
    ) AS avg_pagerank

FROM early_network_outcome

WHERE outcome = 'DECLINING'

GROUP BY
    CASE
        WHEN early_degree < 25 THEN '0-24'
        WHEN early_degree < 50 THEN '25-49'
        WHEN early_degree < 100 THEN '50-99'
        WHEN early_degree < 200 THEN '100-199'
        ELSE '200+'
    END

ORDER BY
    MIN(early_degree);

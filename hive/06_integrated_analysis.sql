USE community_collapse;

SELECT
    d.source_subreddit,
    d.early_activity,
    d.late_activity,
    d.decline_percentage,
    g.degree,
    ROUND(p.pagerank, 4) AS pagerank

FROM decline_cohort d

LEFT JOIN graph_degree g
    ON d.source_subreddit = g.source_subreddit

LEFT JOIN graph_pagerank p
    ON d.source_subreddit = p.source_subreddit

ORDER BY
    d.decline_percentage DESC,
    g.degree DESC

LIMIT 30;

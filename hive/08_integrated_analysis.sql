USE community_collapse;

DROP TABLE IF EXISTS early_network_outcome;

CREATE TABLE early_network_outcome AS
SELECT
    d.source_subreddit,
    d.early_activity,
    d.late_activity,

    COALESCE(
        dc.decline_percentage,
        0.0
    ) AS decline_percentage,

    COALESCE(
        g.degree,
        0
    ) AS early_degree,

    COALESCE(
        p.pagerank,
        0.0
    ) AS early_pagerank,

    COALESCE(
        c.component_id,
        -1
    ) AS early_component,

    CASE
        WHEN dc.source_subreddit IS NOT NULL
        THEN 'DECLINING'
        ELSE 'STABLE_OR_GROWING'
    END AS outcome

FROM community_decline_analysis d

LEFT JOIN decline_cohort dc
    ON d.source_subreddit = dc.source_subreddit

LEFT JOIN early_graph_degree g
    ON d.source_subreddit = g.source_subreddit

LEFT JOIN early_graph_pagerank p
    ON d.source_subreddit = p.source_subreddit

LEFT JOIN early_graph_components c
    ON d.source_subreddit = c.source_subreddit

WHERE d.early_activity >= 50;

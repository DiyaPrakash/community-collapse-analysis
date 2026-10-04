USE community_collapse;

DROP TABLE IF EXISTS decline_cohort;

CREATE TABLE decline_cohort AS
SELECT
    source_subreddit,
    early_activity,
    late_activity,
    ROUND(
        ((early_activity - late_activity) * 100.0)
        / early_activity,
        2
    ) AS decline_percentage
FROM community_decline_analysis
WHERE early_activity >= 50
  AND late_activity < early_activity;


DROP TABLE IF EXISTS stable_cohort;

CREATE TABLE stable_cohort AS
SELECT
    source_subreddit,
    early_activity,
    late_activity,
    ROUND(
        ((late_activity - early_activity) * 100.0)
        / early_activity,
        2
    ) AS growth_percentage
FROM community_decline_analysis
WHERE early_activity >= 50
  AND late_activity >= early_activity;

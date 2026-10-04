USE community_collapse;

DROP TABLE IF EXISTS early_graph_degree;

CREATE EXTERNAL TABLE early_graph_degree (
    source_subreddit STRING,
    degree INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/community_collapse/graph/results/early_degree';


DROP TABLE IF EXISTS early_graph_pagerank;

CREATE EXTERNAL TABLE early_graph_pagerank (
    source_subreddit STRING,
    pagerank DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/community_collapse/graph/results/early_pagerank';


DROP TABLE IF EXISTS early_graph_components;

CREATE EXTERNAL TABLE early_graph_components (
    source_subreddit STRING,
    component_id BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/community_collapse/graph/results/early_components';

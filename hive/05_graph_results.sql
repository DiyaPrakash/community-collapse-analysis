USE community_collapse;

DROP TABLE IF EXISTS graph_degree;

CREATE EXTERNAL TABLE graph_degree (
    source_subreddit STRING,
    degree INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/community_collapse/graph/results/degree';


DROP TABLE IF EXISTS graph_pagerank;

CREATE EXTERNAL TABLE graph_pagerank (
    source_subreddit STRING,
    pagerank DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/community_collapse/graph/results/pagerank';

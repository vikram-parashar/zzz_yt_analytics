CREATE SEQUENCE pipeline_runs_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 14 NO CYCLE;;
CREATE TABLE bridge_agent_alias("name" VARCHAR, alias VARCHAR, PRIMARY KEY("name", alias));;
CREATE TABLE bridge_video_agent(video_id VARCHAR, agent_name VARCHAR, confidence FLOAT, attribution_weight FLOAT DEFAULT(0.0), PRIMARY KEY(video_id, agent_name));;
CREATE TABLE dim_agent("name" VARCHAR PRIMARY KEY UNIQUE, img VARCHAR, rank VARCHAR, "attribute" VARCHAR, speciality VARCHAR, faction VARCHAR, release_date DATE);;
CREATE TABLE dim_channel(channel_id VARCHAR PRIMARY KEY, channel_name VARCHAR, thumbnail VARCHAR, country VARCHAR, ingested_date DATE);;
CREATE TABLE dim_patch("version" VARCHAR, agent_name VARCHAR, banner_start DATE, banner_end DATE, PRIMARY KEY("version", agent_name));;
CREATE TABLE dim_video(video_id VARCHAR PRIMARY KEY, title VARCHAR, description VARCHAR, channel_id VARCHAR, published_at TIMESTAMP, thumbnail VARCHAR, tags VARCHAR[], duration_seconds INTEGER, ingested_date DATE, latest_view_count BIGINT, latest_like_count BIGINT, latest_comment_count BIGINT, discovery_type VARCHAR DEFAULT('popular'));;
CREATE TABLE fact_agent_daily(agent_name VARCHAR, snapshot_date DATE, attributed_views DOUBLE, attributed_likes DOUBLE, attributed_comments DOUBLE, video_count BIGINT, PRIMARY KEY(agent_name, snapshot_date));;
CREATE TABLE fact_channel_daily(channel_id VARCHAR, snapshot_date DATE, subscriber_count BIGINT, view_count BIGINT, video_count INTEGER, ingested_at TIMESTAMP DEFAULT(CURRENT_TIMESTAMP), PRIMARY KEY(channel_id, snapshot_date));;
CREATE TABLE fact_video_daily(video_id VARCHAR, snapshot_date DATE, view_count BIGINT, like_count BIGINT, comment_count BIGINT, ingested_at TIMESTAMP DEFAULT(CURRENT_TIMESTAMP), PRIMARY KEY(video_id, snapshot_date));;
CREATE TABLE pipeline_info("key" VARCHAR PRIMARY KEY, "value" VARCHAR);;
CREATE TABLE pipeline_runs(id INTEGER DEFAULT(nextval('pipeline_runs_seq')) PRIMARY KEY, pipeline VARCHAR NOT NULL, run_date DATE NOT NULL, started_at TIMESTAMP, completed_at TIMESTAMP, status VARCHAR DEFAULT('running'), rows_affected INTEGER DEFAULT(0), "error" VARCHAR);;


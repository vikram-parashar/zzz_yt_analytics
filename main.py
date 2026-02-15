import duckdb

DB_PATH = "data/warehouse.db"
con = duckdb.connect(DB_PATH)
# con.sql("""
#         drop table dim_video;
#         drop table dim_channel;
# """)
con.sql("""
        select * from fact_video_daily
""").show()

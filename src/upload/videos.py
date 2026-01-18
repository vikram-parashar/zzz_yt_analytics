import pandas as pd
import pandas_gbq

from config import BQ_DATASET,  PROJECT_ID, VIDEO_TABLE


def main():
    df = pd.read_csv("data/dim_videos.csv")
    df["publish_date"] = pd.to_datetime(df["publish_date"])
    df["discovered_at"] = pd.to_datetime(df["discovered_at"])
    # print(df["publish_date"])
    # return 

    table_id = f"{BQ_DATASET}.{VIDEO_TABLE}"
    pandas_gbq.to_gbq(df, table_id, project_id=PROJECT_ID)

    print("Upload complete")


if __name__ == "__main__":
    main()


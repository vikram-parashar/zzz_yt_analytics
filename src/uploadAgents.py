import pandas as pd
import pandas_gbq
import pendulum

from config import BQ_DATASET, AGENT_TABLE, PROJECT_ID


def main():
    df = pd.read_csv("data/dim_agent.csv")
    df["release_date"] = df["release_date"].map(
        lambda x: pendulum.from_format(x, "YYYY-MM-DD").date() if pd.notna(x) else x
    )
    # print(df)
    # return 

    table_id = f"{BQ_DATASET}.{AGENT_TABLE}"
    pandas_gbq.to_gbq(df, table_id, project_id=PROJECT_ID)

    print("Upload complete")


if __name__ == "__main__":
    main()

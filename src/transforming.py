import logging

import pandas as pd


def transform_tripdata(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting transformation")
    # datetime conversion

    df["trip_duration_min"]= df["trip_time"] / 60


    #invalid trips remove
    df=df[(df['trip_duration_min'] > 0) & (df["trip_miles"] >0)].copy()

    # viteză medie mph
    df.loc[:,"avg_speed_mph"] = df["trip_miles"] / (
            df["trip_duration_min"] / 60 + 1e-6
    )


    # Downcast numerics
    df.loc[:,"trip_miles"] = pd.to_numeric(df["trip_miles"], downcast="float")
    df.loc[:,"trip_duration_min"] = pd.to_numeric(df["trip_duration_min"], downcast="float")
    df.loc[:,"avg_speed_mph"] = pd.to_numeric(df["avg_speed_mph"], downcast="float")

    logging.info(
        f"Transformation done | Memory: "
        f"{df.memory_usage(deep=True).sum() / 1024 ** 2:.2f} MB"
    )

    return df



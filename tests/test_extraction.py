import pytest
import pandas as pd
from src.transforming import transform_tripdata

def test_transform_tripdata():
    df = pd.DataFrame({
        "trip_time": [600, 0],
        "trip_miles": [5.0, 0],
        "driver_pay": [10.0, 5.0],
        "tips": [2.0, 0.0],
        "base_passenger_fare": [12.0, 5.0],
        "pickup_datetime": pd.to_datetime(["2026-01-01 08:00", "2026-01-01 09:00"]),
        "dropoff_datetime": pd.to_datetime(["2026-01-01 08:10", "2026-01-01 09:05"]),
    })
    df_transformed = transform_tripdata(df)
    assert "trip_duration_min" in df_transformed.columns
    assert df_transformed["trip_duration_min"].min() > 0
    assert "avg_speed_mph" in df_transformed.columns


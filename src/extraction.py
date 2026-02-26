from __future__ import annotations

import pandas as pd
from src.logger import logger
from pathlib import  Path


RAW_path = Path("C:/Users/Cristi/PycharmProjects/pythonProject-S3/raw_data/tripdata_2026-01.parquet")



REQUIRED_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "trip_miles",
    "trip_time",
    "driver_pay",
    "tips",
    "base_passenger_fare"
]



def data_extraction(path:Path|str =RAW_path) ->pd.DataFrame:
    logger.info("Starting the data extraction")
    path=Path(path)

    if not path.exists():
        logger.info(f"File {path} not found ")
        raise FileNotFoundError(f"{path} not found ")

    try:
        data=pd.read_parquet(path, columns=REQUIRED_COLUMNS, engine="pyarrow")
        logger.info(f"Dataset loaded successfully| Rows: {len(data)} | Columns: {len(data.columns)}")

        return data

    except Exception as err:
        logger.exception("Error while reading the parquet file")
        raise err



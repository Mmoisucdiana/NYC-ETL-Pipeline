import logging

from src.extraction import data_extraction
from src.logger import logger
from transforming import transform_tripdata
from pathlib import Path
import sys

OUTPUT_PATH = Path("processed_data/tripdata_2026-01_processed.parquet")

def run_pipeline():
    try:
        logger.info("--------Pipeline started-------------")

        # data extraction
        df=data_extraction()
        print("Extraction successful with rows:", len(df))

        #data transformation

        df_transformed=transform_tripdata(df)

        logger.info(f"Transformation successful !Number of total rows after transform: {len(df_transformed)}")

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

        df_transformed.to_parquet(OUTPUT_PATH, engine="pyarrow", index=False)

        logger.info(f'Saved processed file to {OUTPUT_PATH}')

        logger.info("-------Pipeline Finished successfully----------")

    except Exception as e :
        logger.exception("Pipeline failed")
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()


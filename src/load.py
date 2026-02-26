from pathlib import Path
import pandas as pd
import logging

def load_data(df: pd.DataFrame, output_path: Path):
    logging.info("Starting load process")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", index=False, compression="snappy")
    logging.info(f"Data successfully saved to {output_path}")
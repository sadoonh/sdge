# load.py
from __future__ import annotations

from pathlib import Path
import os
import sys
import logging
import tempfile
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

INPUT_CSV = Path("./data/combined_utility_data.csv")

SCHEMA = "landing"
TABLE = "utility_data"

# We force these headers purely by INDEX/POSITION.
COLUMNS_HEADERS = [
    "id",
    "file_name",
    "zip_code",
    "month",
    "year",
    "customer_class",
    "combined",
    "total_customer",
    "total_kwh",
    "average_kwh",
]

CHUNKSIZE = 200_000
ON_BAD_LINES = "error"  

logger = logging.getLogger("load")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
logger.addHandler(_handler)
logger.propagate = False


def build_dsn() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER")
    pwd = os.getenv("PGPASSWORD")
    db = os.getenv("PGDATABASE")
    if not all([host, user, pwd, db]):
        raise RuntimeError("Set DATABASE_URL or PGHOST, PGUSER, PGPASSWORD, PGDATABASE (optional PGPORT).")
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"


def write_staging_csv_by_index(in_csv: Path, forced_headers: list[str]) -> tuple[Path, int]:
    """
    Create a temp CSV whose header is exactly `forced_headers` and whose columns
    are taken by POSITION from the input (truncating or padding as needed).
    The original column names are ignored.
    """
    if not in_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {in_csv}")

    # Temp file placed next to the source for easier inspection
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", dir=in_csv.parent) as tmp:
        staging_path = Path(tmp.name)

    n = len(forced_headers)
    wrote_header = False
    total_rows = 0

    read_kwargs = {"chunksize": CHUNKSIZE}
    if ON_BAD_LINES != "error":
        read_kwargs["engine"] = "python"
        read_kwargs["on_bad_lines"] = ON_BAD_LINES

    for chunk in pd.read_csv(in_csv, header=0, **read_kwargs):
        # Ensure exactly `n` columns by position
        if chunk.shape[1] >= n:
            data_part = chunk.iloc[:, :n].copy()
        else:
            # pad missing trailing columns with empty strings
            data_part = chunk.copy()
            for _ in range(n - data_part.shape[1]):
                data_part[f"__pad_{data_part.shape[1]}"] = ""
            data_part = data_part.iloc[:, :n]

        data_part.columns = forced_headers
        total_rows += len(data_part)
        data_part.to_csv(staging_path, index=False, header=not wrote_header, mode="a")
        wrote_header = True

    logger.info(f"Prepared staging file {staging_path} with {total_rows:,} rows")
    return staging_path, total_rows


def copy_into_postgres(dsn: str, csv_path: Path, schema: str, table: str, columns: list[str]) -> None:
    cols_sql = ",".join(f'"{c}"' for c in columns)
    qualified = f'"{schema}"."{table}"'
    copy_sql = f"COPY {qualified} ({cols_sql}) FROM STDIN WITH CSV HEADER"
    logger.info(f"Loading data into {qualified} using COPY")
    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur, open(csv_path, "r", encoding="utf-8") as f:
            cur.copy_expert(copy_sql, f)
    logger.info("Load complete")


def main() -> None:
    try:
        dsn = build_dsn()
        staging_path, rows = write_staging_csv_by_index(INPUT_CSV, COLUMNS_HEADERS)
        if rows == 0:
            logger.info("No rows to load. Exiting.")
            return
        try:
            copy_into_postgres(dsn, staging_path, SCHEMA, TABLE, COLUMNS_HEADERS)
        finally:
            try:
                staging_path.unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"Could not delete staging file {staging_path}: {e}")
    except Exception as e:
        logger.error(str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()

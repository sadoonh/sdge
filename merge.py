# merge.py
from pathlib import Path
import logging
import sys
from typing import Iterable
import pandas as pd

# Fixed locations
SRC_DIR = Path("./data")
OUT_CSV = SRC_DIR / "combined_utility_data.csv"

# Tunables
PATTERN = "*.csv"
CHUNKSIZE = 200_000
ON_BAD_LINES = "skip"  # "skip" or "error"
ENGINE = "python" if ON_BAD_LINES != "error" else None

FORCED_HEADER = [
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
OWNED_COLUMNS = ("id", "file_name")
FORCED_DATA_COLS = FORCED_HEADER[2:]
DATA_COL_COUNT = len(FORCED_DATA_COLS)

logger = logging.getLogger("merge")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
logger.addHandler(_handler)
logger.propagate = False


def find_input_csvs(src_dir: Path, out_csv: Path, pattern: str) -> list[Path]:
    """Return input CSVs, excluding the output file if present."""
    candidates = sorted(src_dir.glob(pattern))
    files = [f for f in candidates if f.resolve() != out_csv.resolve()]
    if not files:
        raise FileNotFoundError(f"No CSV files found in {src_dir} matching {pattern!r}")
    return files


def discover_union_columns(files: Iterable[Path]) -> list[str]:
    """
    Return the union of input headers, preserving first-seen order across files.
    Columns owned by this script (e.g., id, file_name) are excluded.
    """
    union: list[str] = []
    seen: set[str] = set()

    for path in files:
        for col in pd.read_csv(path, nrows=0).columns.tolist():
            if col not in seen:
                seen.add(col)
                union.append(col)

    for owned in OWNED_COLUMNS:
        if owned in union:
            union.remove(owned)
            logger.info(f"Found existing '{owned}' in inputs; it will be recomputed.")

    return union


def reader_kwargs(chunksize: int) -> dict:
    """Build keyword arguments for pandas.read_csv respecting error handling."""
    kwargs: dict = {"chunksize": chunksize}
    if ENGINE:
        kwargs["engine"] = ENGINE
        kwargs["on_bad_lines"] = ON_BAD_LINES
    return kwargs


def align_by_position(df: pd.DataFrame, union_cols: list[str], count: int) -> pd.DataFrame:
    """
    Align df to the union of columns (adds placeholders for missing),
    then select exactly `count` leading columns, renaming by index to FORCED_DATA_COLS.
    """
    missing = max(0, count - len(union_cols))
    placeholders = [f"__pad{i}" for i in range(missing)]
    aligned = df.reindex(columns=union_cols + placeholders)
    data = aligned.iloc[:, :count].copy()

    rename_map = {src: dst for src, dst in zip(data.columns, FORCED_DATA_COLS, strict=False)}
    return data.rename(columns=rename_map)


def merge_csvs(
    src_dir: Path = SRC_DIR,
    out_csv: Path = OUT_CSV,
    pattern: str = PATTERN,
    chunksize: int = CHUNKSIZE,
) -> Path:
    """
    Concatenate all CSVs under `src_dir` into `out_csv`.
    Output schema (by position): id, file_name, zip_code, month, year, customer_class,
    combined, total_customer, total_kwh, average_kwh
    """
    files = find_input_csvs(src_dir, out_csv, pattern)
    logger.info(f"Found {len(files)} CSV file(s) in {src_dir}")

    union_cols = discover_union_columns(files)
    logger.info(f"Discovered {len(union_cols)} union column(s) (used by index)")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if out_csv.exists():
        logger.info(f"Output exists, removing: {out_csv}")
        out_csv.unlink()

    pd.DataFrame(columns=FORCED_HEADER).to_csv(out_csv, index=False)

    total_rows = 0
    next_id = 1
    read_args = reader_kwargs(chunksize)

    for path in files:
        logger.info(f"Processing: {path.name}")
        ncols = len(pd.read_csv(path, nrows=0).columns)
        rows_in_file = 0

        for chunk in pd.read_csv(path, **read_args):
            rows = len(chunk)
            rows_in_file += rows
            total_rows += rows

            existing_owned = [c for c in OWNED_COLUMNS if c in chunk.columns]
            if existing_owned:
                chunk = chunk.drop(columns=existing_owned)

            data_part = align_by_position(chunk, union_cols, DATA_COL_COUNT)

            ids = pd.Series(range(next_id, next_id + rows), index=chunk.index, dtype="int64", name="id")
            next_id += rows

            file_col = pd.Series([path.stem] * rows, index=chunk.index, name="file_name")

            final = pd.concat([ids, file_col, data_part], axis=1).reindex(columns=FORCED_HEADER)
            final.to_csv(out_csv, index=False, header=False, mode="a")

        logger.info(f"  → shape: ({rows_in_file:,}, {ncols:,})")

    logger.info(f"Final combined shape: ({total_rows:,}, {len(FORCED_HEADER):,})")
    logger.info(f"Combined CSV written to {out_csv.resolve()}")
    return out_csv


if __name__ == "__main__":
    merge_csvs()

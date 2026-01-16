"""
dataprocessing_truths_to_parquet.py

Purpose:
Convert the ACIC22 ground-truth CSV into one Parquet file per simulation index.

What the script does:
1) Reads the truth CSV (comma-separated, UTF-8 BOM tolerant).
2) Cleans/normalizes key columns:
   - Strips whitespace in column names and key string fields.
   - Parses:
       dataset.num  -> int (required, non-null)
       year         -> nullable Int64 (optional)
       id.practice  -> nullable Int64 (optional)
       SATT         -> float
3) Validates that exactly EXPECTED_N_SIM unique dataset.num values exist.
4) Writes exactly one Parquet per dataset.num:
      truth_0001.parquet ... truth_3400.parquet

Notes:
- This script standardizes dtypes only; it does not change the substantive truth values.
- Parquet writing requires an engine such as pyarrow (recommended) or fastparquet.
"""

from __future__ import annotations


from pathlib import Path
import pandas as pd

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
ROOT = Path(r"C:\Users\User\OneDrive\Documents\1. Studium und Ausbildung\6. MA AWG Hamburg\5. Semester Masterarbeit\aus 4. Semester übertragen_Masterarbeit")
IN_PATH  = ROOT / "raw data and objects" / "ground_truth"
OUT_DIR  = ROOT / "ground_truth_parquet"

FILENAME_TEMPLATE = "truth_{idx:04d}.parquet"
EXPECTED_N_SIM = 3400

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def resolve_input_path(path: Path) -> Path:
    """
    Accepts either an explicit file path or a path without suffix and tries common suffixes.
    """
    if path.exists() and path.is_file():
        return path

    if path.suffix:
        candidates = [path]
    else:
        candidates = [path.with_suffix(ext) for ext in [".csv", ".CSV"]]

    for c in candidates:
        if c.exists():
            return c

    raise FileNotFoundError(
        "Input file not found. Tried:\n" + "\n".join(str(c) for c in candidates)
    )


def normalize_string_series(s: pd.Series) -> pd.Series:
    """
    Strips whitespace and normalizes empty strings to <NA>.
    Keeps dtype as pandas string.
    """
    s = s.astype("string")
    s = s.str.strip()
    return s.replace("", pd.NA)


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main() -> None:
    in_path = resolve_input_path(IN_PATH)
    print(f"Reading: {in_path}")

    # Your CSV header shows comma-separated values:
    # dataset.num,Confounding Strength,...,id.practice,SATT
    df = pd.read_csv(
        in_path,
        sep=",",
        encoding="utf-8-sig",
        na_values=["NA", ""],
        keep_default_na=True,
        low_memory=False,
        dtype={
            "dataset.num": "string",
            "SATT": "string",
            "variable": "string",
            "level": "string",
            "year": "string",
            "id.practice": "string",
            "Confounding Strength": "string",
            "Confounding Source": "string",
            "Impact Heterogeneity": "string",
            "Idiosyncrasy of Impacts": "string",
        },
    )

    # Defensive cleanup of column names
    df.columns = [c.strip() for c in df.columns]

    # Required columns check
    required = {"dataset.num", "SATT"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise KeyError(f"Missing required columns: {missing}. Available: {list(df.columns)}")

    # Normalize key string columns
    for c in [
        "Confounding Strength",
        "Confounding Source",
        "Impact Heterogeneity",
        "Idiosyncrasy of Impacts",
        "variable",
        "level",
    ]:
        if c in df.columns:
            df[c] = normalize_string_series(df[c])

    # year -> nullable integer
    if "year" in df.columns:
        df["year"] = normalize_string_series(df["year"])
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    # id.practice -> nullable integer
    if "id.practice" in df.columns:
        df["id.practice"] = normalize_string_series(df["id.practice"])
        df["id.practice"] = pd.to_numeric(df["id.practice"], errors="coerce").astype("Int64")

    # dataset.num -> strict integer key
    df["dataset.num"] = normalize_string_series(df["dataset.num"])
    df["dataset.num"] = pd.to_numeric(df["dataset.num"], errors="raise").astype(int)

    # SATT -> float
    # In your snippet it's already dot-decimal; this conversion is still robust if commas appear.
    satt = normalize_string_series(df["SATT"])
    satt = satt.str.replace(" ", "", regex=False)
    satt = satt.str.replace(",", ".", regex=False)
    df["SATT"] = pd.to_numeric(satt, errors="raise").astype(float)

    # Ensure output directory exists
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Sanity check: number of distinct simulations
    sims = sorted(df["dataset.num"].unique().tolist())
    n_sims = len(sims)
    print(f"Found {n_sims} distinct dataset.num values.")
    if n_sims != EXPECTED_N_SIM:
        raise ValueError(f"Expected {EXPECTED_N_SIM} simulations, but found {n_sims}.")

    # Write one parquet per dataset.num
    written = 0
    try:
        for idx, g in df.groupby("dataset.num", sort=True):
            out_path = OUT_DIR / FILENAME_TEMPLATE.format(idx=int(idx))
            g.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)
            written += 1

            if written % 200 == 0:
                print(f"  wrote {written}/{EXPECTED_N_SIM} ...")

    except Exception as e:
        raise RuntimeError(
            "Parquet writing failed. If you see an engine-related error, "
            "install pyarrow (recommended): pip install pyarrow\n"
            f"Original error: {e}"
        )

    print(f"Done. Wrote {written} parquet files to:\n{OUT_DIR}")


if __name__ == "__main__":
    main()
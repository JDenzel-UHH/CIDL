"""
acic22_dataprocessing.py

Preprocessing pipeline for ACIC Track1 data:

1) Merge CSVs inside ZIPs -> one Parquet per simulation (sim_####.parquet)
2) Enforce consistent dtypes across all Parquets (in-place overwrite)
3) Optional validation: compare a few manually merged reference Parquets against outputs

MERGE LOGIC (per dataset ####):
- Read:
    patient/acic_patient_####.csv
    patient_year/acic_patient_year_####.csv
    practice/acic_practice_####.csv
    practice_year/acic_practice_year_####.csv
- Drop column "Y" from practice_year (if present)
- Merge:
    patient LEFT JOIN patient_year on "id.patient"
    then LEFT JOIN practice on "id.practice"
    then LEFT JOIN practice_year on ["id.practice", "year"]
- Compute (Dask -> Pandas) and write:
    sim_####.parquet (pyarrow + snappy, index=False)

Reference: see official documentation "acic22_File merging instructions".

DTYPE ENFORCEMENT LOGIC:
- For each Parquet:
  - Cast columns according to DTYPES_TARGET
  - Special case: if target dtype is integer and current dtype is float,
    do fillna(0) then astype(int64) (matches original behavior)

NOTES:
- Download the raw data from the ACIC22 homepage.
- Each track is provided as a single ZIP (track1a, track1b, track1c).
- Each ZIP must contain the folders: "patient", "patient_year", "practice", "practice_year".
- The ZIP may contain additional files (e.g., PDFs); those are ignored.
"""

from __future__ import annotations

import os
import time
import zipfile
import tempfile
import shutil
from typing import Dict, List, Optional

import dask.dataframe as dd
import pandas as pd

# =============================================================================
# CONFIGURATIONS
# - BASE_FOLDER: location of raw Track1 ZIP archives
# - OUT_FOLDER: target location for merged simulation Parquets (sim_####.parquet)
# - Validation requires manually merged reference Parquets 
#   (named: merged_####.parquet) stored in MANUAL_MERGED_PATH, and their dataset
#   IDs listed in VALIDATION_ID.
# =============================================================================
BASE_FOLDER = r"C:\..."

# Input ZIPs (each contains patient/, patient_year/, practice/, practice_year/ + PDF)
TRACK1A_ZIP = os.path.join(BASE_FOLDER, "track1a_20220404.zip")
TRACK1B_ZIP = os.path.join(BASE_FOLDER, "track1b_20220404.zip")
TRACK1C_ZIP = os.path.join(BASE_FOLDER, "track1c_20220404.zip")

# Output folder
OUT_FOLDER = r"D:\..."

# Dask read parameters
DASK_BLOCKSIZE = "32MB"
BLOCK_SIZE = 10

# Which steps to run
RUN_MERGE = True
RUN_FIX_DTYPES_INPLACE = True
RUN_VALIDATION = False  # set True only if you provide MANUAL_MERGED_PATH + VALIDATION_ID

# Validation inputs (optional)
MANUAL_MERGED_PATH = r"C:\..."
VALIDATION_IDS = [
    ["0055", "1111", "1197"],  # track1a example IDs
    ["1212", "2100", "2290"],  # track1b example IDs
    ["2678", "2999", "3313"],  # track1c example IDs
]


# =============================================================================
# DTYPE TARGETS
# =============================================================================
DTYPES_TARGET: Dict[str, str] = {
    "id.patient": "int64", "id.practice": "int64", "V1": "float64", "V2": "int64", "V3": "int64",
    "V4": "float64", "V5": "object", "year": "int64", "Y": "float64", "X1": "int64", "X2": "object",
    "X3": "int64", "X4": "object", "X5": "int64", "X6": "float64", "X7": "float64", "X8": "float64",
    "X9": "float64", "Z": "int64", "post": "int64", "n.patients": "int64", "V1_avg": "float64",
    "V2_avg": "float64", "V3_avg": "float64", "V4_avg": "float64", "V5_A_avg": "float64",
    "V5_B_avg": "float64", "V5_C_avg": "float64"
}


# =============================================================================
# HELPERS
# =============================================================================
def _process_in_blocks(items: List[str], block_size: int):
    """Yield (block_number, list_of_ids) to limit memory usage."""
    for i in range(0, len(items), block_size):
        yield (i // block_size + 1), items[i:i + block_size]


def _read_dd_csv(local_csv_path: str) -> dd.DataFrame:
    """Read a CSV with Dask using the same parameters as in the original scripts."""
    return dd.read_csv(
        local_csv_path,
        sep=",",
        decimal=".",
        assume_missing=True,
        blocksize=DASK_BLOCKSIZE,
        dtype_backend="numpy_nullable",
    )


def _extract_member(zf: zipfile.ZipFile, member: str, temp_dir: str) -> str:
    """Extract a ZIP member into a temporary directory and return the local file path."""
    out_path = os.path.join(temp_dir, os.path.basename(member))
    with zf.open(member) as f_in, open(out_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return out_path


def _merge_one_dataset(zf: zipfile.ZipFile, zip_members: set[str], dataset_id: str) -> Optional[pd.DataFrame]:
    """
    Merge one dataset #### from an already-open ZipFile.
    Returns None if any required file is missing.
    """
    files_needed = {
        "patient": f"patient/acic_patient_{dataset_id}.csv",
        "patient_year": f"patient_year/acic_patient_year_{dataset_id}.csv",
        "practice": f"practice/acic_practice_{dataset_id}.csv",
        "practice_year": f"practice_year/acic_practice_year_{dataset_id}.csv",
    }

    missing = [k for k, v in files_needed.items() if v not in zip_members]
    if missing:
        print(f"Skip {dataset_id}: missing {missing}")
        return None

    with tempfile.TemporaryDirectory() as temp_dir:
        p_patient = _extract_member(zf, files_needed["patient"], temp_dir)
        p_patient_year = _extract_member(zf, files_needed["patient_year"], temp_dir)
        p_practice = _extract_member(zf, files_needed["practice"], temp_dir)
        p_practice_year = _extract_member(zf, files_needed["practice_year"], temp_dir)

        df_patient = _read_dd_csv(p_patient)
        df_patient_year = _read_dd_csv(p_patient_year)
        df_practice = _read_dd_csv(p_practice)
        df_practice_year = _read_dd_csv(p_practice_year)

        # Drop "Y" from practice_year if present (matches original scripts)
        if "Y" in df_practice_year.columns:
            df_practice_year = df_practice_year.drop(columns=["Y"])

        df_merged = (
            df_patient
            .merge(df_patient_year, on="id.patient", how="left")
            .merge(df_practice, on="id.practice", how="left")
            .merge(df_practice_year, on=["id.practice", "year"], how="left")
        )

        return df_merged.compute()


def _write_parquet(df: pd.DataFrame, out_folder: str, dataset_id: str) -> str:
    """Write one simulation dataset as Parquet: sim_####.parquet."""
    os.makedirs(out_folder, exist_ok=True)
    out_path = os.path.join(out_folder, f"sim_{dataset_id}.parquet")
    df.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)
    return out_path


# =============================================================================
# STEP 1: MERGE (ZIP -> Parquet)
# =============================================================================
def merge_zip_range(zip_path: str, out_folder: str, start_id: int, end_id: int) -> None:
    """Merge a contiguous dataset ID range from one Track ZIP."""
    os.makedirs(out_folder, exist_ok=True)
    dataset_ids = [f"{i:04d}" for i in range(start_id, end_id + 1)]

    print(f"\nMerging ZIP: {os.path.basename(zip_path)}")
    print(f"ID range: {start_id:04d}..{end_id:04d}")
    print(f"Output folder: {out_folder}")

    with zipfile.ZipFile(zip_path) as zf:
        zip_members = set(zf.namelist())

        for block_no, block in _process_in_blocks(dataset_ids, BLOCK_SIZE):
            print(f"\nMerge block {block_no}: {len(block)} datasets")

            for dataset_id in block:
                t0 = time.time()
                df = _merge_one_dataset(zf, zip_members, dataset_id)
                if df is None:
                    continue
                out_path = _write_parquet(df, out_folder, dataset_id)
                print(f"Wrote {os.path.basename(out_path)} ({time.time() - t0:.1f}s)")


# =============================================================================
# STEP 2: FIX DTYPES IN-PLACE (overwrite files)
# =============================================================================
def fix_dtypes_inplace(folder: str, start_id: int, end_id: int) -> None:
    """
    Read sim_####.parquet, enforce DTYPES_TARGET, and overwrite the same file.

    Safe overwrite:
    - write to a temporary file in the same directory
    - then os.replace(temp, final) (atomic replace on Windows)
    """
    folder = os.path.abspath(folder)
    os.makedirs(folder, exist_ok=True)

    total = end_id - start_id + 1
    print(f"\nFixing dtypes IN-PLACE: {folder}")
    print(f"Files: {total} (sim_{start_id:04d}..sim_{end_id:04d})")

    for i, sim_id in enumerate(range(start_id, end_id + 1), 1):
        final_path = os.path.join(folder, f"sim_{sim_id:04d}.parquet")
        if not os.path.exists(final_path):
            print(f"Missing: {final_path}")
            continue

        df = pd.read_parquet(final_path, engine="pyarrow")

        for col, dtype in DTYPES_TARGET.items():
            if col not in df.columns:
                continue
            if dtype.startswith("int") and pd.api.types.is_float_dtype(df[col]):
                df[col] = df[col].fillna(0).astype("int64")
            else:
                df[col] = df[col].astype(dtype)

        tmp_path = final_path + ".__tmp__"
        df.to_parquet(tmp_path, engine="pyarrow", compression="snappy", index=False)
        os.replace(tmp_path, final_path)

        if i % 50 == 0 or i == total:
            print(f"Processed {i}/{total}")


# =============================================================================
# STEP 3: VALIDATION (requires manual reference Parquets)
# =============================================================================
def validate_equals(manual_merged_path: str, sim_folder: str, ids: List[str]) -> None:
    """Compare merged_####.parquet to sim_####.parquet using df.equals()."""
    print("\nValidation (df.equals):")
    manual_merged_path = os.path.abspath(manual_merged_path)
    sim_folder = os.path.abspath(sim_folder)

    for id_ in ids:
        manual_file = os.path.join(manual_merged_path, f"merged_{id_}.parquet")
        sim_file = os.path.join(sim_folder, f"sim_{id_}.parquet")

        if not os.path.exists(manual_file):
            print(f"Missing manual reference: {manual_file}")
            continue
        if not os.path.exists(sim_file):
            print(f"Missing simulation parquet: {sim_file}")
            continue

        df_manual = pd.read_parquet(manual_file, engine="pyarrow")
        df_sim = pd.read_parquet(sim_file, engine="pyarrow")

        identical = df_manual.equals(df_sim)
        print(f"{id_}: identical = {identical}")

        if not identical:
            print("Manual head:")
            print(df_manual.head())
            print("Sim head:")
            print(df_sim.head())


# =============================================================================
# RUN
# =============================================================================
def main() -> None:
    # 1) Merge all tracks into OUT_FOLDER
    if RUN_MERGE:
        merge_zip_range(TRACK1A_ZIP, OUT_FOLDER, 1, 1200)
        merge_zip_range(TRACK1B_ZIP, OUT_FOLDER, 1201, 2400)
        merge_zip_range(TRACK1C_ZIP, OUT_FOLDER, 2401, 3400)

    # 2) Enforce datatypes (in-place)
    if RUN_FIX_DTYPES_INPLACE:
        fix_dtypes_inplace(OUT_FOLDER, 1, 3400)

    # 3) Validation (optional): compare manual references to final Parquets in OUT_FOLDER
    if RUN_VALIDATION:
        for ids in VALIDATION_IDS:
            validate_equals(MANUAL_MERGED_PATH, OUT_FOLDER, ids)


if __name__ == "__main__":
    main()

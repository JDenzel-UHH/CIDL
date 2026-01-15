#######################################################################################
#
# loader.py
#
# Handles:
# - Efficient loading of files from S3 bucket into memory or Pandas DataFrames
# - Optional caching of raw bytes + parsed metadata
# - Simulation loading by index (ACIC)
# - Metadata-driven selection (difficulty tiers / DGP properties)
#
#######################################################################################

import json
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd

import cidl.backend


# --------------------------------------------------------------------
# CONFIG / CONSTANTS
# --------------------------------------------------------------------
DEFAULT_SIM_PREFIX = "acic22/simulations"

# These can be either a local filesystem path OR an S3 key
DEFAULT_ACIC22_METADATA = "acic22/metadata/acic22_metadata.json"  # list of {index, filename, dgp}
DEFAULT_ACIC22_DGP_INFO = "acic22/metadata/acic22_dgp_info.json"   # dict with "dgps" containing difficulty tier


# Strict allowed arguments (no aliasing, no automatic underscore insertion)
VALID_DIFFICULTIES = {"all", "very_easy", "easy", "medium", "hard", "very_hard"}


# --------------------------------------------------------------------
# INTERNAL GLOBAL STATE (CACHE)
# --------------------------------------------------------------------
_CACHE: dict[str, bytes] = {}        # S3 key -> raw bytes
_META_CACHE: dict[str, object] = {}  # source -> parsed JSON object


# --------------------------------------------------------------------
# LOW-LEVEL I/O HELPERS
# --------------------------------------------------------------------
def _download_file(key: str, use_cache: bool = True) -> BytesIO:
    """
    Download a file from S3 into memory (BytesIO).

    Args:
        key: Full S3 object key (e.g., "acic22/sim_0001.parquet")
        use_cache: If True, store raw bytes in an in-memory cache

    Returns:
        BytesIO object containing the file content
    """
    if use_cache and key in _CACHE:
        return BytesIO(_CACHE[key])

    cidl.backend._ensure_connected()

    obj = cidl.backend._BUCKET.Object(key)
    buf = BytesIO()
    obj.download_fileobj(buf)
    buf.seek(0)

    if use_cache:
        _CACHE[key] = buf.getvalue()

    return buf


def _read_file_bytes(data: BytesIO, filename: str) -> pd.DataFrame:
    """
    Interpret a BytesIO payload based on file extension.
    Supported: parquet, csv, json.

    Returns:
        pd.DataFrame
    """
    name = filename.lower()
    data.seek(0)

    if name.endswith(".parquet"):
        return pd.read_parquet(data)
    if name.endswith(".csv"):
        return pd.read_csv(data)
    if name.endswith(".json"):
        # supports JSON lines and standard JSON
        try:
            return pd.read_json(data, lines=True)
        except Exception:
            data.seek(0)
            return pd.read_json(data)

    raise ValueError(f"Unsupported file type: {filename}")


def _read_json_source(source: str, use_cache: bool = True):
    """
    Read JSON either from local filesystem path or from S3 (by key).
    Caches the parsed object in _META_CACHE.

    Args:
        source: local path OR S3 key
        use_cache: cache parsed json

    Returns:
        Parsed Python object (dict/list)
    """
    if use_cache and source in _META_CACHE:
        return _META_CACHE[source]

    p = Path(source)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f)
    else:
        raw = _download_file(source, use_cache=use_cache).read()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("utf-8-sig")
        obj = json.loads(text)

    if use_cache:
        _META_CACHE[source] = obj

    return obj


# --------------------------------------------------------------------
# METADATA + SELECTION HELPERS
# --------------------------------------------------------------------
def _load_acic22_metadata(source: str = DEFAULT_ACIC22_METADATA, use_cache: bool = True) -> dict[int, dict]:
    """
    Load simulation metadata and index it by simulation index.

    Returns:
        dict: index -> record (must contain at least 'filename' and 'dgp')
    """
    obj = _read_json_source(source, use_cache=use_cache)
    if not isinstance(obj, list):
        raise ValueError(f"ACIC22 metadata JSON must be a list. Got: {type(obj)}")

    by_index: dict[int, dict] = {}
    for rec in obj:
        if "index" not in rec:
            raise ValueError("ACIC22 metadata record missing 'index'.")
        idx = int(rec["index"])
        by_index[idx] = rec

    return by_index


def _load_dgp_info(source: str = DEFAULT_ACIC22_DGP_INFO, use_cache: bool = True) -> dict[int, dict]:
    """
    Load DGP info and index it by DGP id.

    Returns:
        dict: dgp_id -> dgp_record (must contain 'difficulty_tier')
    """
    obj = _read_json_source(source, use_cache=use_cache)
    if not isinstance(obj, dict) or "dgps" not in obj:
        raise ValueError("DGP info JSON must be a dict containing key 'dgps'.")

    dgp_list = obj["dgps"]
    if not isinstance(dgp_list, list):
        raise ValueError("DGP info JSON field 'dgps' must be a list.")

    by_dgp: dict[int, dict] = {}
    for rec in dgp_list:
        if "dgp" not in rec:
            raise ValueError("DGP record missing 'dgp'.")
        by_dgp[int(rec["dgp"])] = rec

    return by_dgp


def _difficulty_to_tiers(difficulty: str | None) -> set[str] | None:
    """
    Convert a public difficulty argument into the tier labels used in the DGP info JSON.

    Allowed inputs:
        "all", "very_easy", "easy", "medium", "hard", "very_hard"

    Notes:
        - Strict mapping: "easy" -> {"easy"} (does NOT include "very_easy")
        - "all" returns None (no filtering).
    """
    if difficulty is None:
        return None

    d = str(difficulty).strip()
    if d not in VALID_DIFFICULTIES:
        allowed = ", ".join(sorted(VALID_DIFFICULTIES))
        raise ValueError(f"Invalid difficulty='{difficulty}'. Allowed values: {allowed}")

    if d == "all":
        return None

    return {d}


def _indices_for_difficulty(
    difficulty: str | None,
    metadata_source: str = DEFAULT_ACIC22_METADATA,
    dgp_info_source: str = DEFAULT_ACIC22_DGP_INFO,
    use_cache: bool = True
) -> list[int]:
    """
    Return simulation indices matching the requested difficulty filter.
    """
    meta = _load_acic22_metadata(metadata_source, use_cache=use_cache)
    dgp_info = _load_dgp_info(dgp_info_source, use_cache=use_cache)

    tiers = _difficulty_to_tiers(difficulty)
    if tiers is None:
        return sorted(meta.keys())

    allowed_dgps = {dgp_id for dgp_id, rec in dgp_info.items() if rec.get("difficulty_tier") in tiers}

    return sorted(
        idx for idx, rec in meta.items()
        if "dgp" in rec and int(rec["dgp"]) in allowed_dgps
    )


# --------------------------------------------------------------------
# PUBLIC API
# --------------------------------------------------------------------
def load_file(key: str, use_cache: bool = True) -> pd.DataFrame:
    """
    Load any supported file from S3 into a Pandas DataFrame using full key (e.g., "acic22/sim_0001.parquet").

    Supported formats: .parquet, .csv, .json
    """
    data = _download_file(key, use_cache=use_cache)
    return _read_file_bytes(data, filename=key)


def load_simulation(index: int, prefix: str = DEFAULT_SIM_PREFIX, use_cache: bool = True) -> pd.DataFrame:
    """
    Load a single ACIC simulation by its index.

    Args:
        index: simulation index (typically 1..3400)
        prefix: S3 prefix (default: "acic22")
        use_cache: cache file bytes in memory
    """
    cidl.backend._ensure_connected()

    filename = f"sim_{int(index):04d}.parquet"
    key = f"{prefix}/{filename}"

    data = _download_file(key, use_cache=use_cache)
    return _read_file_bytes(data, filename)


def load_simulations(indices: list[int], prefix: str = DEFAULT_SIM_PREFIX, use_cache: bool = True, metadata_source: str = DEFAULT_ACIC22_METADATA) -> dict[int, pd.DataFrame]:
    """
    Load multiple simulations by index.

    Uses acic22_metadata.json (index -> filename) if available; otherwise falls back to sim_{index:04d}.parquet.

    Returns:
        dict: index -> DataFrame
    """
    cidl.backend._ensure_connected()
    meta = _load_acic22_metadata(metadata_source, use_cache=use_cache)

    out: dict[int, pd.DataFrame] = {}
    for idx in indices:
        idx = int(idx)
        filename = meta.get(idx, {}).get("filename", f"sim_{idx:04d}.parquet")
        key = f"{prefix}/{filename}"
        data = _download_file(key, use_cache=use_cache)
        out[idx] = _read_file_bytes(data, filename)

    return out


def load_by_difficulty(difficulty: str, prefix: str = DEFAULT_SIM_PREFIX, use_cache: bool = True, metadata_source: str = DEFAULT_ACIC22_METADATA, dgp_info_source: str = DEFAULT_ACIC22_DGP_INFO) -> dict[int, pd.DataFrame]:
    """
    Load all simulations matching a difficulty label.

    difficulty must be one of:
        "very_easy", "easy", "medium", "hard", "very_hard"
    """
    indices = _indices_for_difficulty(
        difficulty=difficulty,
        metadata_source=metadata_source,
        dgp_info_source=dgp_info_source,
        use_cache=use_cache
    )
    return load_simulations(indices, prefix=prefix, use_cache=use_cache, metadata_source=metadata_source)


# Convenience wrappers
def load_very_easy(**kwargs) -> dict[int, pd.DataFrame]:
    return load_by_difficulty("very_easy", **kwargs)


def load_easy(**kwargs) -> dict[int, pd.DataFrame]:
    return load_by_difficulty("easy", **kwargs)


def load_medium(**kwargs) -> dict[int, pd.DataFrame]:
    return load_by_difficulty("medium", **kwargs)


def load_hard(**kwargs) -> dict[int, pd.DataFrame]:
    return load_by_difficulty("hard", **kwargs)


def load_very_hard(**kwargs) -> dict[int, pd.DataFrame]:
    return load_by_difficulty("very_hard", **kwargs)


def load_random_simulations(n: int, difficulty: str = "all", prefix: str = DEFAULT_SIM_PREFIX, seed: int | None = None, use_cache: bool = True, metadata_source: str = DEFAULT_ACIC22_METADATA, dgp_info_source: str = DEFAULT_ACIC22_DGP_INFO) -> dict[int, pd.DataFrame]:
    """
    Load n randomly sampled simulations, optionally filtered by difficulty.

    Args:
        n: number of simulations to load (must be > 0)
        difficulty: one of {"all", "very_easy", "easy", "medium", "hard", "very_hard"}
        seed: RNG seed for reproducibility
    """
    if not isinstance(n, int) or n <= 0:
        raise ValueError("n must be a positive integer.")

    eligible = _indices_for_difficulty(
        difficulty=difficulty,
        metadata_source=metadata_source,
        dgp_info_source=dgp_info_source,
        use_cache=use_cache
    )

    if n > len(eligible):
        raise ValueError(f"Requested n={n}, but only {len(eligible)} simulations available for difficulty='{difficulty}'.")

    rng = np.random.default_rng(seed)
    sampled = rng.choice(np.array(eligible, dtype=int), size=n, replace=False).tolist()
    sampled.sort()

    return load_simulations(sampled, prefix=prefix, use_cache=use_cache, metadata_source=metadata_source)


def load_prefix(prefix: str = DEFAULT_SIM_PREFIX, limit: int | None = None, use_cache: bool = True) -> dict[str, pd.DataFrame]:
    """
    Load all supported tabular files under a given S3 prefix into memory.

    This function lists every S3 object whose key starts with `prefix/` and attempts to load it
    as a Pandas DataFrame (supported formats: .parquet, .csv, .json). It is convenient for small
    prefixes, but can be memory-intensive for large collections (e.g., thousands of simulations).

    Args:
        prefix: S3 prefix to scan (default: "acic22")
        limit: if set, load only the first `limit` objects returned by the S3 listing
        use_cache: if True, cache raw object bytes in memory for faster repeated access

    Returns:
        dict: s3_key -> DataFrame (only successfully loaded files are included)
    """
    cidl.backend._ensure_connected()

    keys = [obj.key for obj in cidl.backend._BUCKET.objects.filter(Prefix=prefix)]
    if limit is not None:
        keys = keys[:limit]

    out: dict[str, pd.DataFrame] = {}
    for key in keys:
        try:
            out[key] = load_file(key, use_cache=use_cache)
        except Exception as e:
            print(f"Failed to load {key}: {e}")

    return out

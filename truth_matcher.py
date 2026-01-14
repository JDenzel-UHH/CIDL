#######################################################################################
#
# truth_loader.py
#
# Handles:
# - Index-based loading of ACIC22 ground-truth files from S3
# - Automatic matching of truth files to already-loaded simulations (by index)
# - Clear mismatch diagnostics (missing / extra indices)
# - Default behavior: warnings (not exceptions), with optional interactive prompt
#
# Notes:
# - This module is intentionally limited to loading + matching/validation.
# - Model evaluation / scoring should live in a separate module (e.g., evaluation.py).
#
#######################################################################################

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Iterable, Any, Optional
import sys
import warnings

import pandas as pd
from botocore.exceptions import ClientError

import cidl.loaders as loaders


# --------------------------------------------------------------------
# CONFIG / CONSTANTS
# --------------------------------------------------------------------
DEFAULT_TRUTH_PREFIX = "acic22/truth"
VALID_ON_MISMATCH = {"warn", "error", "ignore"}
VALID_ON_MISSING = {"warn", "error", "ignore"}


# --------------------------------------------------------------------
# BUNDLE TYPE
# --------------------------------------------------------------------
@dataclass(frozen=True)
class TruthBundle:
    """
    Container that keeps truth loading results transparent and reproducible.

    truth: dict[int, pd.DataFrame] with keys equal to indices successfully loaded
    missing_truth_files: indices that were requested but not found in storage
    missing_for_simulations: simulation indices that do NOT have truth loaded
    extra_truth: loaded truth indices that are not part of simulation indices
    warnings: human-readable warnings that were emitted/collected
    """
    simulation_indices: list[int]
    truth_indices_requested: list[int]
    truth: dict[int, pd.DataFrame]

    missing_truth_files: list[int]
    missing_for_simulations: list[int]
    extra_truth: list[int]

    warnings: list[str]

    @property
    def is_full_match(self) -> bool:
        return (len(self.missing_for_simulations) == 0) and (len(self.extra_truth) == 0)

    @property
    def truth_indices_loaded(self) -> list[int]:
        return sorted(self.truth.keys())


# --------------------------------------------------------------------
# INTERNAL HELPERS
# --------------------------------------------------------------------
def _normalize_indices(indices: Any) -> list[int]:
    """
    Normalize various index inputs into a sorted list of unique ints.
    Accepts: list/tuple/set/iterable of numbers/strings, dict keys, etc.
    """
    if indices is None:
        return []

    if isinstance(indices, Mapping):
        raw = list(indices.keys())
    elif isinstance(indices, (list, tuple, set)):
        raw = list(indices)
    else:
        # allow any iterable (e.g., numpy array), but treat single int as scalar
        if isinstance(indices, (int,)):
            raw = [indices]
        else:
            raw = list(indices)

    out: set[int] = set()
    for x in raw:
        try:
            out.add(int(x))
        except Exception as e:
            raise TypeError(f"Index value '{x}' cannot be converted to int.") from e

    return sorted(out)


def _truth_key(index: int, prefix: str) -> str:
    prefix = str(prefix).strip("/")
    filename = f"truth_{int(index):04d}.parquet"
    return f"{prefix}/{filename}"


def _is_missing_s3_object_error(e: ClientError) -> bool:
    """
    Heuristic for S3 'not found' errors across providers.
    """
    code = str(e.response.get("Error", {}).get("Code", "")).strip()
    # Common patterns: "404", "NoSuchKey", "NotFound"
    return code in {"404", "NoSuchKey", "NotFound"} or "Not Found" in str(e)


def _can_prompt() -> bool:
    try:
        return sys.stdin is not None and sys.stdin.isatty()
    except Exception:
        return False


def _format_mismatch_message(
    sim_indices: list[int],
    truth_loaded: list[int],
    missing_for_sims: list[int],
    extra_truth: list[int],
    missing_files: list[int],
) -> str:
    parts: list[str] = []
    parts.append("Truth/Simulation mismatch detected.")
    parts.append(f"- Simulation indices: {sim_indices}")
    parts.append(f"- Truth indices loaded: {truth_loaded}")

    if missing_for_sims:
        parts.append(f"- Missing truth for simulation indices: {missing_for_sims}")
    if extra_truth:
        parts.append(f"- Extra truth indices (not in simulations): {extra_truth}")
    if missing_files:
        parts.append(f"- Truth files not found in storage: {missing_files}")

    parts.append(
        "Hint: Use automatic index matching (bundle_for_simulations(simulations)) "
        "to load exactly the truth datasets corresponding to your simulations."
    )
    return "\n".join(parts)


def _handle_mismatch(
    msg: str,
    on_mismatch: str,
    prompt: bool,
    warnings_out: list[str],
) -> None:
    """
    Apply mismatch policy: warn/error/ignore, optionally prompt to continue.
    """
    if on_mismatch not in VALID_ON_MISMATCH:
        raise ValueError(f"on_mismatch must be one of {sorted(VALID_ON_MISMATCH)}")

    if on_mismatch == "ignore":
        return

    if on_mismatch == "error":
        raise ValueError(msg)

    # warn
    warnings_out.append(msg)
    warnings.warn(msg, UserWarning)

    if prompt:
        if _can_prompt():
            answer = input("Mismatch detected. Continue anyway? [y/N]: ").strip().lower()
            if answer not in {"y", "yes"}:
                raise RuntimeError("Aborted by user due to truth/simulation mismatch.")
        else:
            # do not block in non-interactive contexts
            warnings_out.append("Prompt requested, but environment is non-interactive; proceeding with warning only.")
            warnings.warn(
                "Prompt requested, but environment is non-interactive; proceeding with warning only.",
                UserWarning
            )


def _handle_missing_file(
    idx: int,
    key: str,
    on_missing: str,
    warnings_out: list[str],
) -> None:
    """
    Apply missing-file policy: warn/error/ignore for missing truth files in storage.
    """
    if on_missing not in VALID_ON_MISSING:
        raise ValueError(f"on_missing must be one of {sorted(VALID_ON_MISSING)}")

    if on_missing == "ignore":
        return

    msg = f"Truth file not found for index {idx} (key='{key}')."

    if on_missing == "error":
        raise FileNotFoundError(msg)

    warnings_out.append(msg)
    warnings.warn(msg, UserWarning)


# --------------------------------------------------------------------
# PUBLIC API
# --------------------------------------------------------------------
def load_truth(index: int, prefix: str = DEFAULT_TRUTH_PREFIX, use_cache: bool = True) -> pd.DataFrame:
    """
    Load a single truth file by simulation index.
    """
    key = _truth_key(int(index), prefix=prefix)
    return loaders.load_file(key, use_cache=use_cache)


def load_truths(
    indices: Iterable[int],
    prefix: str = DEFAULT_TRUTH_PREFIX,
    use_cache: bool = True,
    on_missing: str = "warn",
) -> TruthBundle:
    """
    Load multiple truth files by index (indices-only).

    IMPORTANT:
      - This function intentionally does NOT accept a simulations dict.
      - If you already have simulations (dict[int, ...]), use truth_for_simulations(simulations).
    """
    # Enforce Variant B: no Mapping/dict accepted here
    if isinstance(indices, Mapping):
        raise TypeError(
            "load_truths() expects an iterable of indices (e.g., [1,2,3]). "
            "You passed a Mapping/dict. Use truth_for_simulations(simulations) instead."
        )

    idxs = _normalize_indices(indices)
    warnings_out: list[str] = []

    truth: dict[int, pd.DataFrame] = {}
    missing_files: list[int] = []

    for idx in idxs:
        key = _truth_key(idx, prefix=prefix)
        try:
            truth[idx] = loaders.load_file(key, use_cache=use_cache)
        except ClientError as e:
            if _is_missing_s3_object_error(e):
                missing_files.append(idx)
                _handle_missing_file(idx, key, on_missing=on_missing, warnings_out=warnings_out)
            else:
                raise
        except FileNotFoundError:
            missing_files.append(idx)
            _handle_missing_file(idx, key, on_missing=on_missing, warnings_out=warnings_out)

    loaded = sorted(truth.keys())
    missing_for_requested = sorted(set(idxs) - set(loaded))

    # In indices-only mode there is no separate "simulation" context; we mirror requested indices.
    return TruthBundle(
        simulation_indices=idxs,
        truth_indices_requested=idxs,
        truth=truth,
        missing_truth_files=sorted(missing_files),
        missing_for_simulations=missing_for_requested,
        extra_truth=[],
        warnings=warnings_out,
    )


def truth_for_simulations(
    simulations: Mapping[int, Any],
    *,
    truth_indices: Optional[Iterable[int]] = None,
    prefix: str = DEFAULT_TRUTH_PREFIX,
    use_cache: bool = True,
    on_missing: str = "warn",
    on_mismatch: str = "warn",
    prompt: bool = False,
) -> TruthBundle:
    """
    Load truth matched to a simulations dict (Mapping[int, ...]) and return a TruthBundle.

    Standard usage (recommended):
        sims = loaders.load_random_simulations(10)
        bundle = truth_for_simulations(sims)  # loads truth for sims.keys()

    Advanced usage (explicit truth selection; validated against simulations):
        bundle = truth_for_simulations(sims, truth_indices=[...], prompt=True)

    Behavior:
      - If truth_indices is None: truth indices are exactly sims.keys() (automatic matching).
      - If truth_indices is provided: load those truth indices and compare vs sims.keys().
        On mismatch, emits warning (default) and optionally prompts.
    """
    if not isinstance(simulations, Mapping):
        raise TypeError(
            "truth_for_simulations() expects a Mapping/dict with simulation indices as keys. "
            "If you only have indices, use load_truths(indices)."
        )

    sim_indices = _normalize_indices(simulations.keys())
    truth_requested = sim_indices if truth_indices is None else _normalize_indices(truth_indices)

    loaded_bundle = load_truths(
        truth_requested,
        prefix=prefix,
        use_cache=use_cache,
        on_missing=on_missing,
    )

    truth_loaded = loaded_bundle.truth_indices_loaded

    # Compare loaded truth indices to simulation indices
    missing_for_sims = sorted(set(sim_indices) - set(truth_loaded))
    extra_truth = sorted(set(truth_loaded) - set(sim_indices))

    warnings_out = list(loaded_bundle.warnings)

    # Only run mismatch warning/prompt when user explicitly overrides truth_indices
    if truth_indices is not None and (missing_for_sims or extra_truth):
        msg = _format_mismatch_message(
            sim_indices=sim_indices,
            truth_loaded=truth_loaded,
            missing_for_sims=missing_for_sims,
            extra_truth=extra_truth,
            missing_files=loaded_bundle.missing_truth_files,
        )
        _handle_mismatch(
            msg=msg,
            on_mismatch=on_mismatch,
            prompt=prompt,
            warnings_out=warnings_out,
        )

    return TruthBundle(
        simulation_indices=sim_indices,
        truth_indices_requested=truth_requested,
        truth=loaded_bundle.truth,
        missing_truth_files=loaded_bundle.missing_truth_files,
        missing_for_simulations=missing_for_sims,
        extra_truth=extra_truth,
        warnings=warnings_out,
    )


# Optional: backward-compatible alias (keeps old name, but now strictly sims-only by signature above)
bundle_for_simulations = truth_for_simulations

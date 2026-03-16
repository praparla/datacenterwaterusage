"""Deduplication engine for multi-source data center water records.

When 20+ scrapers feed the same pipeline, the same document or data point
may appear from different sources (e.g., a permit in both DEQ ArcGIS and
EPA ECHO).  This module identifies and merges duplicates.

Matching strategy (in priority order):
1. Exact match on (permit_number, document_date, source_portal) — same record re-scraped
2. Exact match on (permit_number, document_date) across portals — cross-source duplicate
3. Exact match on source_url — same document fetched twice
4. Fuzzy match on document_title for the same permit — near-duplicate titles

When duplicates are found the most *complete* and *recent* record wins.
A ``sources`` field is added listing every portal that produced the record.
"""

from __future__ import annotations

import re
from datetime import datetime
from difflib import SequenceMatcher

import pandas as pd


def _normalize_title(title: str) -> str:
    """Lowercase, strip whitespace/punctuation for fuzzy comparison."""
    if not isinstance(title, str):
        return ""
    title = title.lower().strip()
    title = re.sub(r"[^a-z0-9\s]", "", title)
    return re.sub(r"\s+", " ", title)


def title_similarity(a: str, b: str) -> float:
    """Return 0-1 similarity between two document titles."""
    na, nb = _normalize_title(a), _normalize_title(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


def _completeness_score(row: pd.Series) -> int:
    """Score how many useful fields a row has filled in."""
    score = 0
    for col in (
        "extracted_water_metric",
        "extracted_quote",
        "permit_number",
        "company_llc_name",
        "document_date",
        "document_url",
        "local_file_path",
    ):
        if col in row.index:
            val = row[col]
            if isinstance(val, str):
                if val.strip():
                    score += 1
            elif pd.notna(val):
                score += 1
    return score


def _pick_best_row(group: pd.DataFrame) -> pd.Series:
    """From a group of duplicate rows, pick the best and annotate sources."""
    if len(group) == 1:
        row = group.iloc[0].copy()
        row["sources"] = row.get("source_portal", "")
        return row

    # Collect all source portals
    all_sources = sorted(group["source_portal"].dropna().unique())

    # Score each row: completeness + recency
    scored = group.copy()
    scored["_completeness"] = scored.apply(_completeness_score, axis=1)
    scored["_scraped_ts"] = pd.to_datetime(scored["scraped_at"], errors="coerce")

    # Sort: most complete first, then most recent
    scored = scored.sort_values(
        ["_completeness", "_scraped_ts"], ascending=[False, False]
    )
    best = scored.iloc[0].copy()
    best["sources"] = "; ".join(all_sources)

    # Merge non-empty fields from other rows into best
    for _, other in scored.iloc[1:].iterrows():
        for col in group.columns:
            if col.startswith("_"):
                continue
            best_val = best.get(col)
            other_val = other.get(col)
            if (pd.isna(best_val) or best_val == "") and pd.notna(other_val) and other_val != "":
                best[col] = other_val

    # Drop internal columns
    for col in ("_completeness", "_scraped_ts"):
        if col in best.index:
            best = best.drop(col)

    return best


def deduplicate(
    df: pd.DataFrame,
    title_threshold: float = 0.85,
) -> pd.DataFrame:
    """Deduplicate a results DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The full results data (from results.csv).
    title_threshold : float
        Minimum SequenceMatcher ratio to consider two titles duplicates
        (only applied when permit_number matches).

    Returns
    -------
    pd.DataFrame
        Deduplicated DataFrame with an added ``sources`` column.
    """
    if df.empty:
        if "sources" not in df.columns:
            df["sources"] = pd.Series(dtype=str)
        return df

    df = df.copy()

    # Ensure scraped_at is datetime for recency comparison
    if "scraped_at" in df.columns:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")

    # --- Pass 1: Exact URL dedup ---
    # Group by source_url (non-empty only)
    url_mask = df["source_url"].notna() & (df["source_url"] != "")
    url_groups = df[url_mask].groupby("source_url")

    kept_indices = set()
    results = []

    for _url, group in url_groups:
        best = _pick_best_row(group)
        results.append(best)
        kept_indices.update(group.index)

    # Add rows without URLs as-is
    for idx in df.index:
        if idx not in kept_indices:
            row = df.loc[idx].copy()
            row["sources"] = row.get("source_portal", "")
            results.append(row)

    pass1_df = pd.DataFrame(results)
    if pass1_df.empty:
        pass1_df["sources"] = pd.Series(dtype=str)
        return pass1_df

    # --- Pass 2: Permit + date dedup ---
    has_permit = (
        pass1_df["permit_number"].notna() & (pass1_df["permit_number"] != "")
    )
    permit_df = pass1_df[has_permit].copy()
    no_permit_df = pass1_df[~has_permit].copy()

    if not permit_df.empty:
        # Normalize document_date for grouping
        permit_df["_date_key"] = pd.to_datetime(
            permit_df["document_date"], errors="coerce"
        ).dt.strftime("%Y-%m")

        deduped_permit = []
        for (_permit, _date), group in permit_df.groupby(
            ["permit_number", "_date_key"], dropna=False
        ):
            if len(group) <= 1:
                deduped_permit.append(group.iloc[0])
            else:
                deduped_permit.append(_pick_best_row(group))

        permit_df = pd.DataFrame(deduped_permit)
        if "_date_key" in permit_df.columns:
            permit_df = permit_df.drop(columns=["_date_key"])

    result_df = pd.concat([permit_df, no_permit_df], ignore_index=True)

    # --- Pass 3: Fuzzy title dedup within same permit ---
    if title_threshold < 1.0 and not result_df.empty:
        has_permit2 = (
            result_df["permit_number"].notna() & (result_df["permit_number"] != "")
        )
        to_fuzzy = result_df[has_permit2]
        no_fuzzy = result_df[~has_permit2]

        fuzzy_results = []
        for permit, group in to_fuzzy.groupby("permit_number"):
            if len(group) <= 1:
                fuzzy_results.append(group)
                continue

            # Pairwise title comparison — merge similar titles
            merged_indices = set()
            clusters = []
            indices = list(group.index)

            for i, idx_a in enumerate(indices):
                if idx_a in merged_indices:
                    continue
                cluster = [idx_a]
                title_a = group.loc[idx_a].get("document_title", "")
                for idx_b in indices[i + 1 :]:
                    if idx_b in merged_indices:
                        continue
                    title_b = group.loc[idx_b].get("document_title", "")
                    if title_similarity(title_a, title_b) >= title_threshold:
                        cluster.append(idx_b)
                        merged_indices.add(idx_b)
                merged_indices.add(idx_a)
                clusters.append(cluster)

            for cluster_idxs in clusters:
                cluster_df = group.loc[cluster_idxs]
                best = _pick_best_row(cluster_df)
                fuzzy_results.append(pd.DataFrame([best]))

        if fuzzy_results:
            result_df = pd.concat(
                [pd.DataFrame(no_fuzzy)] + fuzzy_results, ignore_index=True
            )
        else:
            result_df = no_fuzzy.reset_index(drop=True)

    # Ensure sources column exists
    if "sources" not in result_df.columns:
        result_df["sources"] = result_df.get("source_portal", "")

    return result_df.reset_index(drop=True)

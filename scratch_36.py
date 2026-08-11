"""
Export each ACTIVE Trader Hunting pattern as its own .sql file into
active_queries/, matching the repo layout. Inactive patterns get exported
into inactive_queries/ if EXPORT_INACTIVE is True.

Just press play.
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from ai.snowflake.compat.connection import sf


# ============================================================================
# CONFIG
# ============================================================================
REPO_ROOT        = Path("/Users/trevorjosephallen/PycharmProjects/TFA-Trader-Hunting-Automation")
ACTIVE_DIR       = REPO_ROOT / "active_queries"
INACTIVE_DIR     = REPO_ROOT / "inactive_queries"
EXPORT_INACTIVE  = True    # also write inactive patterns to inactive_queries/
CLEAN_STALE      = True    # delete .sql files in active_queries/ that no longer exist as active in Snowflake
# ============================================================================

PATTERNS_TABLE = "gbi_fraud_bap_db.ai_live_biz_app.TRADER_HUNTING_PATTERNS"
VIEW_SCHEMA    = "gbi_fraud_bap_db.ai_live_biz_app"


def load_patterns():
    q = f"""
        SELECT TABLE_NAME, CREATED_BY, CREATED_TS, QUERY_TYPE, ACTIVITY
        FROM {PATTERNS_TABLE}
        ORDER BY TABLE_NAME
    """
    df = sf.run_query(q)
    df.columns = [c.upper() for c in df.columns]
    df = df.drop_duplicates(subset=["TABLE_NAME"], keep="last").reset_index(drop=True)
    return df


def get_ddl(table_name):
    q = f"select get_ddl('view', '{VIEW_SCHEMA}.{table_name}') as query"
    try:
        df = sf.run_query(q)
        df.columns = [c.upper() for c in df.columns]
        return df["QUERY"].iloc[0]
    except Exception as e:
        print(f"    ! DDL fetch failed for {table_name}: {e}")
        return None


def format_file(row, ddl):
    header = (
        f"-- Pattern:    {row['TABLE_NAME']}\n"
        f"-- Type:       {row['QUERY_TYPE']}\n"
        f"-- Activity:   {row['ACTIVITY']}\n"
        f"-- Created by: {row['CREATED_BY']}\n"
        f"-- Created:    {row['CREATED_TS']}\n"
        f"-- Auto-generated from Snowflake get_ddl — do not edit by hand.\n\n"
    )
    return header + ddl.strip().rstrip(";") + ";\n"


def write_patterns(df, target_dir, label):
    target_dir.mkdir(parents=True, exist_ok=True)
    written = []
    print(f"\n→ Writing {len(df)} {label} patterns to {target_dir}")
    print("─" * 70)
    for i, (_, row) in enumerate(df.iterrows(), 1):
        name = row["TABLE_NAME"]
        print(f"  [{i:>3}/{len(df)}] {name:<50} ", end="", flush=True)
        ddl = get_ddl(name)
        if not ddl:
            print("FAILED")
            continue
        path = target_dir / f"{name}.sql"
        path.write_text(format_file(row, ddl), encoding="utf-8")
        written.append(path.name)
        print(f"✓ ({len(ddl):,} chars)")
    return written


def clean_stale(target_dir, keep_names):
    """Remove .sql files not in keep_names. Preserves .gitkeep."""
    if not target_dir.exists():
        return
    removed = []
    for f in target_dir.glob("*.sql"):
        if f.name not in keep_names:
            f.unlink()
            removed.append(f.name)
    if removed:
        print(f"\n🗑  Removed {len(removed)} stale files from {target_dir.name}:")
        for n in removed:
            print(f"     - {n}")


def run():
    print("Loading patterns from Snowflake...")
    df = load_patterns()
    active   = df[df["ACTIVITY"] == "Active"].reset_index(drop=True)
    inactive = df[df["ACTIVITY"] == "Inactive"].reset_index(drop=True)
    print(f"Loaded {len(df)} total: {len(active)} active, {len(inactive)} inactive")

    active_written = write_patterns(active, ACTIVE_DIR, "active")
    if CLEAN_STALE:
        clean_stale(ACTIVE_DIR, set(active_written))

    if EXPORT_INACTIVE:
        inactive_written = write_patterns(inactive, INACTIVE_DIR, "inactive")
        if CLEAN_STALE:
            clean_stale(INACTIVE_DIR, set(inactive_written))

    print("\n✅ Done. Next: cd into repo and run the git commands.")


run()
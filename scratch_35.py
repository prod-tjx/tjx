"""
Export Trader Hunting pattern DDLs into a single consolidated SQL file,
grouped and labeled by pattern type (TH vs GCT).

Just press play. No CLI args. Tweak the CONFIG block below if needed.
"""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from ai.snowflake.compat.connection import sf


# ============================================================================
# CONFIG — edit these if you want, otherwise just press play
# ============================================================================
OUTPUT_FILE      = "patterns_active.sql"   # where to write the consolidated DDL
INCLUDE_INACTIVE = False                   # True = include inactive patterns too
SPLIT_BY_TYPE    = False                   # True = write patterns_TH.sql + patterns_GCT.sql
# ============================================================================


PATTERNS_TABLE = "gbi_fraud_bap_db.ai_live_biz_app.TRADER_HUNTING_PATTERNS"
VIEW_SCHEMA    = "gbi_fraud_bap_db.ai_live_biz_app"

TYPE_LABELS = {
    "TH":  "Trader Hunting Patterns",
    "GCT": "Good Customer Patterns",
}


# ---------- Snowflake I/O ----------

def load_patterns(include_inactive=False):
    where = "" if include_inactive else "WHERE ACTIVITY = 'Active'"
    q = f"""
        SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CREATED_BY,
               CREATED_TS, QUERY_TYPE, ACTIVITY
        FROM {PATTERNS_TABLE}
        {where}
        ORDER BY QUERY_TYPE, TABLE_NAME
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


# ---------- Formatting ----------

def extract_columns(ddl):
    if not ddl:
        return []
    m = re.search(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+[\w\.]+\s*\(([^)]+)\)", ddl, re.IGNORECASE)
    if not m:
        return []
    return [c.strip() for c in m.group(1).split(",") if c.strip()]


def banner(text, char="=", width=80):
    line = char * width
    return f"-- {line}\n-- {text}\n-- {line}\n"


def format_pattern_block(row, ddl):
    cols = extract_columns(ddl)
    cols_str = ", ".join(cols) if cols else "(could not parse)"
    header = (
        f"-- {'-' * 78}\n"
        f"-- Pattern:    {row['TABLE_NAME']}\n"
        f"-- Type:       {row['QUERY_TYPE']} ({TYPE_LABELS.get(row['QUERY_TYPE'], 'Unknown')})\n"
        f"-- Activity:   {row['ACTIVITY']}\n"
        f"-- Created by: {row['CREATED_BY']}\n"
        f"-- Created:    {row['CREATED_TS']}\n"
        f"-- Columns ({len(cols)}): {cols_str}\n"
        f"-- {'-' * 78}\n"
    )
    ddl_clean = ddl.strip().rstrip(";") + ";"
    return header + ddl_clean + "\n\n"


def build_toc(df):
    lines = ["-- TABLE OF CONTENTS", "--"]
    for qtype, label in TYPE_LABELS.items():
        group = df[df["QUERY_TYPE"] == qtype]
        if group.empty:
            continue
        lines.append(f"-- [{qtype}] {label} ({len(group)})")
        for name in group["TABLE_NAME"].tolist():
            lines.append(f"--     - {name}")
        lines.append("--")
    return "\n".join(lines) + "\n\n"


def build_file_header(df, include_inactive):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    scope = "ALL patterns (active + inactive)" if include_inactive else "ACTIVE patterns only"
    counts = df["QUERY_TYPE"].value_counts().to_dict()
    counts_str = ", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "none"
    return (
        banner("TRADER HUNTING — CONSOLIDATED PATTERN DDL EXPORT")
        + f"-- Generated:  {now}\n"
        + f"-- Scope:      {scope}\n"
        + f"-- Total:      {len(df)}  ({counts_str})\n"
        + f"-- Source:     {PATTERNS_TABLE}\n"
        + "--\n"
        + "-- Auto-generated — re-run the script to refresh from Snowflake.\n"
        + "-- " + "=" * 78 + "\n\n"
    )


# ---------- Orchestration ----------

def write_consolidated(df, out_path, include_inactive):
    total = len(df)
    written = 0
    failed = 0

    print(f"\n→ Writing {total} patterns to {out_path}")
    print("─" * 70)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(build_file_header(df, include_inactive))
        f.write(build_toc(df))

        for qtype, label in TYPE_LABELS.items():
            group = df[df["QUERY_TYPE"] == qtype]
            if group.empty:
                continue

            print(f"\n[{qtype}] {label}  ({len(group)} patterns)")
            f.write(banner(f"SECTION: {qtype} — {label}  ({len(group)} patterns)"))
            f.write("\n")

            for i, (_, row) in enumerate(group.iterrows(), 1):
                name = row["TABLE_NAME"]
                print(f"  [{i:>3}/{len(group)}] {name:<50} ", end="", flush=True)

                ddl = get_ddl(name)
                if not ddl:
                    f.write(f"-- !! Skipped {name} — DDL fetch failed\n\n")
                    failed += 1
                    print("FAILED")
                    continue

                f.write(format_pattern_block(row, ddl))
                f.flush()

                cols = extract_columns(ddl)
                written += 1
                print(f"✓ ({len(cols)} cols, {len(ddl):,} chars)")

    print("─" * 70)
    print(f"Done: {written} written, {failed} failed → {out_path}")


def run():
    print("Loading patterns from Snowflake...")
    df = load_patterns(include_inactive=INCLUDE_INACTIVE)
    if df.empty:
        print("No patterns found. Nothing to export.")
        return

    counts = df["QUERY_TYPE"].value_counts().to_dict()
    print(f"Loaded {len(df)} patterns: " + ", ".join(f"{k}={v}" for k, v in counts.items()))

    if SPLIT_BY_TYPE:
        for qtype in TYPE_LABELS:
            group = df[df["QUERY_TYPE"] == qtype].reset_index(drop=True)
            if group.empty:
                print(f"[{qtype}] no patterns, skipping")
                continue
            write_consolidated(group, f"patterns_{qtype}.sql", INCLUDE_INACTIVE)
    else:
        write_consolidated(df, OUTPUT_FILE, INCLUDE_INACTIVE)


# Run on press-play
run()
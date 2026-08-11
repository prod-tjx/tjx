Help me turn this into a python script that will consoolidate the ddl outputs into a single file with column names with what is activee so i can upload them all to a repo
5:55 PM

I'll build a script that pulls all active patterns from Snowflake and writes their DDLs into a single consolidated file, organized and labeled so it's repo-ready.
Consolidation script for active TH pattern DDLs
Claude's response was interrupted

labeled by pattern type
5:55 PM

Got it — I'll restructure so output is clearly labeled/grouped by pattern type (TH vs GCT), with a table-of-contents header and per-section dividers.
Consolidation script for active pattern DDLs, grouped by type
Claude's response was interrupted

SHOW OUTPUT WHILE WRITING PATTERNS
5:56 PM
Created 2 files, ran 2 commands, read a file

Reuses your existing sf.run_query connection and DDL-fetch logic, then streams output so you can watch it work.

Run it:
bash

python export_active_patterns.py                    # patterns_active.sql (active only)
python export_active_patterns.py --include-inactive # everything
python export_active_patterns.py --split-by-type    # patterns_TH.sql + patterns_GCT.sql

Live progress looks like:

Loading patterns from Snowflake...
Loaded 47 patterns: TH=38, GCT=9

→ Writing 47 patterns to patterns_active.sql
──────────────────────────────────────────────────────────────────────

[TH] Trader Hunting Patterns  (38 patterns)
  [  1/38] TRADER_SUSPICIOUS_EMAIL_01                          ✓ (56 cols, 2,143 chars)
  [  2/38] TRADER_CANCEL_REQUOTE_PT                            ✓ (56 cols, 1,987 chars)
  ...

Output file structure: header with timestamp/counts → TOC grouped by type → SECTION: TH banner with every TH pattern block (metadata comment + column list + CREATE OR REPLACE VIEW...;) → SECTION: GCT same way. Each DDL is terminated with ; so the whole file is replayable as a single SQL script.

One thing worth flagging for repo hygiene: since CREATED_TS is in every block header, the file will diff on every run even when no DDL actually changed. If you want clean diffs, drop CREATED_TS from format_pattern_block — the tracking table still has the authoritative timestamp.
Export active patterns
PY

Just want to run the entire python file without using commands just as a scratch

make it as accesible as possible if i pressed -play form the top of the file
5:59 PM

Here's a zero-argument version — just hit play and it runs.
Ran a command, created a file, read a file

Press play, it runs. Three toggles at the top if you want to change behavior:
python

OUTPUT_FILE      = "patterns_active.sql"
INCLUDE_INACTIVE = False
SPLIT_BY_TYPE    = False

No if __name__ == "__main__" guard, no argparse — run() is called at module level so it fires whether you hit play in PyCharm, run the cell in Jupyter, or shift-enter the whole file. Output lands next to wherever your working directory is pointed.
Export active patterns
PY

/Users/trevorjosephallen/PycharmProjects/TFA-Trader-Hunting-Automation/.venv/bin/python /Users/trevorjosephallen/Library/Application Support/JetBrains/PyCharm2025.3/scratches/scratch_32.py
Traceback (most recent call last):
  File "/Users/trevorjosephallen/Library/Application Support/JetBrains/PyCharm2025.3/scratches/scratch_32.py", line 50, in <module>
    def get_ddl(table_name: str) -> str | None:
TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'
Process finished with exit code 1
6:01 PM

That's a Python <3.10 syntax issue — the str | None union. Your venv is on an older Python. Easy fix, I'll swap to Optional.
Remove Python 3.10+ union syntax

Wait — that function already had no annotations. Let me check what's actually on line 50:
Viewed 2 files

Your traceback says the error is at line 50, but the current file is clean — PyCharm scratches don't auto-sync when you re-download. You're still running the old scratch content.

Copy the fresh file contents below into your scratch (select all → paste over):
Presented file

The current file has zero str | None annotations anywhere, so once you replace the scratch contents it'll run. Alternatively, if you want the union syntax to work going forward, add this as the very first line of any file:
python

from __future__ import annotations

That makes all annotations lazy strings and str | None works on Python 3.7+.
Export active patterns
PY

Im on 3.9 python venv
6:01 PM

Good — 3.9 is exactly why it broke. The file I just gave you has no union annotations, so it'll work on 3.9 as-is. Just make sure your PyCharm scratch has the current contents (paste over the old scratch).

For extra safety against any lingering X | Y I might have missed, add this one line at the very top of the file — it makes all type hints work on 3.9:
python

from __future__ import annotations

Put it above the docstring or right after it, before any other imports. Then press play.

OK FULL OUT{PUT IN CHAT
6:01 PM

Here's the full file — copy everything below into your scratch, replacing what's there:
python

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



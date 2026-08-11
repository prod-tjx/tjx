"""
Validator v3 — points at TFA-Trader-Hunting-Automation2
Press play.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from ai.snowflake.compat.connection import sf


# ============================================================================
REPO_ROOT  = Path("/Users/trevorjosephallen/PycharmProjects/TFA-Trader-Hunting-Automation2")
ACTIVE_DIR = REPO_ROOT / "active_queries"
MODE       = "pull"   # "check" | "pull" | "interactive"
# ============================================================================

PATTERNS_TABLE = "gbi_fraud_bap_db.ai_live_biz_app.TRADER_HUNTING_PATTERNS"
VIEW_SCHEMA    = "gbi_fraud_bap_db.ai_live_biz_app"


def load_active_patterns():
    q = f"""
        SELECT TABLE_NAME, CREATED_BY, CREATED_TS, QUERY_TYPE, ACTIVITY
        FROM {PATTERNS_TABLE}
        WHERE ACTIVITY = 'Active'
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


def normalize(ddl):
    if not ddl:
        return ""
    ddl = re.sub(r"--[^\n]*", "", ddl)
    ddl = re.sub(r"/\*.*?\*/", "", ddl, flags=re.DOTALL)
    return re.sub(r"\s+", " ", ddl.strip().upper()).rstrip(";").strip()


def read_repo_ddl(path):
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def format_file(table_name, ddl):
    header = (
        f"-- Pattern:    {table_name}\n"
        f"-- Source:     Snowflake ({VIEW_SCHEMA})\n"
        f"-- Auto-generated from Snowflake get_ddl — do not edit by hand.\n\n"
    )
    return header + ddl.strip().rstrip(";") + ";\n"


def repo_name_for(table_name, existing_files):
    target = f"{table_name}.sql".lower()
    for f in existing_files:
        if f.lower() == target:
            return f
    return f"{table_name}.sql"


def run():
    if not ACTIVE_DIR.exists():
        print(f"❌ {ACTIVE_DIR} does not exist. Check REPO_ROOT.")
        return

    print(f"Mode: {MODE}")
    print(f"Repo: {ACTIVE_DIR}\n")

    print("Loading active patterns from Snowflake...")
    sf_df = load_active_patterns()
    sf_names = set(sf_df["TABLE_NAME"])
    print(f"  → {len(sf_names)} active patterns in Snowflake")

    repo_files = {f.name for f in ACTIVE_DIR.glob("*.sql")}
    repo_stems = {f[:-4] for f in repo_files}
    print(f"  → {len(repo_files)} .sql files in repo")

    sf_lower   = {n.lower(): n for n in sf_names}
    repo_lower = {n.lower(): n for n in repo_stems}

    only_in_sf   = [sf_lower[k]   for k in sf_lower   if k not in repo_lower]
    only_in_repo = [repo_lower[k] for k in repo_lower if k not in sf_lower]
    in_both      = [sf_lower[k]   for k in sf_lower   if k in repo_lower]

    drifted = []
    matched = []
    print(f"\nComparing {len(in_both)} patterns present in both...")
    for name in in_both:
        sf_ddl = get_ddl(name)
        if not sf_ddl:
            continue
        repo_path = ACTIVE_DIR / repo_name_for(name, repo_files)
        repo_ddl = read_repo_ddl(repo_path)
        if normalize(sf_ddl) == normalize(repo_ddl):
            matched.append(name)
        else:
            drifted.append((name, sf_ddl, repo_path))

    print("\n" + "=" * 70)
    print("VALIDATION REPORT")
    print("=" * 70)
    print(f"✅ Identical:           {len(matched)}")
    print(f"⚠️  Drifted (DDL diff): {len(drifted)}")
    print(f"➕ Only in Snowflake:   {len(only_in_sf)}  (missing from repo)")
    print(f"➖ Only in repo:        {len(only_in_repo)}  (not active in Snowflake)")

    if only_in_sf:
        print("\n➕ Missing from repo:")
        for n in sorted(only_in_sf):
            print(f"     + {n}")

    if only_in_repo:
        print("\n➖ Not active in Snowflake (stale in repo?):")
        for n in sorted(only_in_repo):
            print(f"     - {n}")

    if drifted:
        print("\n⚠️  Drift detected:")
        for name, _, path in drifted:
            print(f"     ~ {name}  (repo: {path.name})")

    if MODE == "check":
        print("\nRead-only mode. Re-run with MODE='pull' or 'interactive' to apply changes.")
        return

    if MODE == "pull":
        print("\n→ Pulling Snowflake state into repo...")
        for name in only_in_sf:
            ddl = get_ddl(name)
            if ddl:
                (ACTIVE_DIR / f"{name}.sql").write_text(format_file(name, ddl), encoding="utf-8")
                print(f"     + wrote {name}.sql")
        for name, sf_ddl, path in drifted:
            path.write_text(format_file(name, sf_ddl), encoding="utf-8")
            print(f"     ~ updated {path.name}")
        for name in only_in_repo:
            fname = repo_name_for(name, repo_files)
            (ACTIVE_DIR / fname).unlink()
            print(f"     - deleted {fname}")
        print("\n✅ Repo now mirrors Snowflake. Run git status to review.")
        return

    if MODE == "interactive":
        for name in only_in_sf:
            if input(f"\n➕ Add {name} to repo? [y/N] ").strip().lower() == "y":
                ddl = get_ddl(name)
                if ddl:
                    (ACTIVE_DIR / f"{name}.sql").write_text(format_file(name, ddl), encoding="utf-8")
                    print(f"     wrote {name}.sql")
        for name, sf_ddl, path in drifted:
            if input(f"\n⚠️  Update {path.name} from Snowflake? [y/N] ").strip().lower() == "y":
                path.write_text(format_file(name, sf_ddl), encoding="utf-8")
                print(f"     updated {path.name}")
        for name in only_in_repo:
            if input(f"\n➖ Delete {name}.sql from repo? [y/N] ").strip().lower() == "y":
                fname = repo_name_for(name, repo_files)
                (ACTIVE_DIR / fname).unlink()
                print(f"     deleted {fname}")
        print("\n✅ Done.")


run()
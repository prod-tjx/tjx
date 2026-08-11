"""
Create only the 5 pattern files missing from the repo by pulling their
DDLs from Snowflake. Does not touch drifted files or stale files.

Just press play.
"""
from __future__ import annotations

from pathlib import Path
from ai.snowflake.compat.connection import sf


# ============================================================================
REPO_ROOT  = Path("/Users/trevorjosephallen/PycharmProjects/TFA-Trader-Hunting-Automation2")
ACTIVE_DIR = REPO_ROOT / "active_queries"

PATTERNS_TO_ADD = [
    "AMR_gibberish_em_a1add_velocity",
    "TRADER_5600_BINS_370021_373778",
    "TRADER_5600_BIZ_GOV_457079",
    "TRADER_FREIGHT_FORWARDER",
    "trader_apid_scriptor_low_match",
]
# ============================================================================

VIEW_SCHEMA = "gbi_fraud_bap_db.ai_live_biz_app"


def get_ddl(table_name):
    q = f"select get_ddl('view', '{VIEW_SCHEMA}.{table_name}') as query"
    try:
        df = sf.run_query(q)
        df.columns = [c.upper() for c in df.columns]
        return df["QUERY"].iloc[0]
    except Exception as e:
        print(f"    ! DDL fetch failed for {table_name}: {e}")
        return None


def format_file(table_name, ddl):
    header = (
        f"-- Pattern:    {table_name}\n"
        f"-- Source:     Snowflake ({VIEW_SCHEMA})\n"
        f"-- Auto-generated from Snowflake get_ddl — do not edit by hand.\n\n"
    )
    return header + ddl.strip().rstrip(";") + ";\n"


def run():
    if not ACTIVE_DIR.exists():
        print(f"❌ {ACTIVE_DIR} does not exist.")
        return

    print(f"Target: {ACTIVE_DIR}")
    print(f"Creating {len(PATTERNS_TO_ADD)} new pattern files\n")
    print("─" * 70)

    written = []
    for i, name in enumerate(PATTERNS_TO_ADD, 1):
        print(f"  [{i}/{len(PATTERNS_TO_ADD)}] {name:<50} ", end="", flush=True)
        ddl = get_ddl(name)
        if not ddl:
            print("FAILED")
            continue
        path = ACTIVE_DIR / f"{name}.sql"
        path.write_text(format_file(name, ddl), encoding="utf-8")
        written.append(path.name)
        print(f"✓ ({len(ddl):,} chars)")

    print("─" * 70)
    print(f"\n✅ Wrote {len(written)} files:")
    for n in written:
        print(f"     {n}")
    print("\nNext: run the shell script to commit and push.")


run()

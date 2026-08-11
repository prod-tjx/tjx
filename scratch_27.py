# scratch_get_pattern_sql.py - Pull the DDL for AMR_gibberish pattern
from ai.snowflake.compat.connection import sf

query = "SELECT get_ddl('view', 'gbi_fraud_bap_db.ai_live_biz_app.AMR_gibberish_em_a1add_velocity') AS query"
df = sf.run_query(query)
print(df['query'][0])
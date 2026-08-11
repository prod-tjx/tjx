import sys
import os
import argparse

from ai.snowflake.compat.connection import sf as sf
from ai.snowflake.compat.connection import exceptions


def get_params(sql):
    i = j = 0
    params = []
    while True:
        i = sql.find('{', i + 1)
        if i == -1:
            break
        else:
            j = sql.find('}', i)
            params.append(sql[i + 1:j])

    return list(set(params))


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', dest='sql')
    args = parser.parse_args()

    jenkins_params = dict(os.environ)

    with open("./src/ai/fulfillment/reporting/sql/" + args.sql, 'r') as f:
        sql_query = f.read()

    params = get_params(sql_query)
    sql_params = {param: jenkins_params[param] for param in params}

    sql_list = sql_query.split(";")  # ddl can only only be submitted in once per transaction

    for query in sql_list:
        if query.strip() == "":
            continue

        print(query)
        try:
            df = sf.run_query(query.format(**sql_params))
        except exceptions.ObjectNotExistsError:
            if "DROP TABLE" in query.upper():
                continue
    mr_message()

# create msg
def mr_message():
    mr_table = sf.run_query("""
        select * from gbi_fraud_bap_db.ai_live_biz_app.TRADER_TFA_TEST_REPLAY_IND_SSLA;
     """)

    total_count = len(mr_table)

    if total_count == 0:
        message_string = f"""Total Orders: {total_count}"""

    else:
        output_col = mr_table['order_type']
        alert_col = mr_table['past_due_alert']
        replay_col = mr_table['replay_ind']                                                # NEW

        #apu counts and breakdown
        count_apu = ((output_col.isin(['IPK', 'IDL']))).sum()
        count_within_apu = ((output_col.isin(['IPK', 'IDL'])) & (alert_col == 'SSLA')).sum()
        count_at_risk_apu = ((output_col.isin(['IPK', 'IDL'])) & (alert_col == 'At Risk')).sum()
        count_past_due_apu = ((output_col.isin(['IPK', 'IDL'])) & (alert_col == 'Past Due')).sum()
        count_due_today_apu = ((output_col.isin(['IPK', 'IDL'])) & (alert_col == 'Due Today')).sum()
        count_replay_apu = ((output_col.isin(['IPK', 'IDL'])) & (replay_col == 'Y')).sum() # NEW

        #non-apu counts and breakdown
        count_non_apu = ((~output_col.isin(['IPK', 'IDL']))).sum()
        count_at_risk = ((~output_col.isin(['IPK', 'IDL'])) & (alert_col == 'At Risk')).sum()
        count_past_due = ((~output_col.isin(['IPK', 'IDL'])) & (alert_col == 'Past Due')).sum()
        count_eod = ((~output_col.isin(['IPK', 'IDL'])) & (alert_col == 'EOD Sweep')).sum()
        count_due_today = ((~output_col.isin(['IPK', 'IDL'])) & (alert_col == 'Due Today')).sum()
        count_replay = ((~output_col.isin(['IPK', 'IDL'])) & (replay_col == 'Y')).sum()   # NEW

        #Notification message
        message_string = f"""Total Orders: {total_count}
IPK/IDL: {count_apu}  (Within SLA: {count_within_apu}  At Risk: {count_at_risk_apu}  Due Today: {count_due_today_apu}  Past Due: {count_past_due_apu}  Replay: {count_replay_apu})
Others:  {count_non_apu}  (At Risk: {count_at_risk}  Due Today: {count_due_today}  Past Due: {count_past_due}  EOD Sweep: {count_eod}  Replay: {count_replay})
Select * from gbi_fraud_bap_db.ai_live_biz_app.TRADER_TFA_TEST_REPLAY_IND_SSLA
          """

    print(message_string)
    text_file = open("alert_message_mr.txt", "w")
    text_file.write(f'{message_string}')
    text_file.close()


if __name__ == '__main__':
    sys.exit(run())
"""Please find directions in the data_generation_readme this is a sample file to begin
building your queries.  This file contains an example of 1 fraud rate query and 1 final query to get you started"""
from ai.snowflake.compat.tabletools.table import SimpleTable
import ai.datetools as dt


def create_email_fraud_rate_table(order_date):
    query = f""" SELECT '{order_date}'            AS projection_date
    , o.IP_ADDR_TXT                        AS IP_address_txt
    , COUNT(f.order_id)                           AS trader_90_days
    , COUNT(CASE WHEN f.order_id IS NULL
    THEN o.order_id
    END)                             AS good_90_days
    , COUNT(*)                                    AS total_30_days
    , 100.0 * fraud_90_days / total_30_days       AS fraud_rate_30_days
    FROM gbi_fraud_semantic_db.ai.order_summary o
    LEFT JOIN (select * from gbi_fraud_semantic_db.ai.fraud_order 
            WHERE etl_create_ts < CAST('{order_date}' AS TIMESTAMP)) f
    ON f.order_id = o.order_id
    WHERE 1 = 1
    AND o.order_dt BETWEEN CAST('{order_date}' AS DATE) - 90 AND CAST('{order_date}' AS DATE)
    AND o.IP_ADDR_TXT IS NOT NULL
    GROUP BY o.IP_ADDR_TXT """
    volatile_table = SimpleTable(short_table_name='ip_addr_txt',
                                 contents_sql=query,
                                 table_schema='gbi_fraud_bap_db.ai_live_biz_app',
                                 indices=['ip_address_txt'],
                                 volatile=True)
    volatile_table.update()


def create_final_bt_table(start_date, end_date):
    query = f"""SELECT
                  kafka.data_weborder                                    AS won
                , kafka.event_dt
                , ROUND(CAST(kafka.output_fraud_score_adj AS DECIMAL(10, 4)), 2) AS fraud_score_adj
                , ROUND(CAST(kafka.output_model_threshold AS DECIMAL(10, 4)), 2) AS model_threshold
                , fraud_score_adj - model_threshold                              AS distance_from_threshold
                , CAST(kafka.output_amt AS BIGINT) / 100.00                      AS order_value_usd
                , bem.fraud_rate_30_days      AS b2_domain_fr
                FROM ( SELECT *
                    FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order
                   WHERE event_dt between '{start_date}' and '{end_date}'
                 QUALIFY ROW_NUMBER() OVER (PARTITION BY data_weborder ORDER BY event_ts DESC NULLS LAST) = 1 ) kafka
                LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.email_fraud_rate_bt bem
                 ON bem.email_domain_name = SUBSTRING(kafka.data_billtoemail, POSITION('@' IN (kafka.data_billtoemail)), LENGTH(kafka.data_billtoemail))
                 AND kafka.event_dt = bem.projection_date
               WHERE 1 = 1
                AND kafka.data_salesorg IN ('5600', '3400')
                -- note fraud rate filters here have been removed and the raw fraud rate has been added to the select statement         
    """
    final_table = SimpleTable(short_table_name='meow_query_100_backtesting',
                            contents_sql=query,
                            table_schema='gbi_fraud_bap_db.ai_live_biz_app',
                            indices=['won'])
    final_table.update(new=True)


def main():
    # for testing purposes, run a quick 2 day range to make sure your code runs without having to wait too long
    start_date = dt.date('2024-05-01')
    end_date = dt.date('2024-05-02')

    # to build the whole table from current_date - 180 to current_date - 90
    # start_date = dt.date() - 180
    # end_date = dt.date() - 90
    date_range = start_date ^ end_date

    print(f"Creating backtesting data from {start_date} to {end_date}")

    # add any fraud rate queries to the for loop below
    for d in date_range:
        print(f"Creating historical fraud rates for {d}")
        create_email_fraud_rate_table(d)

    # outside the for loop, we create the final table
    create_final_bt_table(start_date, end_date)


if __name__ == '__main__':
    main()
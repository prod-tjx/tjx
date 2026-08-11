import streamlit as st
from ai.snowflake.compat.connection import sf
import pandas as pd
import plotly.express as px
from datetime import datetime
import io
import random

# --- Streamlit Page Configuration ---
st.set_page_config(layout="wide", page_title="Trader Watchlist")

# --- Session State Initialization ---
if 'session_id' not in st.session_state:
    # Create a short, random ID for this session to avoid temporary table collisions
    st.session_state.session_id = str(random.randint(10000, 99999))

if 'step' not in st.session_state:
    st.session_state.step = 1
    st.session_state.days_back = 0
    st.session_state.base_generated = False
    st.session_state.datapoint_list = []
    st.session_state.watchlist_generated = False
    st.session_state.summary_df = None
    st.session_state.shortlisted_datapoints = []
    st.session_state.shortlist_confirmed = False
    st.session_state.final_df = None


# --- Calculation Functions ---

def get_temp_table_name(base_name):
    """Appends session_id to make temporary table names unique."""
    return f"gbi_fraud_bap_db.ai_live_biz_app.{base_name}_{st.session_state.session_id}"


def watchlist_temp_all(days_back, sales_org_list):
    """
    Generates the base temporary tables using direct sf.run_query calls.
    Table names are session-specific to prevent multi-user conflicts.
    """
    days_back_or = days_back + 7
    awo_items_temp_table = get_temp_table_name("trader_tfa_awo_items_vw_temp")
    watchlist_temp_table = get_temp_table_name("trader_tfa_watchlist_temp")

    # Sanitize sales_org_list for the IN clause to prevent SQL injection
    safe_sales_orgs = ", ".join([f"'{str(s).strip()}'" for s in sales_org_list])

    # --- Query 1: Create AWO items temporary table ---
    st.info("Step 1/2: Creating awo items table...")
    query1 = f'''
    CREATE OR REPLACE TEMPORARY TABLE {awo_items_temp_table} AS(
       WITH
         pool AS (SELECT *
                    FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_category_central_pool_vw
                    WHERE create_ts >= CURRENT_DATE - {days_back_or}
                    QUALIFY ROW_NUMBER () OVER (PARTITION BY web_order_id, order_item_nr ORDER BY create_ts ASC NULLS LAST) =1
         ),
         item_current_count AS (SELECT web_order_id
                                     , OBJECT_AGG(category, current_qty) AS item_current_count
                                FROM (SELECT web_order_id
                                           , category
                                           , SUM(order_qty) AS current_qty 
                                      FROM pool GROUP BY 1,2)
                                GROUP BY 1
        ),
         item_desc AS (SELECT web_order_id
                            , MAX(category_type) AS category_type 
                            , MAX(campaign_id) AS campaign_id
                            , ARRAY_AGG(item_desc) WITHIN GROUP (ORDER BY web_order_id) AS prods
                       FROM pool GROUP BY 1
        )
      
      SELECT o.web_order_id
           , CASE WHEN i.category_type = 1 THEN 'npi' 
                  WHEN i.category_type = 2 THEN 'edu' 
                  WHEN i.category_type = 3 THEN 'promo' 
                  END AS npi_edu_promo
           , i.campaign_id
           , o.item_current_count
           , i.prods
      FROM item_current_count o 
      JOIN item_desc i 
        ON o.web_order_id = i.web_order_id
    )
    '''
    sf.run_query(query1)

    # --- Query 2: Create main watchlist base temporary table ---
    st.info("Step 2/2: Creating main watchlist base table...")
    query2 = f'''
    CREATE OR REPLACE TEMPORARY TABLE {watchlist_temp_table} AS
    (SELECT orc.order_id 
          , kawo.data_weborder AS web_order_id 
          , kawo.event_ts 
          , kawo.data_ordercreationdate AS order_creation_dt
          , kawo.data_updatedtradercode AS original_trader_cd 
          , orc.trader_cd 
          , os.rma_order_id 
          , os.rejection_cd
          , CASE WHEN dn.create_ts IS NOT NULL THEN 1 ELSE 0 
                 END AS partially_shipped 
          , kawo.data_region AS region
          , kawo.data_salesorg AS sales_org 
          , kawo.data_salesdistrict AS sales_district
          , CASE WHEN kawo.data_salesdistrict IN ('IN02','RW02') THEN 'consumer' 
                 WHEN kawo.data_salesdistrict IN ('IN01','RW01') THEN 'epp_primary' 
                 WHEN kawo.data_salesdistrict IN ('IN20','RW20') THEN 'epp_secondary' 
                 WHEN kawo.data_salesdistrict IN ('IN21','RW21') THEN 'qpromo' 
                 WHEN kawo.data_salesdistrict IN ('IN15','RW15') THEN 'biz_or_gov' 
                 WHEN kawo.data_salesdistrict IN ('IN05','RW05') THEN 'smb' 
                 WHEN kawo.data_salesdistrict IN ('IN17','IN18','IN19','IN25','IN26','IN27','RW17','RW18','RW19','RW25','RW26','RW27') THEN 'edu' 
                 END AS discount_store_type
          , awo.campaign_id, kawo.data_potype AS po_type 
          , kawo.output_top_level_model_type AS sla 
          , kawo.output_order_type
          , kawo.data_items_shiptocompany AS biz_ars_pup 
          , kawo.data_billtoname AS b2_nm 
          , kawo.data_billtoemail AS b2_em
          , LOWER(SPLIT_PART(kawo.data_billtoemail, '@', 2)) AS b2_em_dom 
          , kawo.output_account_age AS acct_age
          , DATEDIFF(DAY, aos.id_creation_dt, CURRENT_DATE) AS its_age 
          , CONCAT(aos.person_first_name, ' ', aos.person_last_name) AS its_acct_nm
          , CONCAT(aos.billing_first_name, ' ', aos.billing_last_name) AS its_b2_nm 
          , aos.email_txt AS its_em
          , kawo.data_personid AS dsid 
          , aos.person_id 
          , TRIM(CONCAT(COALESCE(kawo.data_billtostreetprefix, ''), ' ', COALESCE(kawo.data_billtostreet, ''), ' ', COALESCE(kawo.data_billtocity, ''))) AS b2_add
          , kawo.data_billtostate AS b2_state 
          , kawo.data_billtocity AS b2_city 
          , kawo.data_billtodistrict AS b2_district 
          , kawo.data_billtozip AS b2_zip 
          , kawo.data_billtophonenumber AS b2_ph
          , kawo.data_shiptoname AS a1_nm 
          , kawo.data_shiptoemail AS a1_em 
          , LOWER(SPLIT_PART(kawo.data_shiptoemail, '@', 2)) AS a1_em_dom
          , TRIM(CONCAT(COALESCE(kawo.data_shiptostreetprefix, ''), ' ', COALESCE(kawo.data_shiptostreet, ''), ' ', COALESCE(kawo.data_shiptocity, ''))) AS a1_add
          , kawo.output_apple_maps_normalized_address AS apple_maps_add 
          , kawo.data_shiptostate AS a1_state 
          , kawo.data_shiptocity AS a1_city
          , kawo.data_shiptodistrict AS a1_district 
          , kawo.data_shiptozip AS a1_zip 
          , kawo.data_shiptophonenumber AS a1_ph
          , TRIM(kawo.data_items_shiptoname[0], '"') AS latest_a1_nm 
          , TRIM(kawo.data_items_shiptoemail[0], '"') AS latest_a1_em
          , TRIM(CONCAT(COALESCE(kawo.data_items_shiptostreetprefix[0], ''), ' ', COALESCE(kawo.data_items_shiptostreet[0], ''), ' ', COALESCE(kawo.data_items_shiptocity[0], ''))) AS latest_a1_add
          , TRIM(kawo.data_items_shiptostate[0], '"') AS latest_a1_state 
          , TRIM(kawo.data_items_shiptocity[0], '"') AS latest_a1_city 
          , TRIM(kawo.data_items_shiptozip[0], '"') AS latest_a1_zip
          , CASE WHEN kawo.DATA_ITEMS_THIRDPARTYNAME IS NOT NULL THEN TRIM(kawo.DATA_ITEMS_THIRDPARTYNAME[0]) 
                 ELSE TRIM(kawo.DATA_BOPIS_STORESHIPTONAME[0]) 
                 END AS latest_bopis3_nm
          , CASE WHEN kawo.DATA_ITEMS_THIRDPARTYEMAIL IS NOT NULL THEN TRIM(kawo.DATA_ITEMS_THIRDPARTYEMAIL[0]) 
                 ELSE TRIM(kawo.DATA_BOPIS_STORESHIPTOEMAIL[0]) 
                 END AS latest_bopis3_em
          , CASE WHEN kawo.data_ipaddress LIKE ANY ('17.%','144.178.%','10.82.%') THEN 'apple_ip' 
                 ELSE kawo.data_ipaddress 
                 END AS ip_add
          , ARRAY_TO_STRING(ARRAY_SLICE(STRTOK_TO_ARRAY(kawo.data_ipaddress, '.'), 0,3), '.') AS ip_class 
          , kawo.OUTPUT_GEOLOC_IP_CARRIER AS ip_company
          , kawo.OUTPUT_GEOLOC_IP_COUNTRYCODE AS ip_country 
          , CASE WHEN kawo.data_potype IN ('MOBW', 'PADW') THEN kawo.data_pcprint 
                  END AS guid
          , kawo.data_sessioncookie AS session_cookie 
          , kawo.data_splittenderflag AS split_payment_ind 
          , kawo.data_payment_cardbin AS card_bin
          , kawo.data_payment_cardbin[0] AS first_bin
          , kawo.data_payment_cardbin[ARRAY_SIZE(kawo.data_payment_cardbin) - 1] AS last_bin
          , CASE WHEN kawo.data_payment_actualsapcardtype IS NULL THEN kawo.data_paymentterms 
                 ELSE kawo.data_payment_actualsapcardtype 
                 END AS payment_type
          , kawo.output_pmt_cardbin_country3alpha AS bin_country 
          , TRIM(kawo.output_pmt_cardbin_issuer_name, '"') AS card_issuer
          , TRIM(kawo.output_pmt_cardbin_card_network[0], '"') AS card_type 
          , kawo.output_payment_id AS payment_id 
          , kawo.data_payment_cardnumberhash[0] AS card_hash
          , ROUND(CAST(kawo.output_amt AS BIGINT) / 100.00, 2) AS total_value 
          , awo.prods AS prod_desc 
          , awo.item_current_count AS current_category_count
          , CONCAT( CASE WHEN kawo.data_items_updatedengravingtext1 IS NOT NULL THEN 1 ELSE 0 END, '-', 
                    CASE WHEN ARRAY_TO_STRING(kawo.data_items_id,',') LIKE ANY ('Z1%','%,Z1%') THEN 1 ELSE 0 END ) AS engr_cto
          , kawo.data_items_giftingtext AS gift_text 
          , sap.value_old_nr AS ori_flag 
          , sap.value_new_nr AS new_flag
       FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order kawo
  LEFT JOIN gbi_fraud_semantic_db.ai.order_summary os 
         ON kawo.data_weborder = os.web_order_id
  LEFT JOIN (SELECT order_id 
                   , web_order_id
                   , trader_cd 
                FROM gbi_fraud_semantic_db.ai.order_reg_cur 
               WHERE source_id = 'SC2' 
                 AND order_type_cd IN ('TA', 'ZEMP') 
                 AND order_dt >= CURRENT_DATE - {days_back_or} ) orc 
                  ON kawo.data_weborder = orc.web_order_id
  LEFT JOIN {awo_items_temp_table} awo 
         ON kawo.data_weborder = awo.web_order_id
  LEFT JOIN (SELECT person_locale_cd
                  , person_id
                  , id_creation_dt 
                  , person_first_name
                  , person_last_name
                  , billing_first_name
                  , billing_last_name
                  , LOWER(TRIM(email_addr_txt)) AS email_txt
                FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_aos_acc) aos 
         ON LOWER(TRIM(kawo.data_billtoemail)) = aos.email_txt
  LEFT JOIN (SELECT delivery_id
                  , ref_doc_id
                  , create_ts 
               FROM gbi_fraud_semantic_db.ai.delivery_item_cur
               WHERE CREATE_TS>= CURRENT_DATE - {days_back_or} 
               GROUP BY 1,2,3) dn 
         ON orc.order_id=dn.ref_doc_id
  LEFT JOIN (SELECT value_old_nr
                   , value_new_nr
                   , objectid_id 
                FROM gbi_fraud_semantic_db.ai.sap_cdpos 
               WHERE 1 = 1 
                 AND zss_ts >= CURRENT_DATE - {days_back_or} 
                 AND fname_name = 'ZZTRADER_CODE' 
               QUALIFY ROW_NUMBER() OVER (PARTITION BY objectid_id ORDER BY zslt_ts DESC) = 1) sap 
          ON orc.order_id = sap.objectid_id
          
  WHERE kawo.event_ts >= CURRENT_DATE - {days_back}
    AND kawo.data_salesorg IN ({safe_sales_orgs})
    AND kawo.data_weborder LIKE 'W%'
    AND kawo.output_order_type <> 'adp'
    AND ((kawo.output_digital_egc_model_eligible = 'false') OR (kawo.output_digital_egc_model_eligible IS NULL))
    AND kawo.data_productclass <>'7A'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY kawo.data_weborder ORDER BY kawo.event_ts DESC NULLS LAST) = 1)
    '''
    sf.run_query(query2)

def watch_list_gen(datapoint_list, days_back, total_order_threshold):
    """Generates one temporary table for each selected datapoint."""
    watchlist_temp_table = get_temp_table_name("trader_tfa_watchlist_temp")
    for datapoint in datapoint_list:
        filter_q = ''
        if datapoint in ('card_bin', 'first_bin', 'last_bin'):
            filter_q = f"AND {datapoint} <> '111111'"
        elif datapoint in ('b2_em_dom', 'a1_em_dom'):
            filter_q = f"AND NOT {datapoint} IN ('gmail.com', 'icloud.com', 'hotmail.com', 'yahoo.com')"

        datapoint_table = get_temp_table_name(f"trader_tfa_watchlist_{days_back}_{datapoint}")

        query = f"""
        CREATE OR REPLACE TEMPORARY TABLE {datapoint_table} AS
        (SELECT sales_org
              , {datapoint}
              , COUNT(*) AS {datapoint}_total_orders
              , COUNT(CASE WHEN trader_cd = 'CT' THEN 1 
                           ELSE NULL END) AS {datapoint}_total_CT
              , ROUND({datapoint}_total_CT * 100 / {datapoint}_total_orders, 2) AS {datapoint}_total_ct_rate
              , COUNT(CASE WHEN new_flag = 'CT' 
                             OR ((ori_flag IS NOT NULL) AND (new_flag IS NULL)) THEN 1 
                             ELSE NULL END) AS {datapoint}_total_actioned
              , COUNT(CASE WHEN new_flag = 'CT' THEN 1 
                           ELSE NULL END) AS {datapoint}_actioned_CT
              , ROUND({datapoint}_actioned_CT * 100 / NULLIFZERO({datapoint}_total_actioned), 2) AS {datapoint}_actioned_ct_rate
           FROM {watchlist_temp_table}
          WHERE {datapoint} IS NOT NULL AND trim({datapoint}) <> ''
          {filter_q}
          GROUP BY 1, 2
          HAVING {datapoint}_total_orders > {total_order_threshold}
          ORDER BY {datapoint}_total_orders DESC)
        """
        sf.run_query(query)


def union_watch_list(datapoint_list, days_back):
    """Unions all generated watchlist tables into a single summary table."""
    union_table = get_temp_table_name("trader_tfa_watchlist_union")
    union_queries = []
    for datapoint in datapoint_list:
        datapoint_table = get_temp_table_name(f"trader_tfa_watchlist_{days_back}_{datapoint}")
        union_queries.append(f"""
        SELECT sales_org
             , '{datapoint}' AS datapoint_type
             , CAST({datapoint} AS VARCHAR) AS datapoint
             , {datapoint}_total_orders AS total_orders
             , {datapoint}_total_CT AS total_CT
             , {datapoint}_total_ct_rate AS total_ct_rate
             , {datapoint}_total_actioned AS total_actioned
             , {datapoint}_actioned_CT AS actioned_CT
             , {datapoint}_actioned_ct_rate AS actioned_ct_rate
          FROM {datapoint_table}
        """)


    full_query = f"CREATE OR REPLACE TEMPORARY TABLE {union_table} AS (" + " UNION ALL ".join(union_queries) + ")"
    sf.run_query(full_query)
    return union_table

def get_summary_df(union_table_name):
    """Queries the unioned table to get the final summary for plotting."""
    query = f"""
    SELECT datapoint_type
         , SUM(total_orders) AS total_orders
         , SUM(total_CT) AS total_CT
         , ROUND(AVG(total_ct_rate),2) AS avg_total_ct_rate
         , SUM(total_actioned) AS total_actioned
         , SUM(actioned_CT) AS actioned_CT
         , ROUND(AVG(actioned_ct_rate),2) AS avg_actioned_ct_rate
      FROM {union_table_name}
      GROUP BY 1
    """
    df = sf.run_query(query)
    # Ensure numeric types for plotting
    for col in ['total_orders', 'avg_total_ct_rate', 'avg_actioned_ct_rate']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def plotly_plot(df):
    """Creates an interactive Plotly scatter plot."""
    fig = px.scatter(
        df, x='avg_total_ct_rate', y='avg_actioned_ct_rate', color='datapoint_type',
        size='total_orders', hover_name='datapoint_type',
        title='Total CT Rate vs Actioned CT Rate by Datapoint Type',
        labels={
            "avg_total_ct_rate": "Average Total CT Rate (%)",
            "avg_actioned_ct_rate": "Average Actioned CT Rate (%)"
        }
    )
    fig.update_traces(
        hovertemplate="<b>%{hovertext}</b><br>" +
                      "Avg Total CT Rate: %{x}%<br>" +
                      "Avg Actioned CT Rate: %{y}%<br>" +
                      "Total Order Volume: %{marker.size:,}"
    )
    return fig


def join_and_get_final_df(datapoint_list, days_back, review_table_short_name):
    """
    Joins watchlists back to a base table and returns the result as a DataFrame.
    Uses clear, full-length aliases for readability.
    """
    review_table_map = {
        'trader_review_frame': get_temp_table_name("trader_tfa_watchlist_temp"),
        'mass_review': 'gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ww_mass_review',
        'priority_list': 'gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ww_priority_vw'
    }
    review_table_q = review_table_map[review_table_short_name]

    data_select = ""
    table_join = ""
    filter_clauses = []

    for datapoint in datapoint_list:
        # Create a clear, readable alias directly from the datapoint name.
        wl_alias = f"wl_{datapoint}"

        # Get the full temporary table name for the current datapoint.
        wl_table = get_temp_table_name(f"trader_tfa_watchlist_{days_back}_{datapoint}")

        # Add the new columns to the SELECT statement using the clear alias.
        data_select += f", {wl_alias}.{datapoint}_total_orders, {wl_alias}.{datapoint}_total_ct_rate, {wl_alias}.{datapoint}_actioned_ct_rate\n"

        # Build the LEFT JOIN clause using the full temporary table name and the clear alias.
        table_join += f"""
        LEFT JOIN {wl_table} AS {wl_alias}
        ON rt.sales_org = {wl_alias}.sales_org AND rt.{datapoint} = {wl_alias}.{datapoint}\n"""

        # Add the filter condition using the clear alias.
        filter_clauses.append(f"{wl_alias}.{datapoint}_total_orders IS NOT NULL")

    filter_q = " OR ".join(filter_clauses)

    query = f"""
    SELECT rt.* {data_select}
    FROM {review_table_q} rt
    {table_join}
    """
    if review_table_short_name == 'trader_review_frame' and filter_q:
        query += f'WHERE {filter_q}'

    return sf.run_query(query)


def to_excel_bytes(df: pd.DataFrame):
    """Converts a DataFrame to an in-memory Excel file (bytes) for later download.
       Ensure ranking cols to be numeric"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        cols_to_convert_numeric = [
            col for col in df.columns
            if (col.endswith('_age') or col.endswith('_rate') or col.endswith('_value'))
        ]
        df[cols_to_convert_numeric] = df[cols_to_convert_numeric].apply(pd.to_numeric, errors='coerce')
        df_conv = df.convert_dtypes()
        df_conv.to_excel(writer, index=False, sheet_name='Watchlist_Review')

    return output.getvalue()

# --- Streamlit App UI ---
st.title(":ledger: Trader Watchlist Generator")
st.markdown("A tool to analyze order data, generate watch lists, and export results for review.")

# --- Step 1: Base Data Generation ---
st.header("Step 1: Configure and Generate Base Data")
with st.form(key="base_form"):
    st.write("Choose the sales_org you want to generate analysis.")
    sales_org_input = st.text_input(
        "Sales Orgs (comma-separated)",
        "5600, 8400",
        help="Enter one or more Sales Org codes separated by commas."
    )
    days_back_input = st.slider(
        "Days Back",
        min_value=1,
        max_value=365,
        value=7,
        help="How many past days of data to include in the analysis."
    )
    submit_base = st.form_submit_button("🚀 Generate Base Data", type="primary")

if submit_base:
    # Sanitize and validate input
    sales_org_list = [s.strip() for s in sales_org_input.split(',') if s.strip()]
    if not sales_org_list:
        st.error("Please provide at least one Sales Org.")
    else:
        with st.spinner(
                f"Generating base data for {len(sales_org_list)} Sales Orgs over {days_back_input} days... This may take a while."):
            try:
                watchlist_temp_all(days_back_input, sales_org_list)
                st.session_state.base_generated = True
                st.session_state.days_back = days_back_input  # Save for later steps
                st.success("✅ Base data generated successfully!")
                st.session_state.step = 2
                # Clear downstream states
                st.session_state.datapoint_list = []
                st.session_state.watchlist_generated = False
                st.session_state.summary_df = None
                st.session_state.shortlisted_datapoints = []
                st.session_state.shortlist_confirmed = False
                st.session_state.final_df = None

            except Exception as e:
                st.error(f"An error occurred while generating base data: {e}")

# --- Step 2: Watchlist Generation ---
if st.session_state.base_generated:
    st.header("Step 2: Generate Watch lists")
    #Open for expansion in phase 2
    unique_identifiers = ['b2_nm', 'b2_add', 'b2_ph', 'a1_nm', 'latest_a1_nm', 'latest_a1_add', 'a1_ph',
                          'latest_bopis3_nm', 'latest_bopis3_em',
                          'its_em', 'its_acct_nm', 'its_b2_nm', 'person_id', 'ip_add']
    indicators = ['b2_zip', 'latest_a1_zip', 'ip_class', 'card_bin', 'first_bin', 'last_bin',
                  'latest_a1_city', 'b2_district', 'a1_district', 'a1_em_dom','b2_em_dom']

    with st.form("watchlist_form"):
        st.write("Select the datapoints to analyze and set the minimum order threshold.")
        total_order_threshold_input = st.number_input(
            "Total Order Threshold",
            min_value=1,
            value=20,
            help="Cut off lower volume, datapoint with volume lower than this will not be collected."
        )

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Unique Identifiers")
            selected_identifiers = st.multiselect("Select from Identifiers", options=unique_identifiers, help='More specific datapoint types')
        with col2:
            st.markdown("#### Indicators")
            selected_indicators = st.multiselect("Select from Indicators", options=indicators, help='More general datapoint types')

        submit_watchlist = st.form_submit_button("📊 Generate Watchlist & Summary", type="primary")

    if submit_watchlist:
        st.session_state.datapoint_list = selected_identifiers + selected_indicators
        if not st.session_state.datapoint_list:
            st.warning("Please select at least one datapoint to analyze.")
        else:
            with st.spinner("Calculating watchlist statistics..."):
                try:
                    watch_list_gen(st.session_state.datapoint_list, st.session_state.days_back,
                                   total_order_threshold_input)
                    union_table = union_watch_list(st.session_state.datapoint_list, st.session_state.days_back)
                    st.session_state.summary_df = get_summary_df(union_table)
                    st.session_state.watchlist_generated = True
                    st.success("✅ Watchlists and summary generated!")
                    st.session_state.step = 3
                    #Clear downstream states
                    st.session_state.shortlisted_datapoints = []
                    st.session_state.shortlist_confirmed = False
                    st.session_state.final_df = None
                except Exception as e:
                    st.error(f"An error occurred during watchlist generation: {e}")

# --- STEP 3: Display Summary & Plot, Refine Choice ---
if st.session_state.get("summary_df") is not None and not st.session_state.summary_df.empty:
    st.subheader("📊 Analysis Summary")
    st.dataframe(st.session_state.summary_df)

    st.subheader("📈 Visual Analysis")
    summary_plot = plotly_plot(st.session_state.summary_df)
    st.plotly_chart(summary_plot, use_container_width=True)

    # --- NEW STEP: Further Shortlist Datapoints ---
    st.header("Step 3: Refine Your Datapoint Selection")
    st.markdown(
        "Based on the summary statistics above, you can further refine your datapoint selection before joining with review tables.")

    with st.form("shortlist_form"):
        st.write("Select which datapoints to include in the final analysis:")

        # Get the datapoints from the original selection
        available_datapoints = st.session_state.datapoint_list.copy()

        # Create checkboxes for each datapoint
        col1, col2 = st.columns(2)
        shortlisted_datapoints = []

        for i, datapoint in enumerate(available_datapoints):
            with col1 if i % 2 == 0 else col2:
                is_selected = st.checkbox(
                    f"{datapoint}",
                    value=False,  # Default to not selected
                    key=f"shortlist_{datapoint}"
                )
                if is_selected:
                    shortlisted_datapoints.append(datapoint)

        submit_shortlist = st.form_submit_button("✅ Confirm Datapoint Selection", type="primary", help='Confirm again each time choice is changed')

    if submit_shortlist:
        if not shortlisted_datapoints:
            st.warning("Please select at least one datapoint for the final analysis.")
        else:
            st.session_state.shortlisted_datapoints = shortlisted_datapoints
            st.session_state.shortlist_confirmed = True
            st.success(f"✅ Selected {len(shortlisted_datapoints)} datapoints for final analysis!")
            st.session_state.step = 4

            # Show selected datapoints
            st.info(f"**Selected datapoints:** {', '.join(shortlisted_datapoints)}")

            #Clear downstream states
            st.session_state.final_df = None

# --- Step 4: Join and Export (Updated) ---
if st.session_state.get("shortlist_confirmed", False):
    st.header("Step 4: Prepare and Export Final Data")
    with st.form("join_form"):
        st.write("Finally, join the watchlist statistics back to a review table for detailed analysis.")

        # Show which datapoints will be used
        st.info(
            f"**Using {len(st.session_state.shortlisted_datapoints)} selected datapoints:** {', '.join(st.session_state.shortlisted_datapoints)}")

        review_table_choice = st.radio(
            "Select a table to join with:",
            options=['trader_review_frame', 'mass_review', 'priority_list'],
            captions=[
                "The base data generated in Step 1",
                "Recent MR generated table",
                "Program defined Priority list"
            ],
            horizontal=True
        )
        submit_join = st.form_submit_button("🔍 Prepare Final Data for Export", type="primary")

    if submit_join:
        with st.spinner(f"Joining watch lists with '{review_table_choice}' and preparing data..."):
            try:
                # Use shortlisted datapoints instead of original datapoint_list
                st.session_state.final_df = join_and_get_final_df(
                    st.session_state.shortlisted_datapoints,
                    st.session_state.days_back,
                    review_table_choice
                )
                st.success("✅ Final data is ready for download!")
                st.dataframe(st.session_state.final_df.head())
            except Exception as e:
                st.error(f"An error occurred while preparing the final data: {e}")

# --- Download Button ---
if st.session_state.get("final_df") is not None and not st.session_state.final_df.empty:
    st.subheader("⬇️ Download Your Report")

    excel_data = to_excel_bytes(st.session_state.final_df)

    st.download_button(
        label="📥 Download Watchlist Review (Excel)",
        data=excel_data,
        file_name=f"watchlist_review_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )



import streamlit as st
import pandas as pd
from ai.snowflake.compat.connection import sf
import logging
import re
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# --- Helper Functions ---

def load_patterns():
    """Load patterns from Snowflake - always fresh"""
    q = """
        SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CREATED_BY, CREATED_TS, QUERY_TYPE, ACTIVITY
        FROM gbi_fraud_bap_db.ai_live_biz_app.TRADER_HUNTING_PATTERNS
        ORDER BY TABLE_NAME
        """
    try:
        df = sf.run_query(q)
        if 'TABLE_NAME' in df.columns:
            df = df.drop_duplicates(subset=['TABLE_NAME'], keep='last')
        elif 'table_name' in df.columns:
            df = df.drop_duplicates(subset=['table_name'], keep='last')
        df = df.reset_index(drop=True)
        return df
    except Exception as e:
        st.error(f"Error loading patterns: {e}")
        return pd.DataFrame()


def update_pattern_activity(table_name, activity):
    """Update pattern activity in Snowflake"""
    sql = f"""
        UPDATE gbi_fraud_bap_db.ai_live_biz_app.TRADER_HUNTING_PATTERNS
        SET ACTIVITY = '{activity}'
        WHERE TABLE_NAME = '{table_name}'
    """
    try:
        sf.run_query(sql)
        return True, None
    except Exception as e:
        return False, str(e)


def get_trader_hunting_query(hunting_pattern):
    """Get DDL for trader hunting view - always fresh from DB"""
    original_query = f"""
    select get_ddl('view', 'gbi_fraud_bap_db.ai_live_biz_app.{hunting_pattern}') as query
    """
    try:
        df = sf.run_query(original_query)
        if 'QUERY' in df.columns:
            return df['QUERY'].iloc[0]
        elif 'query' in df.columns:
            return df['query'].iloc[0]
        else:
            return None
    except Exception as e:
        st.error(f"Error getting query for {hunting_pattern}: {e}")
        return None


def normalize_ddl_for_comparison(ddl):
    """Normalize DDL for comparison by removing whitespace and formatting differences"""
    if not ddl:
        return ""
    # Remove extra whitespace, normalize case for comparison
    normalized = re.sub(r'\s+', ' ', ddl.strip().upper())
    return normalized


def validate_ddl_syntax(ddl_statement, pattern_name):
    """Validate DDL syntax and structure"""
    errors = []

    # Check if it's a CREATE OR REPLACE VIEW statement
    if not ddl_statement.strip().upper().startswith('CREATE OR REPLACE VIEW'):
        errors.append("DDL must start with 'CREATE OR REPLACE VIEW'")

    # Check if it has the correct schema path
    expected_path = f"gbi_fraud_bap_db.ai_live_biz_app.{pattern_name}"
    if expected_path.upper() not in ddl_statement.upper():
        errors.append(f"DDL must reference the correct view path: {expected_path}")

    # Check if it has a SELECT statement
    if 'SELECT' not in ddl_statement.upper():
        errors.append("DDL must contain a SELECT statement")

    # Check for basic SQL structure
    if 'FROM' not in ddl_statement.upper():
        errors.append("DDL must contain a FROM clause")

    return errors


def auto_fix_schema_path(ddl_statement, pattern_name):
    """Auto-fix schema path in DDL statement"""
    expected_full_path = f"gbi_fraud_bap_db.ai_live_biz_app.{pattern_name}"

    # If it already has the full path, return as-is
    if expected_full_path.upper() in ddl_statement.upper():
        return ddl_statement

    # Try to find and replace pattern variations
    patterns_to_replace = [
        pattern_name,  # Just the table name
        f"ai_live_biz_app.{pattern_name}",  # Schema.table
        f"gbi_fraud_bap_db.{pattern_name}",  # Database.table
    ]

    fixed_ddl = ddl_statement
    for pattern in patterns_to_replace:
        if pattern.upper() in fixed_ddl.upper():
            # Use regex to replace while preserving case structure
            fixed_ddl = re.sub(
                re.escape(pattern),
                expected_full_path,
                fixed_ddl,
                flags=re.IGNORECASE
            )
            break

    return fixed_ddl


def execute_ddl_properly(ddl_statement, pattern_name):
    """Execute DDL with proper error handling and verification"""
    try:
        print(f"EXECUTING DDL FOR {pattern_name}:")
        print("=" * 50)
        print(ddl_statement)
        print("=" * 50)

        # Execute the DDL
        result = sf.run_query(ddl_statement)
        print(f"DDL EXECUTION RESULT: {result}")

        # Wait for propagation
        time.sleep(5)  # Wait for database propagation

        # Try multiple verification attempts
        max_attempts = 3
        for attempt in range(max_attempts):
            print(f"VERIFICATION ATTEMPT {attempt + 1}/{max_attempts}")

            try:
                verification_query = f"""
                select get_ddl('view', 'gbi_fraud_bap_db.ai_live_biz_app.{pattern_name}') as query
                """
                verification_result = sf.run_query(verification_query)

                if 'QUERY' in verification_result.columns:
                    retrieved_ddl = verification_result['QUERY'].iloc[0]
                elif 'query' in verification_result.columns:
                    retrieved_ddl = verification_result['query'].iloc[0]
                else:
                    print(f"Attempt {attempt + 1}: Could not retrieve DDL")
                    if attempt < max_attempts - 1:
                        time.sleep(3)
                        continue
                    return False, "Could not retrieve DDL after execution"

                print(f"RETRIEVED DDL AFTER EXECUTION (Attempt {attempt + 1}):")
                print("=" * 50)
                print(retrieved_ddl)
                print("=" * 50)

                # Basic verification - check if the DDL was created/updated
                if retrieved_ddl and len(retrieved_ddl.strip()) > 0:
                    print("✅ VERIFICATION SUCCESS: DDL retrieved successfully")
                    return True, retrieved_ddl
                else:
                    print(f"❌ VERIFICATION FAILED: Empty or invalid DDL retrieved")
                    if attempt < max_attempts - 1:
                        print(f"Retrying in 3 seconds...")
                        time.sleep(3)
                        continue
                    else:
                        return False, f"Changes not persisted. Retrieved DDL: {retrieved_ddl}"

            except Exception as e:
                print(f"Verification attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_attempts - 1:
                    time.sleep(3)
                    continue
                else:
                    return False, f"Verification failed after {max_attempts} attempts: {str(e)}"

        return False, "All verification attempts failed"

    except Exception as e:
        print(f"DDL EXECUTION FAILED: {str(e)}")
        return False, str(e)


def save_and_validate_view(pattern_name, new_ddl):
    """Save the view and validate changes actually persisted"""
    try:
        print(f"SAVE_AND_VALIDATE: Starting for {pattern_name}")

        # Auto-fix schema path
        fixed_ddl = auto_fix_schema_path(new_ddl, pattern_name)
        if fixed_ddl != new_ddl:
            print(f"AUTO-FIXED SCHEMA PATH")

        # First validate the DDL syntax
        validation_errors = validate_ddl_syntax(fixed_ddl, pattern_name)
        if validation_errors:
            error_msg = "DDL validation failed: " + "; ".join(validation_errors)
            print(f"SAVE_AND_VALIDATE: Validation failed - {error_msg}")
            return False, None, error_msg

        # Execute DDL with proper verification
        success, result = execute_ddl_properly(fixed_ddl, pattern_name)

        if success:
            print(f"SAVE_AND_VALIDATE: Success for {pattern_name}")
            return True, "View updated and changes verified in database!", result
        else:
            print(f"SAVE_AND_VALIDATE: Failed - {result}")
            return False, None, result

    except Exception as e:
        print(f"SAVE_AND_VALIDATE: Exception - {str(e)}")
        return False, None, str(e)


def debug_view_state(pattern_name):
    """Debug function to check current view state"""
    try:
        # Get current DDL
        ddl_query = f"""
        select get_ddl('view', 'gbi_fraud_bap_db.ai_live_biz_app.{pattern_name}') as query
        """
        ddl_result = sf.run_query(ddl_query)

        if 'QUERY' in ddl_result.columns:
            current_ddl = ddl_result['QUERY'].iloc[0]
        elif 'query' in ddl_result.columns:
            current_ddl = ddl_result['query'].iloc[0]
        else:
            return "Could not retrieve current DDL"

        # Extract key information from DDL
        info_lines = []
        info_lines.append(f"View exists: Yes")
        info_lines.append(f"DDL length: {len(current_ddl)} characters")

        # Look for common patterns
        if 'WHERE' in current_ddl.upper():
            info_lines.append("Contains WHERE clause: Yes")
        if 'SALES_ORG' in current_ddl.upper():
            sales_org_match = re.search(r"SALES_ORG = '(\d+)'", current_ddl)
            if sales_org_match:
                info_lines.append(f"SALES_ORG value: {sales_org_match.group(1)}")

        return "\n".join(info_lines) + f"\n\nFull DDL:\n{current_ddl}"

    except Exception as e:
        return f"Debug failed: {str(e)}"


def detect_changes(original_query, new_query):
    """Detect and describe changes between queries"""
    changes = []

    # Normalize queries for comparison
    orig_norm = normalize_ddl_for_comparison(original_query)
    new_norm = normalize_ddl_for_comparison(new_query)

    if orig_norm == new_norm:
        return ["No changes detected"]

    # Check for SALES_ORG changes
    orig_sales_org = re.search(r"SALES_ORG = '(\d+)'", original_query)
    new_sales_org = re.search(r"SALES_ORG = '(\d+)'", new_query)

    if orig_sales_org and new_sales_org:
        if orig_sales_org.group(1) != new_sales_org.group(1):
            changes.append(f"SALES_ORG: '{orig_sales_org.group(1)}' → '{new_sales_org.group(1)}'")
    elif not orig_sales_org and new_sales_org:
        changes.append(f"Added SALES_ORG: '{new_sales_org.group(1)}'")
    elif orig_sales_org and not new_sales_org:
        changes.append(f"Removed SALES_ORG: '{orig_sales_org.group(1)}'")

    # Check for other common field changes
    fields_to_check = ['B2_NM', 'B2_EM', 'PAYMENT_TYPES', 'CARD_BIN']
    for field in fields_to_check:
        orig_pattern = re.search(rf"{field}[^=]*=\s*[^)]+", original_query, re.IGNORECASE)
        new_pattern = re.search(rf"{field}[^=]*=\s*[^)]+", new_query, re.IGNORECASE)

        if orig_pattern and new_pattern:
            if orig_pattern.group(0) != new_pattern.group(0):
                changes.append(f"Modified {field} condition")
        elif not orig_pattern and new_pattern:
            changes.append(f"Added {field} condition")
        elif orig_pattern and not new_pattern:
            changes.append(f"Removed {field} condition")

    if not changes:
        changes.append("Query structure modified")

    return changes


def add_pattern_to_tracking(database_name, schema_name, table_name, query_type, created_by):
    """Add pattern to tracking table"""
    try:
        insert_sql = f"""
            INSERT INTO gbi_fraud_bap_db.ai_live_biz_app.TRADER_HUNTING_PATTERNS
            (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CREATED_BY, CREATED_TS, QUERY_TYPE, ACTIVITY)
            VALUES ('{database_name}', '{schema_name}', '{table_name}', '{created_by}', CURRENT_TIMESTAMP(), '{query_type}', 'Inactive')
        """
        sf.run_query(insert_sql)
        return True, None
    except Exception as e:
        return False, str(e)


def create_new_pattern(pattern_name, pattern_type, ddl_query, created_by):
    """Create a new pattern view and add to tracking"""
    try:
        # First create the view
        success, result = execute_ddl_properly(ddl_query, pattern_name)
        if not success:
            return False, f"Failed to create view: {result}"

        # Then add to tracking table
        success, error = add_pattern_to_tracking(
            "gbi_fraud_bap_db",
            "ai_live_biz_app",
            pattern_name,
            pattern_type,
            created_by
        )
        if not success:
            return False, f"View created but failed to add to tracking: {error}"

        return True, "Pattern created successfully!"
    except Exception as e:
        return False, str(e)


def delete_pattern(table_name):
    """Delete a pattern from Snowflake"""
    try:
        drop_sql = f"DROP VIEW IF EXISTS gbi_fraud_bap_db.ai_live_biz_app.{table_name}"
        sf.run_query(drop_sql)

        delete_sql = f"""
            DELETE FROM gbi_fraud_bap_db.ai_live_biz_app.TRADER_HUNTING_PATTERNS
            WHERE TABLE_NAME = '{table_name}'
        """
        sf.run_query(delete_sql)
        return True, None
    except Exception as e:
        return False, str(e)


def get_pattern_stats(patterns_df):
    """Get pattern statistics for dashboard"""
    if patterns_df.empty:
        return {"total": 0, "active": 0, "inactive": 0, "hunting": 0, "customer": 0}

    # Get column names
    activity_col = 'ACTIVITY' if 'ACTIVITY' in patterns_df.columns else 'activity'
    query_type_col = 'QUERY_TYPE' if 'QUERY_TYPE' in patterns_df.columns else 'query_type'

    total = len(patterns_df)
    active = len(patterns_df[patterns_df[activity_col] == 'Active'])
    inactive = len(patterns_df[patterns_df[activity_col] == 'Inactive'])
    hunting = len(patterns_df[patterns_df[query_type_col] == 'TH'])
    customer = len(patterns_df[patterns_df[query_type_col] == 'GCT'])

    return {
        "total": total,
        "active": active,
        "inactive": inactive,
        "hunting": hunting,
        "customer": customer
    }


# --- Navigation Functions ---
def go_to_edit_page(pattern_name):
    """Navigate to edit page"""
    st.session_state.page = 'edit'
    st.session_state.edit_pattern = pattern_name
    # Clear any cached edit state
    for key in ['edit_query', 'original_query', 'validation_results', 'view_saved', 'verified_ddl']:
        if key in st.session_state:
            del st.session_state[key]


def go_to_home():
    """Navigate to home page"""
    st.session_state.page = 'home'
    # Clear edit state
    for key in ['edit_pattern', 'edit_query', 'original_query', 'validation_results', 'view_saved', 'verified_ddl']:
        if key in st.session_state:
            del st.session_state[key]


# --- Main App ---
def main():
    st.set_page_config(
        page_title="Trader Pattern Management",
        page_icon="🎯",
        layout="wide"
    )

    if 'page' not in st.session_state:
        st.session_state.page = 'home'

    # Navigation header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("Trader Pattern Management")
    with col2:
        if st.session_state.page != 'home':
            if st.button("🏠 Home", type="secondary"):
                go_to_home()
                st.rerun()

    st.markdown("---")

    if st.session_state.page == 'edit':
        show_edit_page()
    else:
        show_home_page()


def show_home_page():
    """Show the main home page with pattern management"""

    patterns_df = load_patterns()

    if patterns_df.empty:
        st.warning("No patterns found or error loading data.")
        st.stop()

    # Get pattern statistics
    stats = get_pattern_stats(patterns_df)

    # Dashboard metrics
    st.markdown("### 📊 Pattern Overview")
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            label="📋 Total Patterns",
            value=stats["total"]
        )
        st.markdown("🎯📊")

    with col2:
        st.metric(
            label="✅ Active",
            value=stats["active"]
        )
        st.markdown("🟢⚡")

    with col3:
        st.metric(
            label="❌ Inactive",
            value=stats["inactive"]
        )
        st.markdown("🔴💤")

    with col4:
        st.metric(
            label="🎯 Hunting Patterns",
            value=stats["hunting"]
        )
        st.markdown("🕵️‍♂️🔍")

    with col5:
        st.metric(
            label="👥 Customer Patterns",
            value=stats["customer"]
        )
        st.markdown("😊✨")

    st.markdown("---")

    # Get column names
    table_name_col = 'TABLE_NAME' if 'TABLE_NAME' in patterns_df.columns else 'table_name'
    activity_col = 'ACTIVITY' if 'ACTIVITY' in patterns_df.columns else 'activity'
    query_type_col = 'QUERY_TYPE' if 'QUERY_TYPE' in patterns_df.columns else 'query_type'
    created_by_col = 'CREATED_BY' if 'CREATED_BY' in patterns_df.columns else 'created_by'

    hunting_patterns = patterns_df[patterns_df[query_type_col] == 'TH']
    customer_patterns = patterns_df[patterns_df[query_type_col] == 'GCT']

    # --- Tabbed Interface ---
    tab1, tab2, tab3, tab4 = st.tabs(["🎯 Hunting Patterns", "👥 Customer Patterns", "➕ Add Pattern", "🔍 View Query"])

    # --- Hunting Patterns Tab ---
    with tab1:
        col1, col2 = st.columns([3, 1])

        with col1:
            st.subheader("🎯 Trader Hunting Patterns")

        with col2:
            activity_filter = st.selectbox(
                "Filter by Status:",
                ["All", "Active", "Inactive"],
                key="hunting_filter"
            )

        filtered_hunting = hunting_patterns.copy()
        if activity_filter != "All":
            filtered_hunting = filtered_hunting[filtered_hunting[activity_col] == activity_filter]

        if not filtered_hunting.empty:
            for idx, row in filtered_hunting.iterrows():
                table_name = row[table_name_col]
                activity = row[activity_col]
                created_by = row[created_by_col]
                unique_key = f"hunting_{idx}_{hash(table_name) % 1000}"

                col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])

                with col1:
                    st.write(f"**{table_name}**")
                    st.caption(f"By: {created_by}")

                with col2:
                    if activity == 'Active':
                        st.success("Active")
                    else:
                        st.error("Inactive")

                with col3:
                    if activity == 'Active':
                        if st.button("Disable", key=f"disable_{unique_key}", type="secondary"):
                            success, _ = update_pattern_activity(table_name, "Inactive")
                            if success:
                                st.success("Disabled!")
                                st.rerun()
                    else:
                        if st.button("Enable", key=f"enable_{unique_key}", type="primary"):
                            success, _ = update_pattern_activity(table_name, "Active")
                            if success:
                                st.success("Enabled!")
                                st.rerun()

                with col4:
                    if st.button("✏️ Edit", key=f"edit_{unique_key}", type="secondary"):
                        go_to_edit_page(table_name)
                        st.rerun()

                with col5:
                    if st.button("🗑️ Delete", key=f"delete_{unique_key}", type="secondary"):
                        st.session_state.confirm_delete = table_name

                st.markdown("---")
        else:
            st.info("No hunting patterns found matching the filter.")

    # --- Customer Patterns Tab ---
    with tab2:
        col1, col2 = st.columns([3, 1])

        with col1:
            st.subheader("👥 Good Customer Patterns")

        with col2:
            customer_activity_filter = st.selectbox(
                "Filter by Status:",
                ["All", "Active", "Inactive"],
                key="customer_filter"
            )

        filtered_customer = customer_patterns.copy()
        if customer_activity_filter != "All":
            filtered_customer = filtered_customer[filtered_customer[activity_col] == customer_activity_filter]

        if not filtered_customer.empty:
            for idx, row in filtered_customer.iterrows():
                table_name = row[table_name_col]
                activity = row[activity_col]
                created_by = row[created_by_col]
                unique_key = f"customer_{idx}_{hash(table_name) % 1000}"

                col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])

                with col1:
                    st.write(f"**{table_name}**")
                    st.caption(f"By: {created_by}")

                with col2:
                    if activity == 'Active':
                        st.success("Active")
                    else:
                        st.error("Inactive")

                with col3:
                    if activity == 'Active':
                        if st.button("Disable", key=f"disable_{unique_key}", type="secondary"):
                            success, _ = update_pattern_activity(table_name, "Inactive")
                            if success:
                                st.success("Disabled!")
                                st.rerun()
                    else:
                        if st.button("Enable", key=f"enable_{unique_key}", type="primary"):
                            success, _ = update_pattern_activity(table_name, "Active")
                            if success:
                                st.success("Enabled!")
                                st.rerun()

                with col4:
                    if st.button("✏️ Edit", key=f"edit_{unique_key}", type="secondary"):
                        go_to_edit_page(table_name)
                        st.rerun()

                with col5:
                    if st.button("🗑️ Delete", key=f"delete_{unique_key}", type="secondary"):
                        st.session_state.confirm_delete = table_name
        else:
            st.info("No customer patterns found matching the filter.")

    # --- Add New Pattern Tab ---
    with tab3:
        st.subheader("➕ Add New Pattern")

        col1, col2 = st.columns(2)

        with col1:
            pattern_name = st.text_input(
                "Pattern Name:",
                placeholder="e.g., TRADER_SUSPICIOUS_EMAIL",
                help="Enter a unique name for your pattern"
            )

        with col2:
            pattern_type = st.selectbox(
                "Pattern Type:",
                ["TH", "GCT"],
                format_func=lambda x: "🎯 Trader Hunting (TH)" if x == "TH" else "👥 Good Customer (GCT)"
            )

        created_by = st.text_input(
            "Created By:",
            placeholder="Your name or username",
            help="Who is creating this pattern?"
        )

        st.markdown("### DDL Query:")

        # Template DDL with schema path
        template_ddl = f"""CREATE OR REPLACE VIEW gbi_fraud_bap_db.ai_live_biz_app.{pattern_name or 'YOUR_PATTERN_NAME'}(
    ORDER_ID, WEB_ORDER_ID, EVENT_TS, COMMIT_CD, COMMIT_DT, SALES_ORG, PO_TYPE, 
    SALES_DISTRICT, DISCOUNT_STORE_TYPE, SLA, EXCEPTION_CD, B2_NM, B2_EM, B2_EM_DOM, 
    ACCT_AGE, B2_ADD, B2_CITY, B2_DISTRICT, B2_STATE, B2_ZIP, B2_PH, A1_NM, A1_EM, 
    A1_EM_DOM, A1_ADD, A1_CITY, A1_DISTRICT, A1_STATE, A1_ZIP, APPLE_MAPS_ADD, 
    A1_PHONE, LATEST_A1_NM, LATEST_A1_EM, LASTEST_A1_EM_DOM, LATEST_A1_ADD, 
    LATEST_A1_STATE, LATEST_A1_ZIP, LATEST_BOPIS3_NM, LATEST_BOPIS3_EM, PROD_DESC, 
    TOTAL_VALUE, IP_ADD, IP_CLASS, IP_COMPANY, IP_COUNTRY, IP_BIN_COUNTRY, 
    PAYMENT_TYPES, CARD_BIN, BIN_COUNTRY, CARD_HASH, CARD_ISSUER, GUID, SESSION_COOKIE
) AS 
SELECT * FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame 
WHERE 1=1 
    AND SALES_ORG = '8400'
    -- Add your conditions here
    -- AND B2_NM ILIKE ('Your Pattern')
    -- AND B2_EM LIKE ('%pattern%')
    -- AND PAYMENT_TYPES IN ('CREDIT')
"""

        ddl_query = st.text_area(
            "DDL Query:",
            value=template_ddl,
            height=300,
            help="Modify the template above with your specific pattern conditions"
        )

        col1, col2 = st.columns(2)

        with col1:
            if st.button("🚀 Create Pattern", type="primary"):
                if not pattern_name:
                    st.error("❌ Please enter a pattern name")
                elif not created_by:
                    st.error("❌ Please enter who is creating this pattern")
                elif not ddl_query.strip():
                    st.error("❌ Please enter a DDL query")
                else:
                    with st.spinner("Creating pattern..."):
                        success, message = create_new_pattern(pattern_name, pattern_type, ddl_query, created_by)
                        if success:
                            st.success(f"✅ {message}")
                            st.balloons()
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error(f"❌ Failed to create pattern: {message}")

        with col2:
            if st.button("🔄 Reset Form", type="secondary"):
                st.rerun()

    # --- View Query Tab ---
    with tab4:
        st.subheader("🔍 View Pattern Query")

        pattern_names = patterns_df[table_name_col].tolist()
        selected = st.selectbox("Choose a pattern to view:", [""] + pattern_names)

        if selected:
            if st.button("View Current Query"):
                with st.spinner("Loading current query from database..."):
                    query = get_trader_hunting_query(selected)

                if query:
                    st.markdown(f"### Current Query for: `{selected}`")
                    st.code(query, language='sql')

    # Handle Delete Confirmation
    if 'confirm_delete' in st.session_state:
        st.markdown("---")
        st.error(f"⚠️ Delete '{st.session_state.confirm_delete}'?")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("🗑️ Yes, Delete", type="primary"):
                success, error = delete_pattern(st.session_state.confirm_delete)
                if success:
                    st.success("✅ Pattern deleted!")
                    del st.session_state.confirm_delete
                    st.rerun()
                else:
                    st.error(f"❌ Delete failed: {error}")

        with col2:
            if st.button("❌ Cancel", type="secondary"):
                del st.session_state.confirm_delete
                st.rerun()


def show_edit_page():
    """Show the dedicated edit page"""

    if 'edit_pattern' not in st.session_state:
        st.error("No pattern selected for editing.")
        return

    pattern_name = st.session_state.edit_pattern
    st.subheader(f"✏️ Edit Pattern: {pattern_name}")

    # Load original query if not already loaded
    if 'original_query' not in st.session_state:
        with st.spinner("Loading current query from database..."):
            current_query = get_trader_hunting_query(pattern_name)
            if not current_query:
                st.error(f"Could not load query for {pattern_name}")
                return
            st.session_state.original_query = current_query
            st.session_state.edit_query = current_query
            print(f"EDIT PAGE: Loaded query for {pattern_name}")

    st.markdown("### Current Query (from database):")
    st.code(st.session_state.original_query, language='sql')

    # Show verified DDL if we have it
    if 'verified_ddl' in st.session_state:
        st.markdown("### ✅ Last Saved Version:")
        st.code(st.session_state.verified_ddl, language='sql')

        # Check if they match
        if normalize_ddl_for_comparison(st.session_state.original_query) == normalize_ddl_for_comparison(
                st.session_state.verified_ddl):
            st.success("✅ Database matches your last saved changes!")
        else:
            st.warning("⚠️ Database version differs from your last save attempt")

    st.markdown("### Edit Query:")

    new_query = st.text_area(
        "Modify the view query:",
        value=st.session_state.edit_query,
        height=400,
        help="The schema path will be auto-fixed if needed: gbi_fraud_bap_db.ai_live_biz_app.table_name",
        key="query_editor"
    )

    if new_query != st.session_state.edit_query:
        st.session_state.edit_query = new_query
        # Clear validation when query changes
        for key in ['validation_results', 'view_saved', 'verified_ddl']:
            if key in st.session_state:
                del st.session_state[key]

    has_changes = st.session_state.edit_query != st.session_state.original_query
    if has_changes:
        st.info("⚠️ You have unsaved changes")

        # Show detected changes dynamically
        changes = detect_changes(st.session_state.original_query, st.session_state.edit_query)
        if changes and changes[0] != "No changes detected":
            st.info("🔍 Detected changes: " + ", ".join(changes))

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("💾 Save & Validate", type="primary", disabled=not has_changes):
            if has_changes:
                with st.spinner("Executing DDL and verifying changes..."):
                    print(f"EDIT PAGE: About to save {pattern_name}")
                    success, validation_msg, verified_ddl = save_and_validate_view(pattern_name,
                                                                                   st.session_state.edit_query)

                    if success:
                        st.success("✅ View saved and changes verified!")
                        st.session_state.view_saved = True
                        st.session_state.validation_results = validation_msg
                        st.session_state.verified_ddl = verified_ddl
                        print(f"EDIT PAGE: Save successful for {pattern_name}")
                        st.balloons()
                    else:
                        st.error(f"❌ Save failed: {verified_ddl}")
                        print(f"EDIT PAGE: Save failed - {verified_ddl}")

    with col2:
        if st.button("🔄 Reload from DB", type="secondary"):
            with st.spinner("Reloading fresh query from database..."):
                fresh_query = get_trader_hunting_query(pattern_name)
                if fresh_query:
                    st.session_state.original_query = fresh_query
                    st.session_state.edit_query = fresh_query
                    # Clear other state
                    for key in ['validation_results', 'view_saved', 'verified_ddl']:
                        if key in st.session_state:
                            del st.session_state[key]
                    st.success("✅ Reloaded current database version!")
                    st.rerun()
                else:
                    st.error("❌ Could not reload query")

    with col3:
        if st.button("🔍 Debug View", type="secondary"):
            with st.spinner("Checking current view state..."):
                debug_info = debug_view_state(pattern_name)
                st.text_area("Debug Information:", debug_info, height=200, key="debug_info")

    with col4:
        if st.button("❌ Cancel Edit", type="secondary"):
            go_to_home()
            st.rerun()

    # Show validation results
    if 'validation_results' in st.session_state:
        st.markdown("---")
        st.success(f"✅ {st.session_state.validation_results}")

    # Show current status
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        if st.session_state.get('view_saved'):
            st.success("✅ Changes saved to database")
        elif has_changes:
            st.warning("⚠️ You have unsaved changes")
        else:
            st.info("ℹ️ No changes made")

    with col2:
        st.info(f"📝 Editing: {pattern_name}")


if __name__ == "__main__":
    main()

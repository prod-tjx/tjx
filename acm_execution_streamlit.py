import streamlit as st
import pandas as pd
from datetime import datetime
import io # Needed for in-memory Excel export
from ai_sds_fulfillment_tfa.streamlit.pages.trader.helpers.acm_trader_helper import ACMTraderHelper
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Streamlit Page Configuration ---
st.set_page_config(layout="wide", page_title="ACM Execution Tool")
st.title('''📊 ACM Execution Tool''')

# --- Configuration Mappings ---
ACTION_AND_NOTES_MAPPING = {
    "Clear": {"action": "TRADERCLEAR",
              "notes": "SDS Trader Clear",
              "color_class": "green-button"
              },
    "Confirm": {"action": "TRADERCONFIRM",
                "notes": "SDS Trader Confirm",
                "color_class": "red-button"
                },
    "False Positive": {"action": "TRADERFALSEPOSITIVE",
                       "notes": "SDS Trader Clear",
                       "color_class": "blue-button"
                       },
    "Unworkable": {"action": "TRADERUNWORKABLE",
                   "notes": "SDS Trader Confirm",
                   "color_class": "orange-button"
                   }
}

# --- Session State Default Value ---
STATE_INITIAL_VALUES = {
    'step': 'select_action_type',
    'selected_action_type_char': None,
    'order_ids_input': '',
    'user_notes_suffix': '',
    'processed_data_for_execute': None,
    'api_call_status': None,
    'api_call_message': None
}

# --- State initiation Function ---
def init_session_state():
    for key, default_val in STATE_INITIAL_VALUES.items():
        if key not in st.session_state:
            st.session_state[key] = default_val

init_session_state()

# --- Helper Functions for State Transitions and Logic ---
def load_css(css_file):
    """Function to load and inject a CSS file."""
    with open(css_file) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

def reset_app_state():
    """Resets all relevant session state variables to start fresh."""
    for key, default_val in STATE_INITIAL_VALUES.items():
        st.session_state[key] = default_val
    logging.info('Session state reset to defaults')

def set_action_type_and_proceed(action_char):
    """Sets the chosen action type and moves to the next step."""
    st.session_state.selected_action_type_char = action_char
    st.session_state.step = 'enter_details'
    # Clear previous inputs when changing action type for a clean start
    st.session_state.order_ids_input = ''
    st.session_state.user_notes_suffix = ''
    # Also clear any previous export/API status
    st.session_state.processed_data_for_execute = None
    st.session_state.api_call_status = None
    st.session_state.api_call_message = None

def process_and_generate_data():
    """Processes the user input and prepares data for execution."""
    raw_order_ids = st.session_state.order_ids_input
    user_notes_suffix = st.session_state.user_notes_suffix
    selected_char = st.session_state.selected_action_type_char

    # Input validation
    if not raw_order_ids.strip():
        raise Exception("No order_id's in the text area")

    if not user_notes_suffix.strip():
        raise Exception("No notes suffix in the text area")
        # Force notes suffix for now, can adjust after notes alignment

    # Parse and clean order IDs (split by newline, strip whitespace, remove empty)
    cleaned_order_ids = [
        oid.strip() for oid in raw_order_ids.split('\n') if oid.strip()
    ]

    if not cleaned_order_ids:
        raise Exception("No valid order_id's after refactoring")

    # Get action code and notes prefix based on selected type
    action_code = ACTION_AND_NOTES_MAPPING.get(selected_char).get("action")
    notes_prefix = ACTION_AND_NOTES_MAPPING.get(selected_char).get("notes")

    # Generate data for each order ID
    data_rows = []

    for oid in cleaned_order_ids:
        full_notes = f"{notes_prefix} {user_notes_suffix}"
        data_rows.append({
            'order_id': oid,
            'action': action_code,
            'notes': full_notes,
        })

    st.session_state.processed_data_for_execute = pd.DataFrame(data_rows)
    st.session_state.step = 'review_and_execute'
    # Reset API status when generating new data
    st.session_state.api_call_status = None
    st.session_state.api_call_message = None
    return True

def export_to_excel_and_download():
    """
    Providing a download button (optional step).
    """
    if st.session_state.processed_data_for_execute is None or st.session_state.processed_data_for_execute.empty:
        st.warning("No data to export. Please generate data first.")
        return

    df_to_export = st.session_state.processed_data_for_execute
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"execution_data_{timestamp_str}.xlsx"

    # Provide a download button (standard for web apps) 
    # Create an in-memory Excel file for download
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        df_to_export.to_excel(writer, index=False, sheet_name='Action Data')
    excel_buffer.seek(0) # Rewind the buffer to the beginning

    st.download_button(
        label="Download Excel File (Web)",
        data=excel_buffer,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        help="Click to download the generated Excel file to your local machine via browser."
    )
    st.info("The file is also available for download via your browser.")

def trigger_acm_api_action():
    """
    Calls the ACM API using the prepared DataFrame.
    Updates session state with the result.
    """
    try:
        # Create instance for Trader Action
        acm_client = ACMTraderHelper(env="prod")
        # Use processed dataframe to action
        df = st.session_state.processed_data_for_execute
        acm_client.df_for_action(df)
        # Action the cases
        acm_client.action_cases()

        st.session_state.api_call_status = 'success'
        st.session_state.api_call_message = (f"Action request successfully submitted for above {len(st.session_state.processed_data_for_execute)} orders."
                                             f"\n\n Mass action key {acm_client.mass_action_key}")
    except Exception as e:
        st.session_state.api_call_status = 'failed'
        st.session_state.api_call_message = f"Error sending to ACM API: {e}"

    st.rerun() # Force a rerun to display the API call status

# --- Main Application UI Logic ---

def main_app_ui():
    # Use logging to see current step in terminal
    logging.info(f"Rendering main_app_ui. Current step: {st.session_state.step}")

    # Load CSS from assets folder
    import os
    css_path = os.path.join(os.path.dirname(__file__), 'assets/streamlit_style.css')
    if os.path.exists(css_path):
        load_css(css_path)
    else:
        st.warning("CSS file not found, using default styling.")

    if st.session_state.step == 'select_action_type':
        st.subheader("Step 1: Choose Action Type")

        button_labels = list(ACTION_AND_NOTES_MAPPING.keys())
        cols = st.columns(len(button_labels))

        for i, label in enumerate(button_labels):
            with cols[i]:
                # Get the color class from the mapping
                color_class = ACTION_AND_NOTES_MAPPING[label]["color_class"]

                # Create the invisible marker div with the custom class
                st.markdown(f'<div class="{color_class}"></div>', unsafe_allow_html=True)

                if st.button(label, use_container_width=True, key=f"btn_{label}"):
                    set_action_type_and_proceed(label)
                    st.rerun() # Force rerun to show next step

    elif st.session_state.step == 'enter_details':
        st.subheader(f"Step 2: Enter Details for Action Type {st.session_state.selected_action_type_char}")

        st.info(f"Notes will start with: `{ACTION_AND_NOTES_MAPPING.get(st.session_state.selected_action_type_char).get('notes')}`")

        # Text area for multiple Order IDs
        st.session_state.order_ids_input = st.text_area(
            "Paste Order IDs (one per line):",
            value=st.session_state.order_ids_input,
            height=200,
            help="Paste a list of Order IDs, each on a new line. You can copy directly from an Excel column."
        )

        # Text area for notes suffix
        st.session_state.user_notes_suffix = st.text_area(
            "Notes (suffix like MR, TH, etc):",
            value=st.session_state.user_notes_suffix,
            help="Enter the specific details to append to the predefined notes. This will apply to all Order IDs."
        )

        col_back, col_submit = st.columns([1, 2])
        with col_back:
            if st.button("⬅️ Back to Action Types"):
                reset_app_state() # Go back to step 1
                st.rerun()
        with col_submit:
            if st.button("Generate Data for Execute", type="primary", use_container_width=True):
                if process_and_generate_data(): # Only rerun if processing was successful
                    st.rerun()

    elif st.session_state.step == 'review_and_execute':
        st.subheader("Step 3: Preview and Execute")

        if st.session_state.processed_data_for_execute is not None:
            st.write(f"Here's the data that will be executed: total {len(st.session_state.processed_data_for_execute)} ")
            st.dataframe(st.session_state.processed_data_for_execute, hide_index=True)

            st.markdown("---")
            st.write("Click the button below to save the Excel file:")
            export_to_excel_and_download()

            st.markdown("---")
            st.subheader("Send to ACM API")

            # Display API call status if available
            if st.session_state.api_call_status == 'success':
                st.success(st.session_state.api_call_message)
            elif st.session_state.api_call_status == 'failed':
                st.error(st.session_state.api_call_message)

            # Button to trigger API call
            send_button_help = ""

            if st.button(
                "🚀 Send to Action",
                type="secondary",
                help=send_button_help,
                disabled=(st.session_state.api_call_status is not None)
            ):
                trigger_acm_api_action()

        else:
            st.warning("No data generated. Please go back and enter details.")

        st.markdown("---")
        if st.button("Start Over", type="secondary"):
            reset_app_state()
            st.rerun()

# --- Run the main UI function ---
main_app_ui()

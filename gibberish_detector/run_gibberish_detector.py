from gibberish_detector_helper import *
from ai.snowflake.compat.connection import sf
import yaml

# Load config
with open("config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

input_type = cfg.get("input_type", "excel").lower()

# check: 'input_type' is valid
if input_type not in ["excel", "sql"]:
    raise ValueError(f"Invalid input_type '{input_type}'. Please set it to 'excel' or 'sql' in config.yaml.")

email_col = cfg["common"]["email_col"]

# Load data
if input_type == "sql":
    input_path = None
    df_input = sf.run_query(cfg["sql"]["query"])
elif input_type == "excel":
    input_path = cfg["excel"]["filepath"]
    df_input = pd.read_excel(input_path)
else:
    raise ValueError("input_type must be 'sql' or 'excel'.")

# Predict
try:
    df_result = add_gibberish_scores(df_input, email_col)
    save_result_to_excel(df_result, input_path)
except KeyError:
    logging.error("Invalid column name. Please check and update 'email_col' (case sensitive) in config.yaml, e.g. 'B2_EM' or 'email_txt'.")


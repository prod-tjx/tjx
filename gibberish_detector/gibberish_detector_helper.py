import subprocess
import sys
import os
from datetime import datetime
import logging

# create "logs" folder (if not exist)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/gibberish_detector.log"),
        logging.StreamHandler()
    ]
)

def install_if_missing_libs(import_name, pip_name=None):
    try:
        __import__(import_name)
    except ImportError:
        to_install = pip_name or import_name
        logging.info(f"{import_name} not found. Installing {to_install}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", to_install])

install_if_missing_libs("joblib")
install_if_missing_libs("yaml", "PyYAML")
install_if_missing_libs("pandas")
install_if_missing_libs("sklearn", "scikit-learn")

import joblib
import pandas as pd

# Set up project paths
ROOT_DIR = os.getcwd()
sys.path.append(ROOT_DIR)

# Import feature extraction module
from src.feature_extractor import extract_struct_features, NameDictionary

# Load trained models and vectorizer
ngram_model = joblib.load(os.path.join(ROOT_DIR, "models", "ngram_logistic_model.pkl"))
vectorizer = joblib.load(os.path.join(ROOT_DIR, "models", "ngram_vectorizer.pkl"))
struct_model = joblib.load(os.path.join(ROOT_DIR, "models", "structure_random_forest_model.pkl"))
name_dict = NameDictionary()

# Model parameters
WEIGHT_STRUCT = 0.3
WEIGHT_NGRAM = 0.7

def predict_gibberish(handle: str)-> tuple:
    """
        Predict whether a handle is gibberish based on structural and n-gram features.

        Parameters:
            handle (str): The input handle (part of email before '@') to analyze.

        Returns:
            tuple: A tuple containing (final_score, ngram_score, struct_score).
                   If an error occurs, all values are None.
        """

    try:
        # Extract structural features
        struct_features = extract_struct_features(handle, name_dict)
        X_df = pd.DataFrame([struct_features], columns=struct_model.feature_names_in_)
        struct_score = struct_model.predict_proba(X_df)[:, 1][0]

        # Extract n-gram features
        ngram_input = vectorizer.transform([handle])
        ngram_score = ngram_model.predict_proba(ngram_input)[:, 1][0]

        # Combine scores
        final_score = WEIGHT_STRUCT * struct_score + WEIGHT_NGRAM * ngram_score

        return final_score, ngram_score, struct_score

    except Exception as e:
        logging.error(f"predict_gibberish failed for: {handle}")
        logging.error(f"Error: {type(e).__name__} — {e}")
        return None, None, None


# Prediction function
def add_gibberish_scores(df: pd.DataFrame, email_col="email") -> pd.DataFrame:
    """
        Predict gibberish scores for each email in a DataFrame.

        Parameters:
            df (pd.DataFrame): The input DataFrame containing email data.
            email_col (str): The column name that contains the email addresses. Default is "email".

        Returns:
            pd.DataFrame: A copy of the input DataFrame with added columns:
                          'final_score', 'ngram_score', 'struct_score'.
                          If a row fails prediction, its scores will be None.
        """

    results = []

    for email in df[email_col]:
        handle = None
        try:
            handle = str(email).split("@")[0]
            final_score, ngram_score, struct_score = predict_gibberish(handle)
        except Exception as e:
            logging.error(f"Error processing: {email} -> handle: {handle}")
            logging.error(f"Error: {type(e).__name__}: {e}")
            final_score, ngram_score, struct_score = None, None, None

        results.append((final_score, ngram_score, struct_score))

    df_out = df.copy()
    df_out[["final_score", "ngram_score", "struct_score"]] = results
    return df_out


def save_result_to_excel(df, input_path=None, output_dir="result"):

    # Make sure Excel will treat = as a string not a function to prevent any error when opening file
    def escape_formula(text):
        if isinstance(text, str) and text.startswith('='):
            return "'" + text
        return text
    df_clean = df.apply(lambda col: col.map(escape_formula) if col.dtype == 'object' else col)

    base_name = datetime.now().strftime("%Y%m%d_%H%M")

    # create "result" folder (if not exist)
    os.makedirs(output_dir, exist_ok=True)

    output_file = f"{base_name}_result.xlsx"
    output_path = os.path.join(output_dir, output_file)

    # out put to excel
    df_clean.to_excel(output_path, index=False)
    logging.info(f"Result saved into {output_path}")

import os
import sys
import pandas as pd
import joblib

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(ROOT_DIR)

from src.feature_extractor import extract_struct_features, NameDictionary

name_dict = NameDictionary()
struct_model = joblib.load(os.path.join(ROOT_DIR, "models", "structure_random_forest_model.pkl"))

def analyze_struct_features(email: str):
    handle = email.split("@")[0]
    features = extract_struct_features(handle, name_dict)

    df = pd.DataFrame([features], columns=struct_model.feature_names_in_)

    prob = struct_model.predict_proba(df)[:, 1][0]
    df["struct_score"] = round(prob, 4)

    print(f"Email: {email}")
    print(f"Handle: {handle}")
    print(df)

# Testing
analyze_struct_features('H6sqr8vMGEC6@gmail.com')

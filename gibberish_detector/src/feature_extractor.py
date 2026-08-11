import re
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from datetime import datetime

STRUCT_FEATURE_NAMES = [
    "length",
    "symbol_rate",
    "consonant_rate",
    "digit_streak",
    "uppercase_rate",
    "alternation_rate",
    "transition_rate",
    "name_match"
]

# =========== Struct Feature ================
def camel_case_analysis(handle: str) -> dict:
    """
      Analyze uppercase usage and case alternation in a string.
      Returns:
          dict: A dictionary with:
              - 'uppercase_rate' (float): Percentage of uppercase letters.
              - 'alternation_rate' (float): Rate of switching between upper and lower case.
    """

    letters = [c for c in handle if c.isalpha()]
    total = len(letters)
    if total == 0:
        return {"uppercase_rate": 0, "alternation_rate": 0, "max_upper_streak": 0}

    uppercase_count = sum(1 for c in letters if c.isupper())
    case_transitions = sum(
        1 for i in range(1, total) if letters[i].isupper() != letters[i - 1].isupper()
    )

    return {
        "uppercase_rate": uppercase_count / total,
        "alternation_rate": case_transitions / (total - 1) if total > 1 else 0
    }

def compute_transition_rate(handle: str) -> float:
    """
        Compute the character-type transition rate in a string.
        Switches between letters and non-letters (digits or symbols).
    """

    handle = handle.lower()
    transitions = sum(
        1 for i in range(1, len(handle)) if handle[i].isalpha() != handle[i - 1].isalpha()
    )
    return transitions / max(1, len(handle) - 1)

def is_possible_date(digit_str: str) -> bool:
    """
        Check if a numeric string could represent a date in formats like:
        YYYYMMDD, DDMMYYYY, MMDDYYYY, YYMMDD, DDMMYY, MMDDYY.
        Returns:
        bool: True if the string looks like a valid date, False otherwise.
    """
    if not digit_str.isdigit():
        return False

    # Define acceptable date patterns based on the length of the string
    if len(digit_str) == 8:
        patterns = ["%Y%m%d", "%d%m%Y", "%m%d%Y"]
    elif len(digit_str) == 6:
        patterns = ["%y%m%d", "%d%m%y", "%m%d%y"]
    else:
        return False

    # Check if the year is within 1900 ～ 2050
    for pattern in patterns:
        try:
            parsed_date = datetime.strptime(digit_str, pattern)

            if 1900 <= parsed_date.year <= 2050:
                return True
        except ValueError:
            continue

    # If none of the patterns matched a valid date, return False
    return False


# ===========Name Dictionary Matching============
class NameDictionary:
    """
      Loads name dictionaries from text files and provides methods to check if any name appears in a given handle.
      Each .txt dictionary file should be in the `name_dicts` folder.
    """
    def __init__(self, dict_folder="name_dicts"):
        self.dictionaries = {}
        for filename in os.listdir(dict_folder):
            if filename.endswith(".txt"):
                lang = filename.replace("_names.txt", "")
                self.dictionaries[lang] = self.load_dict(os.path.join(dict_folder, filename))

    def load_dict(self, path):
        with open(path, encoding='utf-8') as f:
            return set(line.strip().lower() for line in f)

    def match_any(self, handle: str) -> int:
        """
            Checks if any name from the loaded dictionaries appears in the given handle.
            Returns:
                int: 1 if any name is found in the handle, 0 otherwise.
         """
        handle = handle.lower()
        for name_set in self.dictionaries.values():
            for name in name_set:
                if name in handle:
                    return 1
        return 0

# ===========Struct Feature Extraction===========
def extract_struct_features(handle: str, name_dict: NameDictionary) -> list:
    """
        Extract structural features from an email handle.

        Features include:
          - length
          - symbol rate
          - consonant rate
          - digit streak (excluding possible dates)
          - camel case features
          - transition rate
          - name match indicator

        Parameters:
            handle (str): The email handle.
            name_dict (NameDictionary): A name dictionary object for matching.

        Returns:
            list: List of extracted numeric features.
    """

    length = len(handle)

    symbol_count = len(re.findall(r"[._\-+]", handle))
    symbol_rate = symbol_count / max(length, 1)

    letters = re.findall(r"[a-zA-Z]", handle)
    if len(letters) < 3:
        consonant_rate = 0
    else:
        consonants = re.findall(r"[bcdfghjklmnpqrstvwxyzBCDFGHJKLMNPQRSTVWXYZ]", handle)
        consonant_rate = len(consonants) / len(letters)

    digit_groups = re.findall(r"\d+", handle)
    digit_streak = max((len(g) for g in digit_groups if not is_possible_date(g)), default=0)

    camel = camel_case_analysis(handle)

    transition_rate = compute_transition_rate(handle)

    name_match = name_dict.match_any(handle)

    return [
        length,
        symbol_rate,
        consonant_rate,
        digit_streak,
        camel["uppercase_rate"],
        camel["alternation_rate"],
        transition_rate,
        name_match
    ]



# =========TF-IDF Vectorizer for N-Gram===========
def build_ngram_vectorizer():
    """
        Build a TF-IDF vectorizer for 2-gram and 3-gram character patterns.
        Returns:
            TfidfVectorizer: Configured vectorizer with max 3000 features.
    """
    return TfidfVectorizer(
        ngram_range=(2, 3),
        analyzer='char',
        max_features=3000
    )

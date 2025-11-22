import pandas as pd
import numpy as np
import json

# ========================================================
#                CLEAN credits.csv
# ========================================================
print("Cleaning credits.csv...")

# pd.read_csv → reads the CSV file into a "DataFrame"
# A DataFrame = an Excel-like table inside Python
credits = pd.read_csv(
    r"C:\Users\hp\Desktop\sql-ultimate-course\me\archive\credits.csv",
    low_memory=False   # tells pandas to read safely even if slow
)

# Convert cast and crew columns into plain text (string)
# because they contain JSON-like lists that confuse SQL
credits["cast"] = credits["cast"].astype(str)
credits["crew"] = credits["crew"].astype(str)

# Convert id into a number but:
# errors='coerce' → if conversion fails, turn the value into NaN instead of error
credits["id"] = pd.to_numeric(credits["id"], errors="coerce")

# Replace NaN with empty strings, because SQL BULK INSERT cannot handle NaN
credits = credits.fillna("")

# Save back to disk as UTF-16
# SQL Server's BULK INSERT handles UTF-16 way better than UTF-8
credits.to_csv(
    r"C:\Users\hp\Desktop\sql-ultimate-course\me\archive\credits_clean.csv",
    index=False,
    encoding="utf-16"
)

print("Saved credits_clean.csv ✔")



# ========================================================
#                CLEAN keywords.csv
# ========================================================
print("Cleaning keywords.csv...")

keywords = pd.read_csv(
    r"C:\Users\hp\Desktop\sql-ultimate-course\me\archive\keywords.csv",
    low_memory=False
)

# Convert id into numeric safely
keywords["id"] = pd.to_numeric(keywords["id"], errors="coerce")

# Convert keywords column to plain text (JSON stored as string)
keywords["keywords"] = keywords["keywords"].astype(str)

# Remove NaN
keywords = keywords.fillna("")

# Save as UTF-16 for SQL Server bulk insert
keywords.to_csv(
    r"C:\Users\hp\Desktop\sql-ultimate-course\me\archive\keywords_clean.csv",
    index=False,
    encoding="utf-16"
)

print("Saved keywords_clean.csv ✔")



# ========================================================
#            CLEAN movies_metadata.csv
# ========================================================
print("Cleaning movies_metadata.csv...")

# dtype=str → treat ALL columns as text
# keeps messy IMDB data safe
df = pd.read_csv(
    r"C:\Users\hp\Desktop\sql-ultimate-course\me\archive\movies_metadata.csv",
    dtype=str,               # do NOT guess numeric types
    low_memory=False,
    na_values=['\\N', 'NA', 'NaN', ''],  # treat these as missing values
    keep_default_na=False               # do not treat empty string as NaN
)

print("Rows loaded:", len(df))


# --------------------------------------------------------
# Fix JSON-like columns that contain lists/objects
# Many rows have single quotes → invalid JSON
# We try loading the JSON → if it breaks, replace ' with "
# --------------------------------------------------------
json_columns = [
    "belongs_to_collection",
    "genres",
    "production_companies",
    "production_countries",
    "spoken_languages"
]

def try_fix_json(value):
    """
    Fixes messy JSON-like strings by:
    - returning empty if blank
    - trying json.loads() (valid JSON)
    - if invalid → replace single quotes with double quotes
    """
    if pd.isna(value) or value == "":
        return value

    try:
        json.loads(value)   # try parsing JSON
        return value        # if works, return original
    except:
        # If JSON is invalid, replace quotes
        return value.replace("'", '"')

# Apply JSON repair on all listed columns
for col in json_columns:
    if col in df.columns:
        df[col] = df[col].apply(try_fix_json)



# --------------------------------------------------------
# Remove unwanted characters from numeric columns
# (commas, dollar signs, spaces, etc.)
# --------------------------------------------------------
num_cols = ["budget", "revenue", "runtime", "popularity", "vote_average", "vote_count"]

for col in num_cols:
    if col in df.columns:
        # Convert to text and strip out all non-numerical characters
        df[col] = (
            df[col].astype(str)
                    .str.replace(r"[^0-9.\-]", "", regex=True)
        )



# --------------------------------------------------------
# Fix release_date column into YYYY-MM-DD
# --------------------------------------------------------
if "release_date" in df.columns:

    def fix_date(d):
        """
        Tries to convert the string to a proper date.
        If not possible → becomes 'NaT' which we turn into empty string.
        """
        try:
            return str(pd.to_datetime(d, errors="coerce")).split(" ")[0]
        except:
            return ""

    df["release_date"] = df["release_date"].apply(fix_date)



# --------------------------------------------------------
# Truncate very long text columns
# SQL Server will fail if lengths are too large
# --------------------------------------------------------
truncate_cols = {
    "homepage": 255,
    "original_title": 255,
    "overview": 4000,
    "poster_path": 255,
    "status": 50,
    "title": 255,
    "tagline": 1000
}

for col, max_len in truncate_cols.items():
    if col in df.columns:
        df[col] = df[col].astype(str).str.slice(0, max_len)



# --------------------------------------------------------
# Save cleaned movies metadata as UTF-16
# --------------------------------------------------------
df.to_csv(
    r"C:\Users\hp\Desktop\sql-ultimate-course\me\archive\movies_metadata_clean_utf16.csv",
    index=False,
    encoding="utf-16",
    sep=","
)

print("Saved movies_metadata_clean_utf16.csv ✔")
print("ALL CLEANING DONE SUCCESSFULLY 🎉")

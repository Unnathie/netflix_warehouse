# 📦 **Bronze Layer – Raw Data Ingestion**

This folder contains all scripts required to ingest **raw movie datasets** into the *Bronze Layer* of the data warehouse.

The Bronze layer stores **raw, untransformed data**, exactly as it arrives from the source files[DB: https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset].
It acts as the foundation for Silver (cleaned) and Gold (analytics) layers.

---

## 🔗 **1. What this folder contains**

### **✓ Python Cleaning Scripts**

* Convert messy CSVs into SQL-compatible UTF-16
* Fix broken JSON-like fields
* Clean malformed numbers, dates
* Remove NaN/nulls
* Produce cleaned files ready for BULK INSERT

### **✓ SQL Server DDL Scripts**

* Creates all `nbronze.*` tables
* Uses `NVARCHAR(MAX)` to store raw, unstructured data
* Ensures no typing errors during ingestion

### **✓ Stored Procedure (Main Pipeline)**

`load_net_bronze.sql`
Automatically:

1. Truncates Bronze tables
2. Bulk inserts data from cleaned CSV files
3. Measures time taken for each load
4. Prints step-by-step logs
5. Catches and prints SQL errors

This mimics how real data engineering pipelines load RAW data.

---

## 📂 **2. Bronze Layer Tables**

These tables store the raw TMDB datasets:

| Table                    | Description                                                       |
| ------------------------ | ----------------------------------------------------------------- |
| `nbronze.movie_metadata` | Raw metadata (JSON-like columns, strings, dates, budget, revenue) |
| `nbronze.credits`        | Raw cast & crew JSON                                              |
| `nbronze.keywords`       | Raw keywords JSON                                                 |
| `nbronze.ratings`        | Full ratings dataset                                              |
| `nbronze.ratings_small`  | Smaller ratings sample                                            |
| `nbronze.links`          | IMDB/TMDB mapping                                                 |
| `nbronze.links_small`    | Smaller mapping dataset                                           |

All columns are intentionally **strings** (NVARCHAR MAX) to avoid load failures.

Typed columns come later in the **Silver layer**.

---

## ⚙️ **3. How to Run the Bronze Load**

### **Step 1 — Clean source files using Python**

Run:

```
python clean_raw_files.py
```

This generates cleaned UTF-16 CSVs, required for SQL Server.

### **Step 2 — Execute the stored procedure**

In SQL Server Management Studio:

```sql
EXEC nbronze.load_net_bronze;
```

You will see logs like:

```
>> TRUNCATING nbronze.ratings_small
>> INSERTING nbronze.ratings_small
Time taken: 1 seconds
...
Time taken to load bronze layer is: 27 seconds
```

---

## 🌟 **4. Why Bronze Layer Is Important**

* Ensures raw data is preserved exactly as-is
* Handles messy or inconsistent files safely
* Prevents schema drift errors
* Supports reprocessing the data anytime
* Keeps ETL pipeline reproducible

Silver and Gold layers depend on this stable foundation.

---

## 🛠 **5. Requirements**

Create a `requirements.txt` in root:

```
pandas
numpy
```

SQL Server needs:

* BULK INSERT enabled
* Access to file paths
* UTF-16 support


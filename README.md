# netflix_warehouse
```
# 🎬 Netflix Medallion Data Warehouse (SQL Server)

This project implements a complete **Medallion Architecture (Bronze → Silver → Gold)** using **SQL Server** on the Netflix Movies & Ratings dataset.  
It focuses on **real-world data ingestion, cleaning, JSON handling, and dimensional modeling for BI**.

---

## 🏗 Architecture

```

Source Data → Bronze → Silver → Gold → BI (Tableau / Power BI)

```

---

## 📁 Layers Overview

### 🥉 Bronze Layer (Raw Data)
Stores unprocessed data from:
- CSV files (ratings, links)
- Database tables (movies, credits, keywords)

**Stored Procedure:**
- `nbronze.load_net_bronze`  
Handles full raw data loads using `BULK INSERT` and direct table inserts.

---

### 🥈 Silver Layer (Cleaned Data)
Performs:
- Data type conversions
- NULL handling and validation
- Boolean normalization
- Date and numeric cleansing
- Surrogate key generation
- JSON format validation

**Tables:**
- Movies, Ratings, Links, Credits, Keywords

**Stored Procedure:**
- `nsilver.load_net_silver`  
Cleans and reloads all Silver tables with logging and error handling.

---

### 🥇 Gold Layer (Analytics / Star Schema)
Built fully using **SQL Views**.

**Dimensions:**
- `ngold.dim_movies`
- `ngold.dim_user`
- `ngold.dim_genre`
- `ngold.dim_prod_company`
- `ngold.dim_country`
- `ngold.dim_language`

**Facts:**
- `ngold.fact_ratings`
- `ngold.fact_movie`
- `ngold.fact_genre`
- `ngold.fact_prod_company`
- `ngold.fact_country`
- `ngold.fact_language`

---

## 🧩 JSON Handling
JSON-like columns were parsed using:

```

OPENJSON(REPLACE(column, '''', '"'))

```

Invalid JSON rows were filtered using:

```

ISJSON(REPLACE(column, '''', '"')) = 1

```

This was used for:
- Genres
- Production companies
- Production countries
- Spoken languages

---

## 🎯 Key Skills Demonstrated
- SQL Data Warehousing
- Medallion Architecture
- Stored Procedures
- Data Cleaning & Validation
- JSON Parsing with OPENJSON
- Star Schema Design
- Fact & Dimension Modeling
- UNIX Timestamp Conversion
- BI-Ready Data Modeling

---

## 📌 Next Step
- Tableau dashboard development using Gold layer views.

---

## 👩‍💻 Author
**Unnathi E Naik**  
Data Analyst | SQL | Data Warehousing
```

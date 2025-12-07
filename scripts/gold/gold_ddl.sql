----------------------------dim_movie-----------------------------
IF OBJECT_ID('ngold.dim_movies','V') IS NOT NULL
    DROP VIEW ngold.dim_movies
GO
CREATE VIEW ngold.dim_movies AS
SELECT
    movie_id,
    id,
    imdb_id,
    title,
    original_title,
    original_language,
    adult,
    video,
    status,
    status_groups,
    YEAR(release_date) AS release_year,
    homepage,
    poster_path,
    tagline,
    overview
FROM nsilver.movies;
GO
-------------------DIM_USERS---------------------------
IF OBJECT_ID('ngold.dim_user','V') IS NOT NULL
    DROP VIEW ngold.dim_user
GO
CREATE VIEW ngold.dim_user AS
SELECT 
       userId,
       COUNT(*) ratings,
       ROUND(AVG(rating),2) average_rating,
       MIN(DATEADD(SECOND, timestamp, '1970-01-01')) AS earliest,
       MAX(DATEADD(SECOND, timestamp, '1970-01-01')) AS latest
  FROM nsilver.ratings
  GROUP BY userId
  GO
  /************************************************************************************
Purpose:  Create a reusable extraction view and two Gold views for genres.
Author:   Unnathi E Naik 
Notes:
  - vw_genre_extracted: a single reusable view that parses the JSON-like `genres`
      column in nsilver.movies and returns one row per (movie_id, genre).
  - dim_genre: distinct list of genres (dimension).
  - fact_genre: bridge/fact mapping between movies and genres.
  - I replace single quotes with double quotes because the source uses single quotes
      and OPENJSON expects valid JSON (double quotes).
  - I restrict to a curated list(gotten from testing DISTINCT) of English genre names in the WHERE clause to
      remove noisy / irrelevant values.
************************************************************************************/

-- =========================
-- 1) DROP + CREATE helper view: ngold.vw_genre_extracted
--    This view parses the genres JSON and returns movie_id, genre_id, genre_name.
-- =========================

-- If a view with this name already exists, drop it so I can recreate it cleanly.
IF OBJECT_ID('ngold.vw_genre_extracted','V') IS NOT NULL
    DROP VIEW ngold.vw_genre_extracted;
GO  -- GO ends the batch so the DROP is executed before the CREATE.

-- Create the view that extracts genres from the raw JSON-like string.
CREATE VIEW ngold.vw_genre_extracted AS
SELECT 
    m.movie_id,                    -- the surrogate movie_id from the silver table (keeps relationship to the movie)
    j.[id]   AS genre_id,          -- the genre id parsed from the JSON element (numeric)
    j.[name] AS genre_name         -- the genre name parsed from the JSON element (string)
FROM nsilver.movies m             -- source table (silver layer) containing raw movie metadata
CROSS APPLY 
    -- OPENJSON expects valid JSON. The source uses single quotes so I replace them with double quotes.
    -- REPLACE(m.genres, '''', '"') converts single quotes (') into double quotes (")
    -- Example transformed string: [{'id':18,'name':'Drama'}] -> [{"id":18,"name":"Drama"}]
    OPENJSON(REPLACE(m.genres, '''', '
    "'))
    -- The WITH clause projects each JSON object into typed columns.
    WITH (
        [id]   INT           '$.id',     -- map the JSON property $.id to a column named id of type INT
        [name] NVARCHAR(200) '$.name'    -- map the JSON property $.name to a column named name of type NVARCHAR
    ) j                              -- alias the OPENJSON result as j, so I can reference j.[id] and j.[name]
WHERE m.genres IS NOT NULL          -- only attempt to parse rows that actually have a genres value
  AND j.[name] IN (                 -- filter to a curated list of clean, standard English genre names
        'Action','Adventure','Animation','Comedy','Crime','Documentary','Drama',
        'Family','Fantasy','Horror','Music','Mystery','Romance',
        'Science Fiction','Thriller'
  );
GO  -- complete definition of vw_genre_extracted

-- =========================
-- 2) DROP + CREATE dim_genre (distinct genre list)
--    This view returns one row per genre_id + genre_name (a proper dimension).
-- =========================

-- Remove the dimension view if already present so I can recreate it without errors.
IF OBJECT_ID('ngold.dim_genre','V') IS NOT NULL
    DROP VIEW ngold.dim_genre;
GO

-- Create the dimension view using the helper view as the source.
CREATE VIEW ngold.dim_genre AS
SELECT DISTINCT
    genre_id,      -- integer id of the genre (unique per genre)
    genre_name     -- human-readable name of the genre (trimmed as needed by consumers)
FROM ngold.vw_genre_extracted;  -- reuse the parsed data so extraction logic lives only in one place
GO

-- =========================
-- 3) DROP + CREATE fact_genre (bridge table: movie_id <-> genre_id)
--    This view returns the many-to-many mapping between movies and genres.
-- =========================

-- Drop if exists so I can create fresh.
IF OBJECT_ID('ngold.fact_genre','V') IS NOT NULL
    DROP VIEW ngold.fact_genre;
GO

-- Create the fact/bridge view using the same helper view.
CREATE VIEW ngold.fact_genre AS
SELECT 
    movie_id,     -- the movie identifier (surrogate from silver)
    genre_id      -- the genre identifier (foreign key to dim_genre)
FROM ngold.vw_genre_extracted;  -- reuse the parsed rows to produce the mapping
GO

/************************************************************************************
Additional notes / rationale (readable comments — not executed):
- I created a single extraction view (vw_genre_extracted) to avoid duplicating the
  OPENJSON parsing logic in multiple places. Views are metadata (no storage),
  so this is maintenance-friendly and efficient.
- The REPLACE(..., '''', '"') call is necessary because source column uses
  single quotes; after this replacement OPENJSON can parse the array of objects.
- The WITH clause in OPENJSON allows us to project JSON properties into typed columns,
  which is safer and faster than parsing raw strings.
- The genre filter (j.[name] IN (...)) is optional but recommended if you want
  to keep a clean set of genre values for reporting/BI. Remove or adjust the list
  if you want more genres or to include foreign-language labels.
- If some source rows contain invalid JSON even after replace, consider wrapping
  the OPENJSON call behind a guard like: WHERE ISJSON(REPLACE(m.genres, '''','"')) = 1
  to avoid runtime errors on malformed rows. (Not included above because data
  looked consistent after the simple replace.)
************************************************************************************/
--------------------------------FACT_RATINGS------------------------------------
GO
IF OBJECT_ID('ngold.fact_ratings','V') IS NOT NULL 
		DROP VIEW ngold.fact_ratings 
GO 
CREATE VIEW ngold.fact_ratings AS
SELECT
rating_id,
userId,
movieId,
rating,
DATEADD(SECOND,timestamp,'1970-01-01') AS rating_date
FROM
nsilver.ratings
GO
-------------------------FACT_MOVIE--------------------------------------
IF OBJECT_ID('ngold.fact_movie','V') IS NOT NULL
	DROP VIEW  ngold.fact_movie
GO
CREATE VIEW ngold.fact_movie AS
SELECT
movieId,
COUNT(*) AS total_rating,
ROUND(AVG(rating),2) AS average_rating,
MIN(DATEADD(SECOND,timestamp,'1970-01-01')) AS earliest_rating,
MAX(DATEADD(SECOND,timestamp,'1970-01-01')) AS latest_rating
FROM
nsilver.ratings
GROUP BY movieId;
----------------------------company----------------------------------------
GO
IF OBJECT_ID('ngold.vw_company_extracted','V') IS NOT NULL
    DROP VIEW ngold.vw_company_extracted;
GO

CREATE VIEW ngold.vw_company_extracted AS
SELECT
    m.movie_id,
    j.id   AS company_id,
    j.name AS company_name
FROM nsilver.movies m
CROSS APPLY OPENJSON(REPLACE(m.production_companies, '''', '"'))
WITH (
    id   INT           '$.id',
    name NVARCHAR(200) '$.name'
) j
WHERE 
    m.production_companies IS NOT NULL
    AND ISJSON(REPLACE(m.production_companies, '''', '"')) = 1;
--In this version of sql there is no try_openjson so i just filtered them
GO
IF OBJECT_ID('ngold.dim_prod_company','V') IS NOT NULL
    DROP VIEW ngold.dim_prod_company;
GO

CREATE VIEW ngold.dim_prod_company AS
SELECT DISTINCT
    company_id,
    company_name
FROM ngold.vw_company_extracted;
GO
IF OBJECT_ID('ngold.fact_prod_company','V') IS NOT NULL
    DROP VIEW ngold.fact_prod_company;
GO

CREATE VIEW ngold.fact_prod_company AS
SELECT
    movie_id,
    company_id
FROM ngold.vw_company_extracted;
----------------------------country------------------------------
GO
IF OBJECT_ID('ngold.vw_country_extracted') IS NOT NULL
		DROP VIEW ngold.vw_country_extracted
GO
CREATE VIEW ngold.vw_country_extracted AS 
SELECT
m.movie_id,
j.iso_3166_1 ,
j.name
FROM
nsilver.movies m
CROSS APPLY
OPENJSON(REPLACE(m.production_countries,'''','"'))
WITH(
	[iso_3166_1] NVARCHAR(2) '$.iso_3166_1',
	[name] NVARCHAR(200) '$.name')j
WHERE production_countries IS NOT NULL and ISJSON(REPLACE(production_countries,'''','"'))=1

GO
IF OBJECT_ID('ngold.dim_country') IS NOT NULL
		DROP VIEW ngold.dim_country
GO
CREATE VIEW ngold.dim_country AS
SELECT
DISTINCT
iso_3166_1,
name
FROM
ngold.vw_country_extracted
GO
IF OBJECT_ID('ngold.fact_country') IS NOT NULL
		DROP VIEW ngold.fact_country
GO
CREATE VIEW ngold.fact_country AS
SELECT

movie_id,
iso_3166_1
FROM
ngold.vw_country_extracted


/*===============================================================
    BRONZE LAYER – RAW INGESTION TABLES
    -------------------------------------------------------------
    This layer stores the raw data exactly as it comes from the
    source files (after minimal cleaning). No transformations,
    no typing, no constraints except basic structure.

    Your CSVs contain messy JSON strings, inconsistent numbers,
    missing fields, etc. → So NVARCHAR(MAX) is the safest choice
    to preserve raw data and avoid BULK INSERT failures.
===============================================================*/

-------------------------------
-- MOVIE METADATA (RAW)
-------------------------------
IF OBJECT_ID('nbronze.movie_metadata') IS NOT NULL
    DROP TABLE nbronze.movie_metadata;
GO

CREATE TABLE nbronze.movie_metadata(
    adult                  NVARCHAR(MAX),   -- 'true'/'false'
    belongs_to_collection  NVARCHAR(MAX),   -- messy JSON-like text
    budget                 NVARCHAR(MAX),   -- numeric but messy → keep raw
    genres                 NVARCHAR(MAX),   -- JSON list of genres
    homepage               NVARCHAR(MAX),
    id                     NVARCHAR(MAX),   -- TMDB movie ID stored as text
    imdb_id                NVARCHAR(MAX),
    original_language      NVARCHAR(MAX),
    original_title         NVARCHAR(MAX),
    overview               NVARCHAR(MAX),
    popularity             NVARCHAR(MAX),
    poster_path            NVARCHAR(MAX),
    production_companies   NVARCHAR(MAX),   -- JSON list
    production_countries   NVARCHAR(MAX),   -- JSON list
    release_date           NVARCHAR(MAX),   -- raw date string
    revenue                NVARCHAR(MAX),
    runtime                NVARCHAR(MAX),
    spoken_languages       NVARCHAR(MAX),   -- JSON list
    status                 NVARCHAR(MAX),
    tagline                NVARCHAR(MAX),
    title                  NVARCHAR(MAX),
    video                  NVARCHAR(MAX),
    vote_average           NVARCHAR(MAX),
    vote_count             NVARCHAR(MAX)
);


-------------------------------
-- RATINGS SMALL FILE
-------------------------------
IF OBJECT_ID('nbronze.ratings_small') IS NOT NULL
    DROP TABLE nbronze.ratings_small;
GO

CREATE TABLE nbronze.ratings_small(
    userId      INT,
    movieId     INT,
    rating      FLOAT,
    timestamp   NVARCHAR(50)   -- timestamp stored as text in raw file
);


-------------------------------
-- DUPLICATE? (Maybe remove later)
-- nbronze.small_ratings (Same structure)
-------------------------------
IF OBJECT_ID('nbronze.small_ratings') IS NOT NULL
    DROP TABLE nbronze.small_ratings;
GO

CREATE TABLE nbronze.small_ratings(
    userId      INT,
    movieId     INT,
    rating      FLOAT,
    timestamp   NVARCHAR(50)
);


-------------------------------
-- LINKS SMALL
-------------------------------
IF OBJECT_ID('nbronze.links_small') IS NOT NULL
    DROP TABLE nbronze.links_small;
GO

CREATE TABLE nbronze.links_small(
    movieId     INT,
    imdbId      INT,
    tmdbId      INT
);


-------------------------------
-- FULL LINKS TABLE
-------------------------------
IF OBJECT_ID('nbronze.links') IS NOT NULL
    DROP TABLE nbronze.links;
GO

CREATE TABLE nbronze.links(
    movieId     INT,
    imdbId      INT,
    tmdbId      INT
);


-------------------------------
-- KEYWORDS
-------------------------------
IF OBJECT_ID('nbronze.keywords') IS NOT NULL
    DROP TABLE nbronze.keywords;
GO

CREATE TABLE nbronze.keywords(
    id          INT,
    keywords    NVARCHAR(MAX)    -- JSON-like list of keywords
);


-------------------------------
-- CREDITS (CAST + CREW)
-------------------------------
IF OBJECT_ID('nbronze.credits') IS NOT NULL
    DROP TABLE nbronze.credits;
GO

CREATE TABLE nbronze.credits(
    cast        NVARCHAR(MAX),   -- large JSON list
    crew        NVARCHAR(MAX),   -- large JSON list
    id          NVARCHAR(MAX)    -- movie ID as text to avoid load errors
);

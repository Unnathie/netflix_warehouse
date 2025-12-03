/*
  Silver Layer DDL for Movie Analytics Platform
  ---------------------------------------------
  This script defines the Silver layer tables used in the Medallion architecture.
  The Silver layer contains cleaned and standardized data extracted from the
  Bronze layer (raw files from TMDB and MovieLens).

  Key Characteristics:
  - Data types are standardized (INT, FLOAT, BIT, NVARCHAR, DATE, etc.)
  - JSON-like fields (genres, cast, crew, keywords) are preserved as NVARCHAR(MAX)
  - No primary keys are enforced on columns that may contain duplicates in Bronze
  - Surrogate keys (e.g., movie_id) are created for internal consistency
  - This layer prepares the data for downstream Gold modeling (facts/dimensions)

  This DDL is fully compatible with the Silver loading Stored Procedure.
*/

IF OBJECT_ID('nsilver.movies') IS NOT NULL
    DROP TABLE nsilver.movies;
GO

CREATE TABLE nsilver.movies (
    movie_id              INT PRIMARY KEY,
    id                    INT,
    imdb_id               NVARCHAR(50),

    title                 NVARCHAR(400),
    original_title        NVARCHAR(400),
    original_language     NVARCHAR(10),

    adult                 BIT,
    video                 BIT,

    status                NVARCHAR(50),
    status_groups         NVARCHAR(50),

    release_date          DATE,
    runtime               INT,

    budget                BIGINT,
    revenue               BIGINT,
    popularity            FLOAT,
    vote_average          FLOAT,
    vote_count            INT,

    homepage              NVARCHAR(255),
    poster_path           NVARCHAR(255),
    tagline               NVARCHAR(400),
    overview              NVARCHAR(MAX),
    spoken_languages      NVARCHAR(MAX),
    genres                NVARCHAR(MAX),
    production_companies  NVARCHAR(MAX),
    production_countries  NVARCHAR(MAX)

);
IF OBJECT_ID('nsilver.ratings') IS NOT NULL 
		DROP TABLE nsilver.ratings
GO
CREATE TABLE nsilver.ratings(
        rating_id   INT,
		userId		INT,
		movieId		INT,
		rating		FLOAT,
		timestamp	BIGINT);

-- can skip ratings small as its just a subset of ratings
IF OBJECT_ID('nsilver.links')IS NOT NULL
		DROP TABLE nsilver.links
GO 
CREATE TABLE nsilver.links(
		movieId		INT PRIMARY KEY,
		imdbId		INT,
		tmdbId		INT

)

IF OBJECT_ID('nsilver.credits') IS NOT NULL
        DROP TABLE nsilver.credits
GO
CREATE TABLE nsilver.credits(
        cast            NVARCHAR(MAX),
        crew            NVARCHAR(MAX),
        id              INT)
---------------------Keywords---------------
IF OBJECT_ID('nsilver.keywords') IS NOT NULL
        DROP TABLE nsilver.keywords
GO
CREATE TABLE nsilver.keywords(
        id          INT,
        keywords    NVARCHAR(MAX))

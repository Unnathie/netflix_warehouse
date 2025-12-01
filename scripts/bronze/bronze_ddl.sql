/*
    File: Bronze Layer DDL (nbronze schema)
    ---------------------------------------

    Purpose:
      Defines the raw landing tables for the Bronze layer in the
      Medallion Architecture (Bronze → Silver → Gold).

      The Bronze layer stores data exactly as it appears in the source
      system (CSV files or external DB tables). No cleaning, validation,
      or transformations are applied here.

    Responsibilities:
      - Create raw tables to store unprocessed datasets:
          • movies_metadata
          • ratings_small (test subset)
          • ratings
          • links_small
          • links
          • keywords
          • credits
      - Preserve original data types (mostly NVARCHAR(MAX))
      - Mirror the source structure 1:1 for reproducibility
      - Ensure idempotent table creation using DROP + CREATE

    Design Notes:
      - JSON-like fields are stored as NVARCHAR(MAX)
      - No primary keys or constraints (Bronze must allow dirty data)
      - All numeric fields in movies_metadata are NVARCHAR(MAX)
        because incoming raw CSV values may be malformed
      - Bronze is meant to be the *source of truth* for downstream
        Silver cleansing

    Author: Unnathi E Naik
    Layer: Bronze
    Status: Production-ready
*/

IF OBJECT_ID('nbronze.movie_metadata') IS NOT NULL
		DROP TABLE nbronze.movie_metadata
GO
CREATE TABLE nbronze.movie_metadata(
		adult                  NVARCHAR(MAX),
		belongs_to_collection  NVARCHAR(MAX),
		budget				   NVARCHAR(MAX),
		genres				   NVARCHAR(MAX),
		homepage			   NVARCHAR(MAX),
		id					   NVARCHAR(MAX),
		imdb_id				   NVARCHAR(MAX),
		original_language	   NVARCHAR(MAX),
		original_title         NVARCHAR(MAX),
		overview			   NVARCHAR(MAX),
		popularity			   NVARCHAR(MAX),
		poster_path			   NVARCHAR(MAX),
		production_companies   NVARCHAR(MAX),
		production_countries   NVARCHAR(MAX),
		release_date		   NVARCHAR(MAX),
		revenue				   NVARCHAR(MAX),
		runtime                NVARCHAR(MAX),
		spoken_languages	   NVARCHAR(MAX),
		status				   NVARCHAR(MAX),
		tagline				   NVARCHAR(MAX),
		title				   NVARCHAR(MAX),
		video				   NVARCHAR(MAX),
		vote_average		   NVARCHAR(MAX),
		vote_count		       NVARCHAR(MAX)
)
--I directly loaded(movies_metadata,keywords,credits) it to DB
IF OBJECT_ID('nbronze.ratings_small') IS NOT NULL 
		DROP TABLE nbronze.ratings_small
GO
CREATE TABLE nbronze.ratings_small(
		userId		INT,
		movieId		INT,
		rating		FLOAT,
		timestamp	NVARCHAR(50)

)
IF OBJECT_ID('nbronze.ratings') IS NOT NULL 
		DROP TABLE nbronze.ratings
GO
CREATE TABLE nbronze.ratings(
		userId		INT,
		movieId		INT,
		rating		FLOAT,
		timestamp	NVARCHAR(50)
)
IF OBJECT_ID('nbronze.links_small')IS NOT NULL
		DROP TABLE nbronze.links_small
GO 
CREATE TABLE nbronze.links_small(
		movieId		INT,
		imdbId		INT,
		tmdbId		INT

)
IF OBJECT_ID('nbronze.links')IS NOT NULL
		DROP TABLE nbronze.links
GO 
CREATE TABLE nbronze.links(
		movieId		INT,
		imdbId		INT,
		tmdbId		INT

)

IF OBJECT_ID('nbronze.keywords')IS NOT NULL
		DROP TABLE nbronze.keywords
GO 
CREATE TABLE nbronze.keywords(
		id   		INT,
		keywords	NVARCHAR(MAX)

)
IF OBJECT_ID('nbronze.credits')IS NOT NULL
		DROP TABLE nbronze.credits
GO 
CREATE TABLE nbronze.credits(
		cast		NVARCHAR(MAX),
		crew		NVARCHAR(MAX),
		id   		NVARCHAR(MAX)
)

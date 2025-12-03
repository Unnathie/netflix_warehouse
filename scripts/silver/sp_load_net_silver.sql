/*
    Stored Procedure: nsilver.load_net_silver
    -----------------------------------------
    Purpose:
      Loads the Silver layer as part of the Medallion Architecture:
      Bronze -> Silver -> Gold.

      The Silver layer contains cleaned, validated, and standardized data
      extracted from the raw Bronze layer. It prepares the data for further
      transformation in the Gold layer without altering the original structure.

    Responsibilities:
      - Truncate and reload Silver tables (idempotent loads)
      - Validate and clean movie metadata (type casting + NULL handling)
      - Normalize booleans, dates, numeric fields
      - Validate JSON-like string arrays (genres, cast, crew, keywords)
      - Generate surrogate keys (movie_id, rating_id)
      - Categorize movie status into status_groups
      - Insert cleaned ratings, links, credits, and keywords
      - Log the execution time for each table load

    Design Notes:
      - The Silver layer intentionally preserves duplicates found in Bronze.
      - No primary keys are enforced on non-unique Bronze IDs.
      - NVARCHAR(MAX) is used for JSON-like columns.
      - This procedure is optimized for batch reloads.

    Author: Unnathi E Naik
    Layer: Silver
    Status: Production-ready
*/

CREATE OR ALTER PROCEDURE 
nsilver.load_net_silver AS
BEGIN
    BEGIN TRY
            PRINT '---------------------';
            PRINT 'LOADING SILVER LAYER';
            PRINT '---------------------';
            DECLARE @start_time DATETIME ,@end_time DATETIME, @start_silver DATETIME, @end_silver DATETIME
            SET @start_time =GETDATE()
            SET @start_silver=GETDATE()
            PRINT '>>>>>>>TRUNCATING TABLE nsilver.movies';
            TRUNCATE TABLE nsilver.movies
            PRINT 'INSERTING INTO TABLE nsilver.movies';
            INSERT INTO nsilver.movies (
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
                release_date,
                runtime,
                budget,
                revenue,
                popularity,
                vote_average,
                vote_count,
                homepage,
                poster_path,
                tagline,
                overview,
                spoken_languages,
                genres,
                production_companies,
                production_countries
    
            )
        
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY TRY_CAST(id AS INT)
                ) AS movie_id,

                CASE WHEN TRY_CAST(id AS INT) BETWEEN 1 AND 20000000 
                     THEN TRY_CAST(id AS INT)
                     ELSE NULL
                END AS id,


                CASE 
                     WHEN imdb_id LIKE 'tt%' 
                          AND ISNUMERIC(RIGHT(TRIM(imdb_id), 7)) = 1
                          AND LEN(TRIM(imdb_id)) BETWEEN 9 AND 10
                     THEN TRIM(imdb_id)
                     ELSE NULL
                END AS imdb_id,
                CASE
                    WHEN  TRIM(title) NOT  IN ('N/A', 'Unknown', 'nan') --unknown is present
                    AND LEN(TRIM(title))<300
                    THEN TRIM(title)
                    ELSE NULL
                END AS title,
            /*CAN check for '/%' and others if you find it you can have that condition 
             there are different lang titles so can't use LIKE '%A-Z%' 
             there are single chars too which are legit movie title so cant use a boundry for shorter titles*/

                CASE
                 WHEN  TRIM(original_title) NOT  IN ('N/A', 'Unknown', 'nan') 
                    AND LEN(TRIM(original_title))<300
                 THEN TRIM(original_title)
                 ELSE NULL
            END AS original_title,

                CASE 
                WHEN LEN(TRIM(original_language)) = 2 
                     AND TRIM(original_language) NOT IN ('','unknown','nan')
                     AND TRIM(original_language) NOT LIKE '%[^A-Za-z]%'

                THEN LOWER(TRIM(original_language))
                ELSE NULL
            END AS original_language,

                CASE TRIM(LOWER(adult))
                    WHEN 'true' THEN 1
                    WHEN 'false' THEN 0
                    ELSE NULL
                END AS adult,
 
                CASE TRIM(LOWER(video))
                    WHEN 'true' THEN 1
                    WHEN 'false' THEN 0
                    ELSE NULL
                END AS video,
                TRIM(LOWER(status)) AS status,
            --You can keep this as it is -cause it has valid information through out
                CASE 
                WHEN LOWER(TRIM(status)) = 'released' THEN 'released'
                WHEN LOWER(TRIM(status)) IN ('in production','post production','planned','rumored') 
                     THEN 'active'
                WHEN LOWER(TRIM(status)) = 'canceled' THEN 'inactive'
                ELSE NULL
            END AS status_groups,
                TRY_CAST(release_date AS DATE) AS release_date,
    
               CASE
                    WHEN TRY_CAST(runtime AS INT) BETWEEN 1 AND 600
                    THEN TRY_CAST(runtime AS INT)
                    ELSE NULL
                END AS runtime
            ,
                CASE 
                    WHEN TRY_CAST(budget AS BIGINT)>1
                    THEN TRY_CAST(budget AS BIGINT)
                    ELSE NULL
                END AS budget,
                CASE 
                    WHEN TRY_CAST(revenue AS BIGINT) IS NOT NULL 
                        AND TRY_CAST(revenue AS BIGINT)>1
                    THEN TRY_CAST(revenue AS BIGINT)
                    ELSE NULL
                END AS revenue
            ,
             TRY_CAST(popularity AS FLOAT) AS popularity,
            --If you want to have a boundry you can add 
                CASE 
                    WHEN TRY_CAST(vote_average AS FLOAT) BETWEEN 0 AND 10
                    THEN ROUND(TRY_CAST(vote_average AS FLOAT),1)
                    ELSE NULL
                END AS vote_average,

                CASE 
                    WHEN TRY_CAST(vote_count AS INT)>=0
                    THEN TRY_CAST(vote_count AS INT)
                    ELSE NULL
                END AS vote_count,

                CASE
                    WHEN ((TRIM(homepage) LIKE 'http%' OR TRIM(homepage) LIKE 'www%' )
                    AND TRIM(homepage) LIKE '%.%')
                    THEN LEFT(TRIM(homepage),255)
                    ELSE NULL
                END AS homepage,
            -- this dataset doesnt have www but you could use it for better analysis
                CASE
                    WHEN (TRIM(poster_path) LIKE '/%.jpg'
                       OR TRIM(poster_path) LIKE '/%.jpeg'
                       OR TRIM(poster_path) LIKE '/%.png')
                    THEN LEFT(TRIM(poster_path),255)
                    ELSE NULL
                END AS poster_path,

                CASE
                    WHEN TRIM(LOWER(tagline)) NOT IN ('','nan','-') 
                         AND TRIM(UPPER(tagline))  LIKE '%[A-Z]%' 
			            AND LEN(TRIM(LOWER(tagline)))  BETWEEN 3 AND 300
                    THEN TRIM(tagline)
                    ELSE NULL
                END AS tagline,
            /*You can keep other lang taglines but from what I analyzed most of
            them were actually title and not taglines so i used eng taglines only*/
                CASE
                WHEN LEN(TRIM(overview)) BETWEEN 10 AND 4000
                THEN 
                    TRIM(overview)
                ELSE 
                    NULL
                END AS overview,
            --You can just take in english overviews by doing LIKE '%A-Z%' since i did take in other lang in title and some more columns
                CASE
                    WHEN TRIM(spoken_languages) LIKE '[[{]%' 
                         AND TRIM(spoken_languages) LIKE '%[}]]'
                    THEN TRIM(spoken_languages)
                    ELSE NULL
                END AS spoken_languages,
             
                CASE
                    WHEN TRIM(genres) LIKE '[[{]%' 
                         AND TRIM(genres) LIKE '%[}]]'
                    THEN TRIM(genres)
                    ELSE NULL
                END AS genres,
                CASE
                WHEN TRIM(production_companies) LIKE '[[{]%' 
                     AND TRIM(production_companies) LIKE '%[}]]'
                THEN TRIM(production_companies)
                ELSE NULL
                END AS production_companies,
                CASE
                WHEN TRIM(production_countries) LIKE '[[{]%' 
                     AND TRIM(production_countries) LIKE '%[}]]'
                THEN TRIM(production_countries)
                ELSE NULL
                END AS production_countries



                FROM
                nbronze.movie_metadata;
              SET @end_time=GETDATE()
              PRINT 'Time taken to insert is '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds';

            ----------------ratings-------------------
            SET @start_time=GETDATE()
            PRINT'>>>>>>>>>>TRUCATING TABLE nsilver.ratings';
            TRUNCATE TABLE nsilver.ratings
            PRINT '>>>>>>>>>>>INSERTING INTO nsilver.ratings';
        
              INSERT INTO nsilver.ratings(
                rating_id,
                userId,
                movieId,
                rating,
                timestamp
            )
            SELECT
                ROW_NUMBER() OVER(ORDER BY userId, movieId) AS rating_id,
                userId,
                movieId,
                rating,
                TRY_CAST(timestamp AS BIGINT) AS timestamp
            FROM nbronze.ratings;
            SET @end_time=GETDATE()
            PRINT 'Time taken to insert is '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
            -------------LINKS--------------
            SET @start_time=GETDATE()
            PRINT '>>>>>>>>>TRUNCATING TABLE nsilver.links';
            TRUNCATE TABLE nsilver.links
            PRINT '>>>>>>>>>INSERTING INTO TABLE nsilver.links';

            INSERT INTO nsilver.links (
                movieId,
                imdbId,
                tmdbId
            )
            SELECT 
                movieId,
                imdbId,
                tmdbId
            FROM nbronze.links;
            SET @end_time=GETDATE()
            PRINT 'Time taken to insert is '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)  + ' Seconds';

            -----------------Credits---------------
            SET @start_time=GETDATE()
            PRINT'>>>>>>>>>>>>TRUNCATING TABLE nsilver.credits';
            TRUNCATE TABLE  nsilver.credits
            PRINT '>>>>>>>>>>>INSERTING INTO TABLE nsilver.credits';
            INSERT INTO nsilver.credits(
                cast,
                crew,
                id)
            SELECT
                CASE
                WHEN TRIM(cast) LIKE '[[{]%' 
                AND TRIM(cast) LIKE '%[}]]'THEN TRIM(cast)
                ELSE NULL
                END AS cast,
                CASE
                WHEN TRIM(crew) LIKE '[[{]%' 
                AND TRIM(crew) LIKE '%[}]]'THEN TRIM(crew)
                ELSE NULL
                END AS crew,
                CASE WHEN TRY_CAST(id AS INT) BETWEEN 1 AND 20000000 
                THEN TRY_CAST(id AS INT)
                ELSE NULL
                END AS id
            FROM
            nbronze.credits;
            SET @end_time=GETDATE()
            PRINT 'Time taken to insert is '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)  + ' Seconds';

            -------------------Keywords----------------
            SET @start_time=GETDATE()
            PRINT '>>>>>>>>>>TRUNCATING TABLE  nsilver.keywords';
            TRUNCATE TABLE nsilver.keywords
            PRINT '>>>>>>>>>>INSERTING TABLE nsilver.keywords';
            INSERT INTO nsilver.keywords(
                    id,
                    keywords)
            SELECT
            CASE 
            WHEN TRY_CAST(id AS INT) BETWEEN 1 AND 20000000
            THEN TRY_CAST(id AS INT)
            ELSE NULL
            END AS id,
            CASE WHEN TRIM(keywords) LIKE '[[{]%' 
                  AND TRIM(keywords) LIKE '%[}]]'
            THEN TRIM(keywords)
            ELSE NULL
            END AS keywords
            FROM
            nbronze.keywords;
            SET @end_time=GETDATE()
            SET @end_silver=GETDATE()
            PRINT 'Time taken to insert is '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)  + ' Seconds';
            PRINT 'Time taken to load silver layer is ' + CAST(DATEDIFF(MINUTE,@start_silver,@end_silver) AS NVARCHAR) + ' minutes';

    --there are 2 more tables but they are just a subset of ratings and links for testing which i would not need so i have not loaded
    END TRY
    BEGIN CATCH
        PRINT '*****ERROR OCCURED*****';
        PRINT 'ERROR MESSAGE: '+ ERROR_MESSAGE()
        PRINT 'ERROR NUMBER: '+ CAST(ERROR_NUMBER() AS NVARCHAR)
        PRINT 'ERROR STATE: '+ CAST(ERROR_STATE() AS NVARCHAR)
    END CATCH
END

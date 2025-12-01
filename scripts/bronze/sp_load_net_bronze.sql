/*
    Stored Procedure: nbronze.load_net_bronze
    -----------------------------------------
    Purpose:
      Loads the Bronze layer as the first stage of the Medallion Architecture:
      Raw Files → Bronze → Silver → Gold.

      The Bronze layer stores raw, minimally processed data exactly as it
      appears in the source files (CSV or external tables). No cleaning,
      transformations, or validation occurs here.

    Responsibilities:
      - Truncate and fully reload raw Bronze tables
      - Ingest CSVs using BULK INSERT for large datasets
      - Preserve original structure and data quality issues
      - Maintain idempotent reloads for consistent ETL runs
      - Log execution time for each dataset load

    Design Notes:
      - Bronze layer = raw landing zone (no business rules applied)
      - Duplicate IDs, malformed values, and inconsistencies are kept exactly as-is
      - Supports both BULK INSERT paths and direct SELECT FROM dbo.* tables
      - Ideal for archiving and reproducibility

    Included Datasets:
      - ratings_small.csv   (optional test subset)
      - ratings.csv         (full)
      - links_small.csv     (optional)
      - links.csv           (full)
      - keywords            (loaded from dbo.keywords)
      - credits             (loaded from dbo.credits)
      - movies_metadata     (loaded from dbo.movies_metadata)

    Author: Unnathi E Naik
    Layer: Bronze
    Status: Production-ready
*/

CREATE OR ALTER PROCEDURE nbronze.load_net_bronze AS
BEGIN
	BEGIN TRY 
		DECLARE @start_time DATETIME ,@end_time DATETIME ,@start_bronze DATETIME ,@end_bronze DATETIME
		PRINT '--------------------'
		PRINT 'LOADING BRONZE LAYER'
		PRINT '--------------------'
		SET @start_time = GETDATE()
		SET @start_bronze=GETDATE()
		PRINT '>>TRUNCATING  nbronze.ratings_small'
		TRUNCATE TABLE nbronze.ratings_small
		PRINT '>>INSERTING  nbronze.ratings_small'
		BULK INSERT nbronze.ratings_small
		FROM 'C:\Users\hp\Desktop\sql-ultimate-course\me\archive\ratings_small.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			ROWTERMINATOR='\n',
			TABLOCK
		)
		SET @end_time=GETDATE()
		PRINT 'Time taken to insert is: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds'
		SET @start_time=GETDATE()
		PRINT '>>TRUNCATING TABLE nbronze.ratings'
		TRUNCATE TABLE nbronze.ratings
		PRINT '>>INSERTING TABLE nbronze.ratings'
		BULK INSERT nbronze.ratings
		FROM 'C:\Users\hp\Desktop\sql-ultimate-course\me\archive\ratings.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			ROWTERMINATOR='\n',
			TABLOCK
		)
		SET @end_time=GETDATE()
		PRINT 'Time taken to insert is: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds'
		SET @start_time=GETDATE()
		PRINT '>>TRUNCATING  nbronze.links_small'
		TRUNCATE TABLE nbronze.links_small
		PRINT '>>INSERTING  nbronze.links_small'
		BULK INSERT nbronze.links_small
		FROM 'C:\Users\hp\Desktop\sql-ultimate-course\me\archive\links_small.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			ROWTERMINATOR='\n',
			TABLOCK
		)
		SET @end_time=GETDATE()
		PRINT 'Time taken to insert is: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds'
		SET @start_time=GETDATE()
		PRINT '>>TRUNCATING  nbronze.links'
		TRUNCATE TABLE nbronze.links
		PRINT '>>INSERTING  nbronze.links'
		BULK INSERT nbronze.links
		FROM 'C:\Users\hp\Desktop\sql-ultimate-course\me\archive\links.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			ROWTERMINATOR='\n',
			TABLOCK
		)
		SET @end_time=GETDATE()
		PRINT 'Time taken to insert is: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds'
		PRINT '>>TRUNCATING  nbronze.keywords'
		TRUNCATE TABLE nbronze.keywords
		PRINT '>>INSERTING  nbronze.keywords'
		TRUNCATE TABLE nbronze.keywords
		INSERT INTO nbronze.keywords(
			   id
			  ,keywords)
		SELECT
			   id
			  ,keywords
		FROM dbo.keywords
		SET @end_time=GETDATE()
		PRINT 'Time taken to insert is: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds'
		PRINT '>>TRUNCATING  nbronze.credits'
		TRUNCATE TABLE nbronze.credits
		PRINT '>>INSERTING  nbronze.credits'
		INSERT INTO nbronze.credits(
			   cast
			  ,crew
			  ,id)
		SELECT
			   cast
			  ,crew
			  ,id
		  FROM NET_DB.dbo.credits
		
		SET @end_time=GETDATE()
		PRINT 'Time taken to insert is: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds'
		PRINT '>>TRUNCATING  nbronze.movie_metadata'
		TRUNCATE TABLE nbronze.movie_metadata
		PRINT '>>INSERTING  nbronze.movie_metadata'
		INSERT INTO nbronze.movie_metadata(
		   adult
		  ,belongs_to_collection
		  ,budget
		  ,genres
		  ,homepage
		  ,id
		  ,imdb_id
		  ,original_language
		  ,original_title
		  ,overview
		  ,popularity
		  ,poster_path
		  ,production_companies
		  ,production_countries
		  ,release_date
		  ,revenue
		  ,runtime
		  ,spoken_languages
		  ,status
		  ,tagline
		  ,title
		  ,video
		  ,vote_average
		  ,vote_count)
		SELECT
		       adult
			  ,belongs_to_collection
			  ,budget
			  ,genres
			  ,homepage
			  ,id
			  ,imdb_id
			  ,original_language
			  ,original_title
			  ,overview
			  ,popularity
			  ,poster_path
			  ,production_companies
			  ,production_countries
			  ,release_date
			  ,revenue
			  ,runtime
			  ,spoken_languages
			  ,status
			  ,tagline
			  ,title
			  ,video
			  ,vote_average
			  ,vote_count
		FROM
		dbo.movies_metadata

		SET @end_time=GETDATE()
		PRINT 'Time taken to insert is: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds'
		SET @end_bronze=GETDATE()
		PRINT 'Time taken to load bronze layer is: '+CAST(DATEDIFF(SECOND,@start_bronze,@end_bronze)AS NVARCHAR) + ' seconds'
	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURED'
		PRINT 'ERROR MESSAGE: '+ERROR_MESSAGE()
		PRINT 'ERROR NUMBER: '+ CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT 'ERROR STATUS: ' + CAST(ERROR_STATE() AS NVARCHAR)
	END CATCH
END

---you can either load it from the database tasks or by writing a schema (i have done both in here) 
---if you dont think you will need small testing datasets like small ratings you can skip

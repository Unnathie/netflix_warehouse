/*===============================================================
   Stored Procedure: nbronze.load_net_bronze
   Purpose:
       - Loads all Bronze layer tables using BULK INSERT
       - Truncates old data
       - Measures time taken for each load
       - Handles errors cleanly

   Why Bronze?
       Bronze layer = RAW ingestion layer.
       We load data "as-is" from cleaned CSV files.
===============================================================*/

CREATE OR ALTER PROCEDURE nbronze.load_net_bronze 
AS
BEGIN
    BEGIN TRY
        
        DECLARE 
            @start_time DATETIME,
            @end_time   DATETIME,
            @start_bronze DATETIME,
            @end_bronze   DATETIME;

        PRINT '--------------------';
        PRINT 'LOADING BRONZE LAYER';
        PRINT '--------------------';

        SET @start_bronze = GETDATE();   -- Track total load time


        -----------------------------------------------------------
        -- 1. ratings_small
        -----------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING nbronze.ratings_small';
        TRUNCATE TABLE nbronze.ratings_small;

        PRINT '>> INSERTING nbronze.ratings_small';
        BULK INSERT nbronze.ratings_small
        FROM 'C:\Users\hp\Desktop\sql-ultimate-course\me\archive\ratings_small.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -----------------------------------------------------------
        -- 2. ratings (full)
        -----------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING nbronze.ratings';
        TRUNCATE TABLE nbronze.ratings;

        PRINT '>> INSERTING nbronze.ratings';
        BULK INSERT nbronze.ratings
        FROM 'C:\Users\hp\Desktop\sql-ultimate-course\me\archive\ratings.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -----------------------------------------------------------
        -- 3. links_small
        -----------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING nbronze.links_small';
        TRUNCATE TABLE nbronze.links_small;

        PRINT '>> INSERTING nbronze.links_small';
        BULK INSERT nbronze.links_small
        FROM 'C:\Users\hp\Desktop\sql-ultimate-course\me\archive\links_small.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -----------------------------------------------------------
        -- 4. links
        -----------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING nbronze.links';
        TRUNCATE TABLE nbronze.links;

        PRINT '>> INSERTING nbronze.links';
        BULK INSERT nbronze.links
        FROM 'C:\Users\hp\Desktop\sql-ultimate-course\me\archive\links.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -----------------------------------------------------------
        -- 5. keywords
        -----------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING nbronze.keywords';
        TRUNCATE TABLE nbronze.keywords;

        PRINT '>> INSERTING nbronze.keywords';
        BULK INSERT nbronze.keywords
        FROM 'C:\Users\hp\Desktop\sql-ultimate-course\me\archive\keywords_clean.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -----------------------------------------------------------
        -- 6. credits
        -----------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING nbronze.credits';
        TRUNCATE TABLE nbronze.credits;

        PRINT '>> INSERTING nbronze.credits';
        BULK INSERT nbronze.credits
        FROM 'C:\Users\hp\Desktop\sql-ultimate-course\me\archive\credits_clean.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -----------------------------------------------------------
        -- 7. movie_metadata (the biggest & most complex file)
        -----------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING nbronze.movie_metadata';
        TRUNCATE TABLE nbronze.movie_metadata;

        PRINT '>> INSERTING nbronze.movie_metadata';
        BULK INSERT nbronze.movie_metadata
        FROM 'C:\Users\hp\Desktop\sql-ultimate-course\me\archive\movies_metadata_clean_utf16.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -----------------------------------------------------------
        -- TOTAL TIME
        -----------------------------------------------------------
        SET @end_bronze = GETDATE();
        PRINT 'Total Bronze Layer Load Time: ' 
              + CAST(DATEDIFF(SECOND, @start_bronze, @end_bronze) AS NVARCHAR) 
              + ' seconds';


    END TRY

    BEGIN CATCH
        PRINT 'ERROR OCCURRED';
        PRINT 'MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'STATE: ' + CAST(ERROR_STATE() AS NVARCHAR);
    END CATCH
END
GO

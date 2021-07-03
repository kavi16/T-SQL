-------------------------------------------------------------------------------------------------------
---       Data Engineer  Skill  Assignment
---       Kavitha Subramanian
------------------------------------------------------------------------------------------------------------

								/*PART 1: QUESTIONS */

/*  1. What is the average age of our customers? */

SELECT

	AVG(DATEDIFF(YEAR, acc.[dateofbirth], GETDATE()))   AS avg_age

FROM [etl].[tblAccounts] AS acc
--------------------------------------------------------------------------------------------------
/*2. How many distinct users have sales? */

SELECT  *
	
	FROM
		(
			SELECT *,
				ROW_NUMBER() OVER(PARTITION BY userid ORDER BY (select 0)) AS rn

			FROM [etl].[tblCompletedSales]
		)AS sq
 WHERE sq.rn =1 --295,525 records
---------------------------------------------------------------------------------------------------------
 /*3) What is the average price of all vehicles sold? */

 SELECT 

	AVG(vehicleprice)             AS  avg_vhcl_price

 FROM [etl].[tblVehiclePurchaseLocks]
---------------------------------------------------------------------------------------------------------
 /*4)How many vehicles does a user lock before completing a sale? */

 SELECT *

	FROM
		(
			SELECT
				userid, 
				vehicleid, 
				COUNT(vehicleid) OVER(PARTITION BY userid ) AS vehicle_cnt

			FROM [etl].[tblCompletedSales]
		) AS sq
	ORDER BY sq.vehicle_cnt DESC

-- Per data ,A user can lock a maximum of 14 vehicles.
--------------------------------------------------------------------------------------------------------
/*5) For customers with at least two purchases, what is the average time between the
 first and second purchases? */

 SELECT 

	*,
	CASE
		WHEN purchase_cnt= LEAD(purchase_cnt,1) OVER(PARTITION BY userid ,purchase_cnt ORDER BY (SELECT 0))
		THEN (diff_days+LEAD(diff_days,1) OVER(PARTITION BY userid ,purchase_cnt ORDER BY (SELECT 0)))/2.0
		ELSE NULL
	END                                                      AS avg_days
 FROM
	(
		SELECT 
			sq.*,
			ISNULL(DATEDIFF(DAY,LAG(saleeffectivedate) OVER (PARTITION BY userid,purchase_cnt 
														ORDER BY (SELECT 0)), saleeffectivedate),0) AS diff_days

		FROM
			(
				SELECT
					userid, 
					purchaseid, 
					COUNT(purchaseid) OVER(PARTITION BY userid ) AS purchase_cnt,
					saleeffectivedate
 
				FROM [etl].[tblCompletedSales]
			) AS sq
  
	) AS final
WHERE purchase_cnt > 1
-----------------------------------------------------------------------------------------------------------------
/*6) What percentage of users convert through each step in the purchase funnel?
E.g. How many users that create an account ever lock a vehicle?  */

SELECT
	*,
	(loc_userid * 100)/acc_userid  AS 'Percentage'

	FROM
		(
			SELECT 
				COUNT(DISTINCT(acc.userid))                                            AS acc_userid,
				(SELECT COUNT(DISTINCT(userid)) FROM [etl].[tblVehiclePurchaseLocks] ) AS loc_userid

			FROM [etl].[tblAccounts]  acc     
		) AS sq
-----------------------------------------------------------------------------------------------------
/*7)How long does it take a user to convert through each step in the purchase funnel?*/

SELECT
		*,
		CASE
			WHEN sales_dt < lock_dt   THEN lock_duration
			ELSE lock_duration  + sale_duration  
		END                                                                               AS total_duration

		FROM
			(
				SELECT DISTINCT
						acc.userid                                                        AS acc_userid,
						acc.[RowLoadedDatetime]                                           AS user_create_dt,
						loc.[VehiclePurchaseLockDateTime]                                 AS lock_dt,
						sales.[SaleEffectiveDate]                                         AS sales_dt,
						ISNULL(DATEDIFF(DAY, acc.[RowLoadedDatetime] , loc.[VehiclePurchaseLockDateTime]),0)   AS lock_duration,
	                    ISNULL(DATEDIFF(DAY, loc.[VehiclePurchaseLockDateTime] , sales.[SaleEffectiveDate]),0) AS sale_duration,	
	                    ROW_NUMBER() OVER (PARTITION BY acc.userid ORDER BY (SELECT 0))   AS rn

				FROM [etl].[tblAccounts]  acc
					LEFT JOIN [etl].[tblVehiclePurchaseLocks]  loc
						ON acc.userid = loc.userid
					LEFT JOIN [etl].[tblCompletedSales] sales
						ON acc.userid = sales.userid
			) AS sq
WHERE sq.rn =1 
----------------------------------------------------------------------------------------------------
------------------------END OF PART 1-----------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
----                            PART 2
--               NAME:           [analytics].[tblSalesAnalysis].sql 
--
----             INPUTS    :     [etl].[tblAccounts]               --user account
----                       :     [etl].[tblCompletedSales]         --finalized sale
----                       :     [etl].[tblVehiclePurchaseLocks]   --vehicle a user starts a purchase for

---              OUTPUTS   :     [analytics].[tblSalesAnalysis]
---
---              HISTORY   :      AUTHOR    DATE            DESCRIPTION
--                                KSUB      2021-06-13      Analysis of Sales data
---------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- 001A - Populate Volatile table --Number of vehicles a user can lock before a sale(vehicle_cnt). 
--average time difference of a user between first and second purchase (avg_days).
----------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #temp1
SELECT 
     final.*

INTO #temp1
	FROM 
		(
		SELECT 
			*,
			CASE
				WHEN purchase_cnt= LEAD(purchase_cnt,1) OVER(PARTITION BY userid ,purchase_cnt ORDER BY (SELECT 0))
				THEN (diff_days+LEAD(diff_days,1) OVER(PARTITION BY userid ,purchase_cnt ORDER BY (SELECT 0)))/2.0
				ELSE NULL
			END                                                      AS avg_days --avg time between two purchases.
		FROM
			(
				SELECT 
					sq.*,
					ISNULL(DATEDIFF(DAY,LAG(saleeffectivedate) OVER (PARTITION BY userid,purchase_cnt 
														ORDER BY (SELECT 0)), saleeffectivedate),0) AS diff_days

				FROM
					(
						SELECT
							userid, 
							purchaseid, 
							COUNT(purchaseid) OVER(PARTITION BY userid ) AS purchase_cnt,--count of purchases a user makes
							COUNT(vehicleid) OVER(PARTITION BY userid )  AS vehicle_cnt,-- count of vehicles a user locks

							saleeffectivedate
 
						FROM [etl].[tblCompletedSales]
					) AS sq
  
			) AS sq1
--WHERE purchase_cnt > 1  /* to check for more than one purchase*/
)AS final		
--- select * from #temp1 WHERE purchase_cnt > 1
---------------------------------------------------------------------------------------------------
-- 001A - Populate Volatile table --user to convert through each step in the purchase funnel. 
--Time duration between a user creates his account and locking a veicle (lock_duration)
--Time duration between locking a veicle and finalizing a sale (sale_duration)
-- total time taken from user creation to finalizing a sale ,end to end in a purchase funnel(total_duration)
----------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #purchaseFunnel
SELECT 
     final.*

INTO #purchaseFunnel
	FROM 
		(
			SELECT
		*,
		CASE
			WHEN sales_dt < lock_dt   THEN lock_duration
			ELSE lock_duration  + sale_duration  
		END                                                                               AS total_duration

		FROM
			(
				SELECT DISTINCT
						acc.userid                                                                              AS acc_userid,
						acc.[RowLoadedDatetime]                                                                 AS user_create_dt,
						loc.[VehiclePurchaseLockDateTime]                                                       AS lock_dt,
						sales.[SaleEffectiveDate]                                                               AS sales_dt,
						ISNULL(DATEDIFF(DAY, acc.[RowLoadedDatetime] , loc.[VehiclePurchaseLockDateTime]),0)    AS lock_duration,
	                    ISNULL(DATEDIFF(DAY, loc.[VehiclePurchaseLockDateTime] , sales.[SaleEffectiveDate]),0)  AS sale_duration,	
	                    ROW_NUMBER() OVER (PARTITION BY acc.userid ORDER BY (SELECT 0))                         AS rn

				FROM [etl].[tblAccounts]  acc
					LEFT JOIN [etl].[tblVehiclePurchaseLocks]  loc
						ON acc.userid = loc.userid
					LEFT JOIN [etl].[tblCompletedSales] sales
						ON acc.userid = sales.userid
			) AS sq
		WHERE sq.rn =1 
		)AS final
-------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #finalTemp
CREATE TABLE #finalTemp
(
  [UserID]            uniqueidentifier        NULL,
  [user_create_dt]    datetime2(7)            NULL,
  [lock_dt]           datetime2(7)            NULL,
  [sales_dt]          datetime2(7)            NULL,
  [lock_duration]     int                     NULL,
  [sale_duration]     int                     NULL,
  [total_duration]    int                     NULL,
  [PurchaseID]        int                     NULL,
  [purchase_cnt]      int                     NULL,
  [vehicle_cnt]       int                     NULL,
  [SaleEffectiveDate] date                    NULL,
  [diff_days]         int                     NULL,
  [avg_days]          int                     NULL,
  [record_nbr]        int                     NULL,
  [time_stamp]        datetime2(7)            NULL,
)
INSERT INTO #finalTemp
SELECT 
		
			pf.acc_userid                                          AS [UserID],
			pf.user_create_dt                                      AS [user_create_dt] ,
			pf.lock_dt                                             AS [lock_dt],
			pf.sales_dt                                            AS [sales_dt],
			pf.lock_duration                                       AS [lock_duration],
			pf.sale_duration                                       AS [sale_duration],
			pf.total_duration                                      AS [total_duration],
			t1.purchaseid                                          AS [PurchaseID],			                       
			t1.purchase_cnt                                        AS [purchase_cnt],			                          
			t1.vehicle_cnt                                         AS [vehicle_cnt],
			t1.saleeffectivedate                                   AS [SaleEffectiveDate],
			t1.diff_days                                           AS [diff_days],
			t1.avg_days                                            AS [avg_days],
			ROW_NUMBER() OVER(ORDER BY pf.acc_userid )             AS [record_nbr],
			GETDATE()                                              AS [time_stamp]

FROM #purchaseFunnel  pf
	LEFT JOIN #temp1 t1
		ON pf.acc_userid = t1.userid
--------------------------------------------------------------------------------------------------------		
-------------------------------------------------------------------------
-- 004A - Merge values into Target Table 
-------------------------------------------------------------------------
MERGE INTO
    [keenCayman].[analytics].[tblSalesAnalysis]                AS dest
USING
    #finalTemp                                                 AS orig
ON
( 
        dest.[UserID]               =         orig.[UserID] 
	AND dest.[record_nbr]           =         orig.[record_nbr]
   
)
WHEN MATCHED AND
(
    dest.[user_create_dt]	           !=        orig.[user_create_dt]           OR
    dest.[lock_dt]                     !=        orig.[lock_dt]                  OR
    dest.[sales_dt]	                   !=        orig.[sales_dt]			     OR   
    dest.[lock_duration]               !=        orig.[lock_duration]            OR
    dest.[sale_duration]               !=        orig.[sale_duration]            OR
    dest.[total_duration]	           !=        orig.[total_duration]     		 OR
    dest.[PurchaseID]                  !=        orig.[PurchaseID]           	 OR
    dest.[purchase_cnt]                !=        orig.[purchase_cnt]             OR
    dest.[vehicle_cnt]	               !=        orig.[vehicle_cnt]	             OR
    dest.[SaleEffectiveDate]           !=        orig.[SaleEffectiveDate]        OR
    dest.[diff_days]		           !=        orig.[diff_days]                OR
    dest.[avg_days]		               !=        orig.[avg_days]                 OR
    dest.[time_stamp]                  !=        orig.[time_stamp]                      
     
 )
THEN UPDATE
SET 
    dest.[UserID]                      =        orig.[UserID]                ,
	dest.[user_create_dt]	           =        orig.[user_create_dt]        ,         
    dest.[lock_dt]                     =        orig.[lock_dt]               ,
    dest.[sales_dt]	                   =        orig.[sales_dt]			     ,
    dest.[lock_duration]               =        orig.[lock_duration]         , 
    dest.[sale_duration]               =        orig.[sale_duration]         , 
    dest.[total_duration]	           =        orig.[total_duration]     	 , 
    dest.[PurchaseID]                  =        orig.[PurchaseID]            , 
    dest.[purchase_cnt]                =        orig.[purchase_cnt]          ,  
    dest.[vehicle_cnt]	               =        orig.[vehicle_cnt]	         ,   
    dest.[SaleEffectiveDate]           =        orig.[SaleEffectiveDate]     ,  
    dest.[diff_days]		           =        orig.[diff_days]             ,  
    dest.[avg_days]		               =        orig.[avg_days]              , 
	dest.[record_nbr]                  =        orig.[record_nbr]            ,
    dest.[time_stamp]                  =        orig.[time_stamp]
	
WHEN NOT MATCHED BY TARGET
THEN INSERT
(
	[UserID]             ,
	[user_create_dt]	 ,
	[lock_dt]          	 ,
	[sales_dt]	         ,
	[lock_duration]    	 ,
	[sale_duration]    	 ,
	[total_duration]	 ,
	[PurchaseID]       	 ,
	[purchase_cnt]     	 ,
	[vehicle_cnt]	     ,
	[SaleEffectiveDate]	 ,
	[diff_days]			 ,
	[avg_days]		     ,
	[record_nbr]         ,
	[time_stamp]       
	
 )
VALUES
(
   orig.[UserID]             ,
   orig.[user_create_dt]	 ,
   orig.[lock_dt]          	 ,
   orig.[sales_dt]	         ,
   orig.[lock_duration]    	 ,
   orig.[sale_duration]    	 ,
   orig.[total_duration]	 ,
   orig.[PurchaseID]       	 ,
   orig.[purchase_cnt]     	 ,
   orig.[vehicle_cnt]	     ,
   orig.[SaleEffectiveDate]	 ,
   orig.[diff_days]			 ,
   orig.[avg_days]		     ,
   orig.[record_nbr]         ,
   orig.[time_stamp]       
);

--truncate table [analytics].[tblSalesAnalysis]
--select * from [keenCayman].[analytics].[tblSalesAnalysis]
---------------------------- TABLE HEADER ------------------------------
--
--     NAME:  spSalesAnalysis.sql 
--
--  HISTORY:  AUTHOR    DATE            DESCRIPTION
--            KSUB      2021-06-13      Update records in tblSalesAnalysis
--           
-----------------------------------------------------------------------

CREATE OR ALTER PROCEDURE analytics.spSalesAnalysis
(
	 @UserID                 uniqueidentifier       ,                     
	 @user_create_dt	     datetime        	    , 
	 @lock_dt                datetime        	    , 
	 @sales_dt               datetime               , 
	 @lock_duration          int                 	, 
	 @sale_duration          int                 	, 
	 @total_duration	     int                 	, 
	 @PurchaseID             int                 	, 
	 @purchase_cnt           int                 	, 
	 @vehicle_cnt	         int                 	, 
	 @SaleEffectiveDate      date                	, 
	 @diff_days		         int                 	, 
	 @avg_days		         int                    ,      
	 @record_nbr             int           
)
AS


BEGIN
SET NOCOUNT ON

UPDATE   [analytics].[tblSalesAnalysis]

SET 
	
		[UserID]                      =       @UserID                ,
		[user_create_dt]	          =       @user_create_dt        ,  
		[lock_dt]                     =       @lock_dt               ,
		[sales_dt]	                  =       @sales_dt			     ,
		[lock_duration]               =       @lock_duration         , 
		[sale_duration]               =       @sale_duration         , 
		[total_duration]	          =       @total_duration        , 
		[PurchaseID]                  =       @PurchaseID            , 
		[purchase_cnt]                =       @purchase_cnt          ,  
		[vehicle_cnt]	              =       @vehicle_cnt           ,  
		[SaleEffectiveDate]           =       @SaleEffectiveDate     ,  
		[diff_days]		              =       @diff_days             ,  
		[avg_days]		              =       @avg_days              , 
		[record_nbr]                  =       @record_nbr            ,
		[time_stamp]                  =       GETDATE()

WHERE 
		[UserID]                      =       @UserID  


END

-----------------------------------------------------------------------------------------------
-- Job Scheduler to update stored procedure for every 10 mins
---------------------------------------------------------------------------------------------------

CREATE PROCEDURE analytics.spCreateSchedule
@intervalType VARCHAR(50),     
@interval int

AS
BEGIN  
    SET NOCOUNT ON;
   
    declare @delayTime nvarchar(50)
    set @delayTime = '06:00'
	
    WHILE 1 = 1
    BEGIN
        waitfor time @delayTime 
        BEGIN
            
            EXECUTE analytics.spSalesAnalysis   [analytics].tblSalesAnalysisType /*Table valued parameter*/
        END
    END
END
-----------------------------------------------------------------------------------------------------
----- table type to pass parameters to stored procedure ----------------------------------------------------------------
-------------------------------------------------------------------------------------
CREATE TYPE [analytics].tblSalesAnalysisType AS TABLE
(
  [UserID]            uniqueidentifier        NOT NULL,
  [user_create_dt]    datetime2(7)                NULL,
  [lock_dt]           datetime2(7)                NULL,
  [sales_dt]          datetime2(7)                NULL,
  [lock_duration]     int                         NULL,
  [sale_duration]     int                         NULL,
  [total_duration]    int                         NULL,
  [PurchaseID]        int                         NULL,
  [purchase_cnt]      int                         NULL,
  [vehicle_cnt]       int                         NULL,
  [SaleEffectiveDate] date                        NULL,
  [diff_days]         int                         NULL,
  [avg_days]          int                         NULL,
  [record_nbr]        int                     NOT NULL,
  [time_stamp]        datetime2(7)                NULL
  )
--------------------------------------------------------------------------------------------------
---- Exec job scheduler for every day -------------------------------------------------------
-------------------------------------------------------------------------------------------------
EXEC analytics.spCreateSchedule
@intervalType = 'DAY',
@interval = 1

-------------------------------------------------------------------------------------------------------
Write up:

1) created a stored procedure to update the table analytics.tblSalesAnalysis  --analytics.spSalesAnalysis
2) created a  (stored procedure) scheduler to run at 6:00 A.M everyday        --analytics.spCreateSchedule
3) create type [analytics].tblSalesAnalysisType  to pass table valued parameter to stored procedure analytics.spSalesAnalysis 

Insights:

[SaleEffectiveDate] from [etl].[tblCompletedSales] seems to be lesser date value when compared to 
[VehiclePurchaseLockDateTime] from [etl].[tblVehiclePurchaseLocks] for some records.
Query:
select * from [analytics].[tblSalesAnalysis] where sales_dt < lock_dt

---------------------------------------------------------------------------------------------------------------




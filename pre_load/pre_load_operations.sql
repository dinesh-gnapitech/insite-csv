
-- This file is used to set up the database for the pre-load operations.
-- It creates the necessary tables and populates them with data from the source database.

-------------------------------------------------------------------------------------------------------------------------------
-- Setup insite_workorder_history table for data loading
-------------------------------------------------------------------------------------------------------------------------------
SELECT 
    '{' + CAST(w.ID AS VARCHAR(266)) + '}' AS ID,
    w.WO_NAME ,
    w.WO_NUMBER,
    w.WO_STATUS,
    w.WO_STATUS_DATE,
    w.WO_TYPE,
    w.NUM_OBJECTS,
    w.VENDOR,
    w.PARENT_WO_ID, -- Parent Work Order ID
	W.ASSIGNED_USER_ID,
    w.ASSIGNED_DATE, -- Assigned Date
    w.DUE_DATE, -- Due Date
    w.CREATED_BY,
    w.CREATED_DATE,
    w.MODIFIED_BY,
    w.MODIFIED_DATE,
    CASE 
        WHEN w.IS_DELETED = 1 THEN 'YES'
        WHEN w.IS_DELETED = 0 THEN 'NO'
        ELSE 'NULL' -- optional, in case NULLs exist
    END AS IS_DELETED,
    w.CLIENT_ID,
    af.AMBIT_NAME,
    w.WO_TAGS,
    it.TYPE AS INSPECT_TYPE,
    ip.INSPECTION_PROGRAM_NAME
    INTO insite_workorder_history
FROM 
    WORKORDER w
LEFT JOIN 
    INSPECTTYPE it ON w.INSPECT_TYPE_ID = it.ID
LEFT JOIN 
    INSPECTIONPROGRAM ip ON w.INSPECTION_PROGRAM_ID = ip.ID
LEFT JOIN 
    AMBIT_FEATURE af ON w.AMBIT_ID = af.AMBIT_ID

-- The above query selects data from the WORKORDER table and joins it with INSPECTTYPE, INSPECTIONPROGRAM, and AMBIT_FEATURE tables.
-- It retrieves relevant fields such as ID, WO_NAME, WO_NUMBER, WO_STATUS, and others.
-- The ID is formatted as a string enclosed in curly braces.
-- The data is inserted into the insite_workorder_history table.
-- The insite_workorder_history table is used to store work order history data.
-- The data from the insite_workorder_history table is ready to be used for migration.

-------------------------------------------------------------------------------------------------------------------------------
-- Setup inspection_insite_history table for data loading
--------------------------------------------------------------------------------------------------------------------------------

--STEP 1 : Select latest structure and switch data and save it in the table

SELECT *
 INTO LatestStructureData
    FROM (
        SELECT 
            SA.*, 
            ROW_NUMBER() OVER (PARTITION BY SA.STR_ID ORDER BY SA.GDB_TO_DATE DESC) AS rn
        FROM STRUCTURE SA
    ) sub
    WHERE rn = 1

 SELECT *
 INTO LatestSwitchData
    FROM (
        SELECT 
            SWA.*, 
            ROW_NUMBER() OVER (PARTITION BY SWA.SWITCH_GUID ORDER BY SWA.GDB_TO_DATE DESC) AS rn
        FROM SWITCH SWA
    ) sub
    WHERE rn = 1

-- STEP 2 : Create view that combines inspection data from both STRUCTUREINSPECTION and SWITCHINSPECTION tables with latest structure and switch data.

CREATE VIEW [dbo].[Inspection_Union] AS
SELECT
    'STRUCTURE' AS ASSET_TYPE,
    SI.STR_ID AS ASSETID,
 SI.WO_ID,
    SI.INSPECTION_DATE,
    ISP.INSPECTION_PROGRAM_NAME,
    WOT.TYPE,
    IST.STATUS,
    S.PROGRESS_STATUS,
    WO.WO_NUMBER,
 SI.MODIFIED_BY,
 NULL AS ASSET_URN

FROM STRUCTUREINSPECTION SI
LEFT JOIN WORKORDER WO 
    ON SI.WO_ID = WO.ID
LEFT JOIN WORKORDERTYPE WOT 
    ON WO.WO_TYPE_ID = WOT.ID
LEFT JOIN INSPECTIONSTATUS IST 
    ON SI.STATUS_ID = IST.ID
LEFT JOIN INSPECTIONPROGRAM ISP 
    ON WO.INSPECTION_PROGRAM_ID = ISP.ID
LEFT JOIN LatestStructureData S 
    ON SI.STR_ID = S.STR_ID

UNION ALL

SELECT
    'SWITCH' AS ASSET_TYPE,
    SWI.SWITCH_ID AS ASSETID,
 SWI.WO_ID,
    SWI.INSPECTION_DATE,
    SISP.INSPECTION_PROGRAM_NAME,
    SWOT.TYPE,
    SIST.STATUS,
    SW.PROGRESS_STATUS,
    SWO.WO_NUMBER,
 SWI.MODIFIED_BY,
 NULL AS ASSET_URN


FROM SWITCHINSPECTION SWI
LEFT JOIN WORKORDER SWO 
    ON SWI.WO_ID = SWO.ID
LEFT JOIN WORKORDERTYPE SWOT 
    ON SWO.WO_TYPE_ID = SWOT.ID
LEFT JOIN INSPECTIONSTATUS SIST 
    ON SWI.STATUS_ID = SIST.ID
LEFT JOIN INSPECTIONPROGRAM SISP 
    ON SWO.INSPECTION_PROGRAM_ID = SISP.ID
LEFT JOIN LatestSwitchData SW 
    ON SWI.SWITCH_ID = SW.SWITCH_GUID

-- This view combines inspection data from both STRUCTUREINSPECTION and SWITCHINSPECTION tables.
-- It includes relevant fields such as ASSET_TYPE, ASSETID, WO_ID, INSPECTION_DATE, and others.

-- Step 3 : Load the view data into the ASSET_INS_TEMP table also added workorder_urn. 
SELECT 
       [ASSET_TYPE]
      , ASSETID
      --,[WO_ID]
      ,CAST([INSPECTION_DATE] AS DATE) AS INSPECTION_DATE
      ,[INSPECTION_PROGRAM_NAME]
      ,[TYPE]
      ,[STATUS]
      ,[PROGRESS_STATUS]
      ,[WO_NUMBER]
      ,[MODIFIED_BY]
      ,[ASSET_URN]
   ,'insite_workorder_history/{'+ CAST([WO_ID] AS VARCHAR(266)) + '}' AS WORKORDER_URN
   INTO ASSET_INS_TEMP
  FROM [dbo].[Inspection_Union]
  order by inspection_date asc
-- This query selects data from the Inspection_Union view and inserts it into the ASSET_INS_TEMP table.
-- It includes relevant fields such as ASSET_TYPE, ASSETID, INSPECTION_DATE, and others.
-- The WORKORDER_URN is constructed using the WO_ID from the ASSET_INS_TEMP table.
-- The data is ordered by INSPECTION_DATE in ascending order.

-- Step 4 : Load the ASSET_INS_TEMP data into the  INTO inspection_insite_history table.
-- Fetch the unique records from ASSET_INS_TEMP and insert them into the inspection_insite_history table with unique ID for each record.
-- The ID is generated using NEWID() to ensure uniqueness.
-- The ASSET_ID is formatted as a string enclosed in curly braces.
-- The data is selected from the ASSET_INS_TEMP table and inserted into the inspection_insite_history table.
SELECT 
   
   '{'+ (CAST(ID AS VARCHAR(266))) +'}' AS ID
   ,[ASSET_TYPE]
   ,ASSET_ID
   ,[INSPECTION_DATE]
   ,[INSPECTION_PROGRAM_NAME]
   ,[TYPE]
    ,[STATUS]
    ,[PROGRESS_STATUS]
    ,[WO_NUMBER]
    ,[MODIFIED_BY]
    ,[ASSET_URN]
    ,[WORKORDER_URN]
 INTO inspection_insite_history

FROM
(SELECT
       NEWID() AS ID
      ,[ASSET_TYPE]
      ,'{'+ (CAST (ASSETID AS VARCHAR(266))) + '}' AS ASSET_ID
      ,[INSPECTION_DATE]
      ,[INSPECTION_PROGRAM_NAME]
      ,[TYPE]
      ,[STATUS]
      ,[PROGRESS_STATUS]
      ,[WO_NUMBER]
      ,[MODIFIED_BY]
      ,[ASSET_URN]
      ,[WORKORDER_URN]
   
     
FROM
(SELECT DISTINCT
       [ASSET_TYPE]
      ,[ASSETID]
      ,[INSPECTION_DATE]
      ,[INSPECTION_PROGRAM_NAME]
      ,[TYPE]
      ,[STATUS]
      ,[PROGRESS_STATUS]
      ,[WO_NUMBER]
      ,[MODIFIED_BY]
      ,[ASSET_URN]
      ,[WORKORDER_URN]
  FROM [dbo].[ASSET_INS_TEMP]) A) B

-- The data from inspection_insite_history table is ready to be used for migration.
-- The table contains the latest inspection data for both STRUCTURE and SWITCH assets.

-- Setup inspection_insite_history_component table for data loading

-- Step 1 : Load the asset component data into the precomponent table from the existing view Vw_SYNC_STR_SWITCH_IAP.
-- Fetch the unique records from the source table and insert them into the precomponent table with unique ID for each record.

SELECT NEWID() AS ID
       ,[S_ID] AS ASSET_ID
       ,WO.WO_NUMBER
      ,[ATTRIBUTE_NAME] AS COMPONENT
      ,[PROPERTY_DESCRIPTION] AS PROBLEM
      ,[ATTRIBUTE_STATUS] AS CONDITION
      ,[PROPERTY_COMMENTS] AS COMMENTS
   Into precomponent
  FROM [dbo].[Vw_SYNC_STR_SWITCH_IAP] CO
  LEFT JOIN WORKORDER WO ON CO.WO_ID = WO.ID
-- This query selects data from the Vw_SYNC_STR_SWITCH_IAP view and inserts it into the precomponent table.
-- It includes relevant fields such as ASSET_ID, WO_NUMBER, COMPONENT, PROBLEM, CONDITION, and COMMENTS.

--Step 2 : Load the precomponent data into the inspection_insite_history_component table.
SELECT
       '{'+ CAST(ID AS varchar(266))+'}' AS ID 
      ,'{'+ CAST(ASSET_ID AS varchar(266))+'}' AS ASSET_ID
      ,[WO_NUMBER]
      ,[COMPONENT]
      ,[PROBLEM]
      ,[CONDITION]
      ,[COMMENTS]
   , NULL AS inspection_urn
     INTO inspection_insite_history_component
  FROM [dbo].[precompnent]

-- The data from inspection_insite_history_component table is ready to be used for migration.
-- The table contains the component data for the inspection history.

-- The data is then transformed and loaded into the target tables for further processing.

-- Delete the temporary tables and views used for data loading.
DROP TABLE IF EXISTS LatestStructureData
DROP TABLE IF EXISTS LatestSwitchData
DROP VIEW IF EXISTS Inspection_Union
DROP TABLE IF EXISTS ASSET_INS_TEMP
DROP TABLE IF EXISTS precomponent

*/
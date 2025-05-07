-- ============================================
-- Database Setup for Pre-load Operations
-- ============================================

-------------------------------------------------------------------------------------------------------------------------------
-- Setup inspection_insite_history table for data loading
-------------------------------------------------------------------------------------------------------------------------------

-- STEP 3: Load data from view into ASSET_INS_TEMP table

SELECT 
    [ASSET_TYPE],
    ASSETID,
    CAST([INSPECTION_DATE] AS DATE) AS INSPECTION_DATE,
    [INSPECTION_PROGRAM_NAME],
    [TYPE],
    [STATUS],
    [PROGRESS_STATUS],
    [WO_NUMBER],
    [MODIFIED_BY],
    [ASSET_URN],
    'insite_workorder_history/{' + CAST([WO_ID] AS VARCHAR(266)) + '}' AS WORKORDER_URN
INTO ASSET_INS_TEMP
FROM [dbo].[Inspection_Union]
ORDER BY inspection_date ASC;

-- STEP 4: Load data into inspection_insite_history

SELECT 
    '{' + CAST(ID AS VARCHAR(266)) + '}' AS ID,
    [ASSET_TYPE],
    ASSET_ID,
    [INSPECTION_DATE],
    [INSPECTION_PROGRAM_NAME],
    [TYPE],
    [STATUS],
    [PROGRESS_STATUS],
    [WO_NUMBER],
    [MODIFIED_BY],
    [ASSET_URN],
    [WORKORDER_URN]
INTO inspection_insite_history
FROM (
    SELECT 
        NEWID() AS ID,
        [ASSET_TYPE],
        '{' + CAST(ASSETID AS VARCHAR(266)) + '}' AS ASSET_ID,
        [INSPECTION_DATE],
        [INSPECTION_PROGRAM_NAME],
        [TYPE],
        [STATUS],
        [PROGRESS_STATUS],
        [WO_NUMBER],
        [MODIFIED_BY],
        [ASSET_URN],
        [WORKORDER_URN]
    FROM (
        SELECT DISTINCT
            [ASSET_TYPE],
            [ASSETID],
            [INSPECTION_DATE],
            [INSPECTION_PROGRAM_NAME],
            [TYPE],
            [STATUS],
            [PROGRESS_STATUS],
            [WO_NUMBER],
            [MODIFIED_BY],
            [ASSET_URN],
            [WORKORDER_URN]
        FROM [dbo].[ASSET_INS_TEMP]
    ) A
) B;

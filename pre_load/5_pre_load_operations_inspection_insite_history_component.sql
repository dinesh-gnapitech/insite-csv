-- ============================================
-- Database Setup for Pre-load Operations
-- ============================================

-------------------------------------------------------------------------------------------------------------------------------
-- Setup inspection_insite_history_component table for data loading
-------------------------------------------------------------------------------------------------------------------------------

-- STEP 1: Load asset component data into precomponent

SELECT 
    NEWID() AS ID,
    [S_ID] AS ASSET_ID,
    WO.WO_NUMBER,
    [ATTRIBUTE_NAME] AS COMPONENT,
    [PROPERTY_DESCRIPTION] AS PROBLEM,
    [ATTRIBUTE_STATUS] AS CONDITION,
    [PROPERTY_COMMENTS] AS COMMENTS
INTO precomponent
FROM [dbo].[Vw_SYNC_STR_SWITCH_IAP] CO
LEFT JOIN WORKORDER WO ON CO.WO_ID = WO.ID;

-- STEP 2: Load into inspection_insite_history_component

SELECT
    '{' + CAST(ID AS VARCHAR(266)) + '}' AS ID,
    '{' + CAST(ASSET_ID AS VARCHAR(266)) + '}' AS ASSET_ID,
    [WO_NUMBER],
    [COMPONENT],
    [PROBLEM],
    [CONDITION],
    [COMMENTS],
    NULL AS inspection_urn
INTO inspection_insite_history_component
FROM [dbo].[precomponent];

-- Drop temp objects now that final tables are populated
DROP TABLE IF EXISTS LatestStructureData;
DROP TABLE IF EXISTS LatestSwitchData;
DROP VIEW IF EXISTS Inspection_Union;
DROP TABLE IF EXISTS ASSET_INS_TEMP;
DROP TABLE IF EXISTS precomponent;
 
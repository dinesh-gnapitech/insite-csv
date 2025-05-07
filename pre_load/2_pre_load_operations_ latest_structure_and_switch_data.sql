-- ============================================
-- Database Setup for Pre-load Operations
-- ============================================

-------------------------------------------------------------------------------------------------------------------------------
-- Setup inspection_insite_history table for data loading
-------------------------------------------------------------------------------------------------------------------------------

-- STEP 1: Create temp tables for latest structure and switch data

SELECT *
INTO LatestStructureData
FROM (
    SELECT 
        SA.*, 
        ROW_NUMBER() OVER (PARTITION BY SA.STR_ID ORDER BY SA.GDB_TO_DATE DESC) AS rn
    FROM STRUCTURE SA
) sub
WHERE rn = 1;

SELECT *
INTO LatestSwitchData
FROM (
    SELECT 
        SWA.*, 
        ROW_NUMBER() OVER (PARTITION BY SWA.SWITCH_GUID ORDER BY SWA.GDB_TO_DATE DESC) AS rn
    FROM SWITCH SWA
) sub
WHERE rn = 1;

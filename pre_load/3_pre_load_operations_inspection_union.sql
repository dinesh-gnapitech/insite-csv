-- ============================================
-- Database Setup for Pre-load Operations
-- ============================================

-------------------------------------------------------------------------------------------------------------------------------
-- Setup inspection_insite_history table for data loading
-------------------------------------------------------------------------------------------------------------------------------

-- STEP 2: Create inspection union view

CREATE VIEW [dbo].[Inspection_Union] (
    ASSET_TYPE,
    ASSETID,
    WO_ID,
    INSPECTION_DATE,
    INSPECTION_PROGRAM_NAME,
    TYPE,
    STATUS,
    PROGRESS_STATUS,
    WO_NUMBER,
    MODIFIED_BY,
    ASSET_URN
) AS
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
LEFT JOIN WORKORDER WO ON SI.WO_ID = WO.ID
LEFT JOIN WORKORDERTYPE WOT ON WO.WO_TYPE_ID = WOT.ID
LEFT JOIN INSPECTIONSTATUS IST ON SI.STATUS_ID = IST.ID
LEFT JOIN INSPECTIONPROGRAM ISP ON WO.INSPECTION_PROGRAM_ID = ISP.ID
LEFT JOIN LatestStructureData S ON SI.STR_ID = S.STR_ID

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
LEFT JOIN WORKORDER SWO ON SWI.WO_ID = SWO.ID
LEFT JOIN WORKORDERTYPE SWOT ON SWO.WO_TYPE_ID = SWOT.ID
LEFT JOIN INSPECTIONSTATUS SIST ON SWI.STATUS_ID = SIST.ID
LEFT JOIN INSPECTIONPROGRAM SISP ON SWO.INSPECTION_PROGRAM_ID = SISP.ID
LEFT JOIN LatestSwitchData SW ON SWI.SWITCH_ID = SW.SWITCH_GUID;
 

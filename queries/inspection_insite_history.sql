WITH LatestStructureData AS (
    SELECT * FROM (
        SELECT SA.*, ROW_NUMBER() OVER (PARTITION BY SA.STR_ID ORDER BY SA.GDB_TO_DATE DESC) AS rn
        FROM STRUCTURE SA
    ) x WHERE rn = 1
),
LatestSwitchData AS (
    SELECT * FROM (
        SELECT SWA.*, ROW_NUMBER() OVER (PARTITION BY SWA.SWITCH_GUID ORDER BY SWA.GDB_TO_DATE DESC) AS rn
        FROM SWITCH SWA
    ) x WHERE rn = 1
),
Inspection_Union AS (
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
    LEFT JOIN LatestSwitchData SW ON SWI.SWITCH_ID = SW.SWITCH_GUID
),
 ASSET_INS_TEMP AS(

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
FROM Inspection_Union

)
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
        FROM ASSET_INS_TEMP
    ) A
) B;

-- This query retrieves inspection history details from the STRUCTUREINSPECTION and SWITCHINSPECTION tables, joining with WORKORDER, WORKORDERTYPE, INSPECTIONSTATUS, and INSPECTIONPROGRAM tables to get additional information.
-- The results are then formatted and ordered by inspection date.
-- The final output includes a unique ID, asset type, asset ID, inspection date, inspection program name, type, status, progress status, work order number, modified by user, asset URN, and work order URN.


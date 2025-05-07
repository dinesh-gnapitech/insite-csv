-- ============================================
-- Database Setup for Pre-load Operations
-- ============================================

-------------------------------------------------------------------------------------------------------------------------------
-- Setup insite_workorder_history table for data loading
-------------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT
    '{' + CAST(w.ID AS VARCHAR(266)) + '}' AS ID,
    w.WO_NAME,
    w.WO_NUMBER,
    w.WO_STATUS,
    w.WO_STATUS_DATE,
    w.WO_TYPE,
    w.NUM_OBJECTS,
    w.VENDOR,
    w.PARENT_WO_ID,
    w.ASSIGNED_USER_ID,
    w.ASSIGNED_DATE,
    w.DUE_DATE,
    w.CREATED_BY,
    w.CREATED_DATE,
    w.MODIFIED_BY,
    w.MODIFIED_DATE,
    CASE 
        WHEN w.IS_DELETED = 1 THEN 'YES'
        WHEN w.IS_DELETED = 0 THEN 'NO'
        ELSE 'NULL'
    END AS IS_DELETED,
    w.CLIENT_ID,
  
    w.WO_TAGS,
    it.TYPE AS INSPECT_TYPE,
    ip.INSPECTION_PROGRAM_NAME
INTO insite_workorder_history
FROM 
    WORKORDER w
LEFT JOIN INSPECTTYPE it ON w.INSPECT_TYPE_ID = it.ID
LEFT JOIN INSPECTIONPROGRAM ip ON w.INSPECTION_PROGRAM_ID = ip.ID;


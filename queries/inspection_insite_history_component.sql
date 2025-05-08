
SELECT 
    '{'+ CAST(NEWID() AS varchar(266)) +'}' AS ID,
    '{'+ CAST(CO.S_ID AS varchar(266)) +'}' AS ASSET_ID,
    WO.WO_NUMBER,
    CO.ATTRIBUTE_NAME AS COMPONENT,
    CO.PROPERTY_DESCRIPTION AS PROBLEM,
    CO.ATTRIBUTE_STATUS AS CONDITION,
    CO.PROPERTY_COMMENTS AS COMMENTS,
    NULL AS inspection_urn
FROM [dbo].[Vw_SYNC_STR_SWITCH_IAP] CO
LEFT JOIN WORKORDER WO ON CO.WO_ID = WO.ID;
-- This query retrieves inspection history details from the Vw_SYNC_STR_SWITCH_IAP view and joins with WORKORDER table to get additional information.
-- The result includes the asset ID, work order number, component name, problem description, condition status, comments, and a placeholder for inspection URN.
-- The final output is formatted with a unique ID for each record.
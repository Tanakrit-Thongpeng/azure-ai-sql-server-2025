-- =============================================
-- Author:		TANAKRIT-THONGPENG
-- Create date: 27/06/2025
-- Description:	SCHEMA QUERY
-- =============================================

SELECT 
    TABLE_CATALOG, 
    TABLE_SCHEMA, 
    TABLE_NAME, 
    COLUMN_NAME, 
    DATA_TYPE
FROM 
    INFORMATION_SCHEMA.COLUMNS
ORDER BY 
    TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
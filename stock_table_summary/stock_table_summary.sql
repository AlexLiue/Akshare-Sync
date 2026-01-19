BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE STOCK_TABLE_SUMMARY';
EXCEPTION
   WHEN OTHERS THEN NULL;
END;

CREATE TABLE STOCK_TABLE_SUMMARY
AS
SELECT
    utc.OWNER AS "所属用户",
    utc.TABLE_NAME AS "表名",
    utcom.COMMENTS AS "表注释",
    utc.COLUMN_NAME AS "字段名",
    CASE
        WHEN utc.DATA_TYPE IN ('VARCHAR2', 'CHAR') THEN utc.DATA_TYPE || '(' || utc.DATA_LENGTH || ')'
        WHEN utc.DATA_TYPE = 'NUMBER' THEN
            CASE
                WHEN utc.DATA_PRECISION IS NOT NULL THEN utc.DATA_TYPE || '(' || utc.DATA_PRECISION ||
                     CASE WHEN utc.DATA_SCALE IS NOT NULL AND utc.DATA_SCALE > 0 THEN ',' || utc.DATA_SCALE ELSE '' END || ')'
                ELSE utc.DATA_TYPE
            END
        ELSE utc.DATA_TYPE
    END AS "字段类型",
    CASE WHEN utc.NULLABLE = 'Y' THEN '是' ELSE '否' END AS "是否可为空",
    utc.COLUMN_ID AS "列序号",
    ucc.COMMENTS AS "字段注释"
FROM
    ALL_TAB_COLUMNS utc
LEFT JOIN
    ALL_TAB_COMMENTS utcom ON utc.OWNER = utcom.OWNER AND utc.TABLE_NAME = utcom.TABLE_NAME
LEFT JOIN
    ALL_COL_COMMENTS ucc ON utc.OWNER = ucc.OWNER
                         AND utc.TABLE_NAME = ucc.TABLE_NAME
                         AND utc.COLUMN_NAME = ucc.COLUMN_NAME
WHERE
    utc.OWNER = UPPER('AKSHARE')
ORDER BY
    utc.OWNER, utc.TABLE_NAME, utc.COLUMN_ID;

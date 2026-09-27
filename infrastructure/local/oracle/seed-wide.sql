WHENEVER SQLERROR EXIT SQL.SQLCODE
SET ECHO OFF FEEDBACK ON HEADING ON PAGESIZE 100 LINESIZE 220 TIMING ON

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE WIDE PURGE';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE TABLE WIDE NOLOGGING AS
SELECT
    CAST(level AS NUMBER(18,0)) AS id,
    CAST(MOD(level, 250000) + 1 AS NUMBER(18,0)) AS account_id,
    CAST(MOD(level, 100) + 1 AS NUMBER(10,0)) AS tenant_id,
    TIMESTAMP '2024-01-01 00:00:00'
        + NUMTODSINTERVAL(MOD(level, 31536000), 'SECOND') AS event_ts,
    DATE '2024-01-01' + MOD(level, 365) AS event_date,
    CAST(MOD(level * 7919, 100000000) / 100 AS NUMBER(14,2)) AS amount,
    CAST(MOD(level, 10000) AS NUMBER(10,0)) AS quantity,
    CAST(MOD(level, 10000) / 10000 AS NUMBER(12,6)) AS rate,
    CAST(CASE MOD(level, 5) WHEN 0 THEN 'NEW' WHEN 1 THEN 'ACTIVE'
         WHEN 2 THEN 'PENDING' WHEN 3 THEN 'CLOSED' ELSE 'ERROR' END
         AS VARCHAR2(12)) AS status,
    CAST('category-' || LPAD(MOD(level, 50), 2, '0') AS VARCHAR2(24)) AS category,
    CAST('subcategory-' || LPAD(MOD(level, 250), 3, '0') AS VARCHAR2(32)) AS subcategory,
    CAST('oracle-local' AS VARCHAR2(24)) AS source_system,
    CAST('region-' || MOD(level, 8) AS VARCHAR2(16)) AS region,
    CAST('BR' AS CHAR(2)) AS country,
    CAST('city-' || LPAD(MOD(level, 500), 3, '0') AS VARCHAR2(24)) AS city,
    CAST(LPAD(MOD(level, 99999999), 8, '0') AS VARCHAR2(12)) AS postal_code,
    CAST('customer-' || LPAD(level, 10, '0') AS VARCHAR2(40)) AS customer_name,
    CAST('user' || level || '@example.test' AS VARCHAR2(64)) AS email,
    CAST('+55' || LPAD(MOD(level, 10000000000), 10, '0') AS VARCHAR2(20)) AS phone,
    CAST('REF-' || LPAD(level, 12, '0') AS VARCHAR2(24)) AS reference_code,
    CAST(RPAD('deterministic oracle benchmark row ' || level || ' ', 180,
         CHR(65 + MOD(level, 26))) AS VARCHAR2(200)) AS description,
    CAST('a01-' || MOD(level, 1000) AS VARCHAR2(32)) AS attr_01,
    CAST('a02-' || MOD(level, 1001) AS VARCHAR2(32)) AS attr_02,
    CAST('a03-' || MOD(level, 1002) AS VARCHAR2(32)) AS attr_03,
    CAST('a04-' || MOD(level, 1003) AS VARCHAR2(32)) AS attr_04,
    CAST('a05-' || MOD(level, 1004) AS VARCHAR2(32)) AS attr_05,
    CAST('a06-' || MOD(level, 1005) AS VARCHAR2(32)) AS attr_06,
    CAST('a07-' || MOD(level, 1006) AS VARCHAR2(32)) AS attr_07,
    CAST('a08-' || MOD(level, 1007) AS VARCHAR2(32)) AS attr_08,
    CAST('a09-' || MOD(level, 1008) AS VARCHAR2(32)) AS attr_09,
    CAST('a10-' || MOD(level, 1009) AS VARCHAR2(32)) AS attr_10,
    CAST('a11-' || MOD(level, 1010) AS VARCHAR2(32)) AS attr_11,
    CAST('a12-' || MOD(level, 1011) AS VARCHAR2(32)) AS attr_12,
    CAST('a13-' || MOD(level, 1012) AS VARCHAR2(32)) AS attr_13,
    CAST('a14-' || MOD(level, 1013) AS VARCHAR2(32)) AS attr_14,
    CAST('a15-' || MOD(level, 1014) AS VARCHAR2(32)) AS attr_15,
    CAST('a16-' || MOD(level, 1015) AS VARCHAR2(32)) AS attr_16,
    CAST('a17-' || MOD(level, 1016) AS VARCHAR2(32)) AS attr_17,
    CAST('a18-' || MOD(level, 1017) AS VARCHAR2(32)) AS attr_18,
    CAST('a19-' || MOD(level, 1018) AS VARCHAR2(32)) AS attr_19,
    CAST('a20-' || MOD(level, 1019) AS VARCHAR2(32)) AS attr_20,
    CAST(CASE WHEN MOD(level, 10) = 0 THEN 'sparse-' || level END AS VARCHAR2(40)) AS sparse_01,
    CAST(CASE WHEN MOD(level, 20) = 0 THEN level / 20 END AS NUMBER(18,0)) AS sparse_02,
    CAST(CASE WHEN MOD(level, 50) = 0 THEN DATE '2020-01-01' + MOD(level, 1000) END AS DATE) AS sparse_03,
    CAST(CASE WHEN MOD(level, 100) = 0 THEN RPAD('x', 100, 'x') END AS VARCHAR2(120)) AS sparse_04,
    CAST(CASE WHEN MOD(level, 200) = 0 THEN MOD(level, 7) / 7 END AS NUMBER(18,8)) AS sparse_05,
    CAST(CASE WHEN MOD(level, 2) = 0 THEN 1 ELSE 0 END AS NUMBER(1,0)) AS flag_active,
    TIMESTAMP '2026-01-01 00:00:00' AS created_at
FROM dual
CONNECT BY level <= &benchmark_rows;

ALTER TABLE WIDE ADD CONSTRAINT WIDE_PK PRIMARY KEY (ID);

BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'WIDE',
        estimate_percent => 100,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        cascade => TRUE,
        degree => 2,
        no_invalidate => FALSE
    );
END;
/

SELECT COUNT(*) AS rows_loaded FROM WIDE;
SELECT COUNT(*) AS columns_created
FROM user_tab_columns WHERE table_name = 'WIDE';

EXIT SUCCESS

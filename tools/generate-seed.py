"""Generate deterministic SQL Server benchmark fixtures (no engine hints)."""
from pathlib import Path

root = Path(__file__).resolve().parents[1]
out = root / "infrastructure/local/sqlserver"
out.mkdir(parents=True, exist_ok=True)
sql = """SET NOCOUNT ON;
IF DB_ID(N'Benchmark') IS NULL CREATE DATABASE Benchmark;
GO
ALTER DATABASE Benchmark SET RECOVERY SIMPLE;
GO
USE Benchmark;
GO
SET NOCOUNT ON;
IF OBJECT_ID('dbo.small','U') IS NULL
CREATE TABLE dbo.small(id bigint NOT NULL PRIMARY KEY CLUSTERED, name nvarchar(64), amount decimal(18,2), updated_at datetime2(3), active bit);
IF OBJECT_ID('dbo.medium','U') IS NULL
CREATE TABLE dbo.medium(id bigint NOT NULL PRIMARY KEY CLUSTERED, name nvarchar(80), amount decimal(18,2), updated_at datetime2(3), category int, parent_id bigint NULL, payload nvarchar(128),
  a int, b bigint, c decimal(28,6), d date, e nvarchar(32), f bit, g float, h nvarchar(64) NULL, i int NULL);
"""
columns = [f"sparse_{i:02d} nvarchar(96) SPARSE NULL" for i in range(1,44)]
sql += "IF OBJECT_ID('dbo.wide','U') IS NULL\nCREATE TABLE dbo.wide(id bigint NOT NULL PRIMARY KEY CLUSTERED, category int NOT NULL, updated_at datetime2(3), amount decimal(28,6), label nvarchar(64),\n" + ",\n".join(columns) + ");\n"
sql += """
-- One million deterministic numbers; fixtures are seeded once, not overwritten.
IF (SELECT COUNT_BIG(*) FROM dbo.small)=0 OR (SELECT COUNT_BIG(*) FROM dbo.medium)=0 OR (SELECT COUNT_BIG(*) FROM dbo.wide)=0
BEGIN
  SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n INTO #numbers
  FROM (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) a(n)
  CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) b(n)
  CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) c(n)
  CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) d(n)
  CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) e(n)
  CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) f(n);
  CREATE UNIQUE CLUSTERED INDEX ix_n ON #numbers(n);
  IF NOT EXISTS(SELECT 1 FROM dbo.small)
  INSERT dbo.small SELECT n,CONCAT(N'Cliente São Paulo ',n),CAST(n%100000 AS decimal(18,2))/100,DATEADD(second,CAST(n AS int),'2025-01-01'),n%2 FROM #numbers WHERE n<=10000;
  IF NOT EXISTS(SELECT 1 FROM dbo.medium)
  INSERT dbo.medium SELECT n,CONCAT(N'Pedido ação ',n),CAST(n%100000 AS decimal(18,2))/100,DATEADD(second,CAST(n AS int),'2025-01-01'),n%100,CASE WHEN n%5=0 THEN NULL ELSE n%10000 END,
    REPLICATE(CONVERT(nvarchar(32),CONVERT(varchar(32),HASHBYTES('MD5',CONVERT(varchar(20),n)),2)),3),n%1000,n*1000,CAST(n AS decimal(28,6))/7,
    DATEADD(day,CAST(n%365 AS int),'2025-01-01'),CONCAT(N'Cat-',n%30),n%2,SQRT(n),CASE WHEN n%3=0 THEN NULL ELSE N'Informação' END,CASE WHEN n%7=0 THEN NULL ELSE n%20 END
    FROM #numbers WHERE n<=200000;
  IF NOT EXISTS(SELECT 1 FROM dbo.wide)
  BEGIN
    DECLARE @start bigint=1;
    WHILE @start<=1000000
    BEGIN
      INSERT dbo.wide SELECT
        CASE WHEN n<=900000 THEN n ELSE 1000000000+n*100 END,
        n%100,DATEADD(second,CAST(n AS int),'2025-01-01'),CAST(n AS decimal(28,6))/7,CONCAT(N'Esparso ',n),
"""
sql += ",\n".join(f"CASE WHEN (n+{i*17})%{7+i%7}=0 THEN CONCAT(N'atributo-{i}-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',{i})),2)) ELSE NULL END" for i in range(1,44))
sql += """
      FROM #numbers WHERE n>=@start AND n<@start+100000;
      SET @start=@start+100000;
      CHECKPOINT;
    END
  END
END;
UPDATE STATISTICS dbo.small WITH FULLSCAN;
UPDATE STATISTICS dbo.medium WITH FULLSCAN;
UPDATE STATISTICS dbo.wide WITH FULLSCAN;
SELECT t.name, SUM(p.rows) rows FROM sys.tables t JOIN sys.partitions p ON p.object_id=t.object_id AND p.index_id IN(0,1) GROUP BY t.name;
GO
"""
(out / "seed.sql").write_text(sql)

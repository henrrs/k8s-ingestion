SET NOCOUNT ON;
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
IF OBJECT_ID('dbo.wide','U') IS NULL
CREATE TABLE dbo.wide(id bigint NOT NULL PRIMARY KEY CLUSTERED, category int NOT NULL, updated_at datetime2(3), amount decimal(28,6), label nvarchar(64),
sparse_01 nvarchar(96) SPARSE NULL,
sparse_02 nvarchar(96) SPARSE NULL,
sparse_03 nvarchar(96) SPARSE NULL,
sparse_04 nvarchar(96) SPARSE NULL,
sparse_05 nvarchar(96) SPARSE NULL,
sparse_06 nvarchar(96) SPARSE NULL,
sparse_07 nvarchar(96) SPARSE NULL,
sparse_08 nvarchar(96) SPARSE NULL,
sparse_09 nvarchar(96) SPARSE NULL,
sparse_10 nvarchar(96) SPARSE NULL,
sparse_11 nvarchar(96) SPARSE NULL,
sparse_12 nvarchar(96) SPARSE NULL,
sparse_13 nvarchar(96) SPARSE NULL,
sparse_14 nvarchar(96) SPARSE NULL,
sparse_15 nvarchar(96) SPARSE NULL,
sparse_16 nvarchar(96) SPARSE NULL,
sparse_17 nvarchar(96) SPARSE NULL,
sparse_18 nvarchar(96) SPARSE NULL,
sparse_19 nvarchar(96) SPARSE NULL,
sparse_20 nvarchar(96) SPARSE NULL,
sparse_21 nvarchar(96) SPARSE NULL,
sparse_22 nvarchar(96) SPARSE NULL,
sparse_23 nvarchar(96) SPARSE NULL,
sparse_24 nvarchar(96) SPARSE NULL,
sparse_25 nvarchar(96) SPARSE NULL,
sparse_26 nvarchar(96) SPARSE NULL,
sparse_27 nvarchar(96) SPARSE NULL,
sparse_28 nvarchar(96) SPARSE NULL,
sparse_29 nvarchar(96) SPARSE NULL,
sparse_30 nvarchar(96) SPARSE NULL,
sparse_31 nvarchar(96) SPARSE NULL,
sparse_32 nvarchar(96) SPARSE NULL,
sparse_33 nvarchar(96) SPARSE NULL,
sparse_34 nvarchar(96) SPARSE NULL,
sparse_35 nvarchar(96) SPARSE NULL,
sparse_36 nvarchar(96) SPARSE NULL,
sparse_37 nvarchar(96) SPARSE NULL,
sparse_38 nvarchar(96) SPARSE NULL,
sparse_39 nvarchar(96) SPARSE NULL,
sparse_40 nvarchar(96) SPARSE NULL,
sparse_41 nvarchar(96) SPARSE NULL,
sparse_42 nvarchar(96) SPARSE NULL,
sparse_43 nvarchar(96) SPARSE NULL);

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
CASE WHEN (n+17)%8=0 THEN CONCAT(N'atributo-1-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',1)),2)) ELSE NULL END,
CASE WHEN (n+34)%9=0 THEN CONCAT(N'atributo-2-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',2)),2)) ELSE NULL END,
CASE WHEN (n+51)%10=0 THEN CONCAT(N'atributo-3-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',3)),2)) ELSE NULL END,
CASE WHEN (n+68)%11=0 THEN CONCAT(N'atributo-4-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',4)),2)) ELSE NULL END,
CASE WHEN (n+85)%12=0 THEN CONCAT(N'atributo-5-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',5)),2)) ELSE NULL END,
CASE WHEN (n+102)%13=0 THEN CONCAT(N'atributo-6-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',6)),2)) ELSE NULL END,
CASE WHEN (n+119)%7=0 THEN CONCAT(N'atributo-7-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',7)),2)) ELSE NULL END,
CASE WHEN (n+136)%8=0 THEN CONCAT(N'atributo-8-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',8)),2)) ELSE NULL END,
CASE WHEN (n+153)%9=0 THEN CONCAT(N'atributo-9-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',9)),2)) ELSE NULL END,
CASE WHEN (n+170)%10=0 THEN CONCAT(N'atributo-10-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',10)),2)) ELSE NULL END,
CASE WHEN (n+187)%11=0 THEN CONCAT(N'atributo-11-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',11)),2)) ELSE NULL END,
CASE WHEN (n+204)%12=0 THEN CONCAT(N'atributo-12-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',12)),2)) ELSE NULL END,
CASE WHEN (n+221)%13=0 THEN CONCAT(N'atributo-13-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',13)),2)) ELSE NULL END,
CASE WHEN (n+238)%7=0 THEN CONCAT(N'atributo-14-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',14)),2)) ELSE NULL END,
CASE WHEN (n+255)%8=0 THEN CONCAT(N'atributo-15-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',15)),2)) ELSE NULL END,
CASE WHEN (n+272)%9=0 THEN CONCAT(N'atributo-16-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',16)),2)) ELSE NULL END,
CASE WHEN (n+289)%10=0 THEN CONCAT(N'atributo-17-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',17)),2)) ELSE NULL END,
CASE WHEN (n+306)%11=0 THEN CONCAT(N'atributo-18-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',18)),2)) ELSE NULL END,
CASE WHEN (n+323)%12=0 THEN CONCAT(N'atributo-19-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',19)),2)) ELSE NULL END,
CASE WHEN (n+340)%13=0 THEN CONCAT(N'atributo-20-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',20)),2)) ELSE NULL END,
CASE WHEN (n+357)%7=0 THEN CONCAT(N'atributo-21-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',21)),2)) ELSE NULL END,
CASE WHEN (n+374)%8=0 THEN CONCAT(N'atributo-22-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',22)),2)) ELSE NULL END,
CASE WHEN (n+391)%9=0 THEN CONCAT(N'atributo-23-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',23)),2)) ELSE NULL END,
CASE WHEN (n+408)%10=0 THEN CONCAT(N'atributo-24-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',24)),2)) ELSE NULL END,
CASE WHEN (n+425)%11=0 THEN CONCAT(N'atributo-25-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',25)),2)) ELSE NULL END,
CASE WHEN (n+442)%12=0 THEN CONCAT(N'atributo-26-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',26)),2)) ELSE NULL END,
CASE WHEN (n+459)%13=0 THEN CONCAT(N'atributo-27-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',27)),2)) ELSE NULL END,
CASE WHEN (n+476)%7=0 THEN CONCAT(N'atributo-28-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',28)),2)) ELSE NULL END,
CASE WHEN (n+493)%8=0 THEN CONCAT(N'atributo-29-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',29)),2)) ELSE NULL END,
CASE WHEN (n+510)%9=0 THEN CONCAT(N'atributo-30-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',30)),2)) ELSE NULL END,
CASE WHEN (n+527)%10=0 THEN CONCAT(N'atributo-31-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',31)),2)) ELSE NULL END,
CASE WHEN (n+544)%11=0 THEN CONCAT(N'atributo-32-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',32)),2)) ELSE NULL END,
CASE WHEN (n+561)%12=0 THEN CONCAT(N'atributo-33-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',33)),2)) ELSE NULL END,
CASE WHEN (n+578)%13=0 THEN CONCAT(N'atributo-34-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',34)),2)) ELSE NULL END,
CASE WHEN (n+595)%7=0 THEN CONCAT(N'atributo-35-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',35)),2)) ELSE NULL END,
CASE WHEN (n+612)%8=0 THEN CONCAT(N'atributo-36-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',36)),2)) ELSE NULL END,
CASE WHEN (n+629)%9=0 THEN CONCAT(N'atributo-37-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',37)),2)) ELSE NULL END,
CASE WHEN (n+646)%10=0 THEN CONCAT(N'atributo-38-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',38)),2)) ELSE NULL END,
CASE WHEN (n+663)%11=0 THEN CONCAT(N'atributo-39-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',39)),2)) ELSE NULL END,
CASE WHEN (n+680)%12=0 THEN CONCAT(N'atributo-40-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',40)),2)) ELSE NULL END,
CASE WHEN (n+697)%13=0 THEN CONCAT(N'atributo-41-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',41)),2)) ELSE NULL END,
CASE WHEN (n+714)%7=0 THEN CONCAT(N'atributo-42-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',42)),2)) ELSE NULL END,
CASE WHEN (n+731)%8=0 THEN CONCAT(N'atributo-43-',CONVERT(varchar(32),HASHBYTES('MD5',CONCAT(n,':',43)),2)) ELSE NULL END
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

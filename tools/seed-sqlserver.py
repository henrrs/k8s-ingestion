"""Seed the dedicated laboratory database and create a read-only source login."""
import json
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[1]
kubectl = [str(root / ".tools/bin/kubectl"), "--context", "company-spark", "-n", "spark-lab"]
subprocess.run(kubectl + ["rollout", "status", "deployment/sqlserver", "--timeout=300s"], check=True)
def execute(sql):
    subprocess.run(kubectl + ["exec", "-i", "deploy/sqlserver", "--", "bash", "-c",
        'SQLCMDPASSWORD="$MSSQL_SA_PASSWORD" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C -b -i /dev/stdin'],
        input=sql.encode(), check=True)
execute((root / "infrastructure/local/sqlserver/seed.sql").read_text())
password = json.loads((root / ".local/credentials.json").read_text())["sqlserver_ingestion"]
password = password.replace("'", "''")
execute(f"""USE master;
IF NOT EXISTS(SELECT 1 FROM sys.sql_logins WHERE name=N'ingestion') CREATE LOGIN ingestion WITH PASSWORD=N'{password}';
GO
USE Benchmark;
IF NOT EXISTS(SELECT 1 FROM sys.database_principals WHERE name=N'ingestion') CREATE USER ingestion FOR LOGIN ingestion;
GRANT SELECT, VIEW DEFINITION TO ingestion;
GRANT VIEW DATABASE STATE, VIEW DATABASE PERFORMANCE STATE, VIEW SECURITY DEFINITION TO ingestion;
GO
""")
print("SQL Server fixtures and ingestion reader are ready.")

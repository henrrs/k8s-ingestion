"""Create the deterministic Oracle wide benchmark fixture."""

import argparse
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
KUBECTL = ROOT / ".tools/bin/kubectl"
BASE = [str(KUBECTL), "--context", "company-spark", "-n", "spark-lab"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    args = parser.parse_args()
    if args.rows <= 0:
        raise ValueError("--rows must be positive")
    subprocess.run(
        BASE + ["rollout", "status", "statefulset/oracle", "--timeout=900s"],
        check=True,
    )
    # The workers pin every range to one SCN. The application schema needs
    # explicit access because privileges on SYS packages are not inherited
    # from the CONNECT/RESOURCE roles created by the container image.
    grant_command = (
        'printf \'ALTER SESSION SET CONTAINER=FREEPDB1;\\n'
        'GRANT EXECUTE ON SYS.DBMS_FLASHBACK TO %s;\\nEXIT;\\n\' '
        '"$APP_USER" | sqlplus -s / as sysdba'
    )
    subprocess.run(
        BASE + ["exec", "oracle-0", "--", "/bin/bash", "-c", grant_command],
        check=True,
    )
    sql = (
        f"DEFINE benchmark_rows = {args.rows}\n"
        + (ROOT / "infrastructure/local/oracle/seed-wide.sql").read_text()
    )
    command = (
        'sqlplus -s "$APP_USER/$APP_USER_PASSWORD@//localhost:1521/FREEPDB1"'
    )
    result = subprocess.run(
        BASE + ["exec", "-i", "oracle-0", "--", "/bin/bash", "-c", command],
        input=sql,
        text=True,
        check=True,
    )
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())

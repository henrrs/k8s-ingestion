"""Generic wheel launcher. Contains no ingestion or source-specific logic."""
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request

def main():
    work = Path(tempfile.mkdtemp(prefix="company-run-"))
    url = os.environ["COMPANY_WHEEL_URL"]
    filename = Path(urllib.parse.urlparse(url).path).name
    if not filename.endswith(".whl"):
        raise ValueError("The execution artifact must be a wheel")
    wheel = work / filename
    with urllib.request.urlopen(url, timeout=60) as response:
        wheel.write_bytes(response.read())
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    expected = os.environ.get("COMPANY_WHEEL_SHA256")
    if not expected or digest != expected:
        raise ValueError("A matching immutable wheel SHA256 is required")
    packages = work / "packages"
    # Runtime dependencies are pinned in the runtime; this wheel has no external
    # dependencies. Avoid resolving an unpinned environment during execution.
    subprocess.run([sys.executable, "-m", "pip", "install", "--no-deps", "--no-index",
                    "--disable-pip-version-check", "--target", str(packages), str(wheel)], check=True)
    sys.path.insert(0, str(packages))
    os.environ["COMPANY_WHEEL_DIGEST"] = digest
    config_path = work / "config.json"
    config_path.write_text(json.dumps(json.loads(os.environ["COMPANY_EXECUTION_CONFIG"])))
    config_path.chmod(0o600)
    module, function = os.environ["COMPANY_ENTRYPOINT"].split(":", 1)
    sys.argv = ["company-run", "--config", str(config_path)]
    getattr(importlib.import_module(module), function)()

if __name__ == "__main__":
    main()

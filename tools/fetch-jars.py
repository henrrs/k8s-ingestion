"""Download pinned runtime artifacts; record/verify SHA256 across rebuilds."""
import concurrent.futures
import hashlib
import json
from pathlib import Path
import urllib.request

root = Path(__file__).resolve().parents[1] / "engines/spark/dependencies"
target = root / "jars"
target.mkdir(parents=True, exist_ok=True)
lock_path = root / "jars.lock.json"
old = json.loads(lock_path.read_text()) if lock_path.exists() else {}

def fetch(coordinate):
    group, artifact, version = coordinate.split(":")
    name = f"{artifact}-{version}.jar"
    url = f"https://repo.maven.apache.org/maven2/{group.replace('.', '/')}/{artifact}/{version}/{name}"
    path = target / name
    if not path.exists():
        temporary = path.with_suffix(".partial")
        with urllib.request.urlopen(url, timeout=120) as response, temporary.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
        temporary.rename(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if coordinate in old and old[coordinate]["sha256"] != digest:
        raise RuntimeError(f"Artifact checksum mismatch: {coordinate}")
    print(f"Verified {name}", flush=True)
    return coordinate, {"url": url, "sha256": digest}

coordinates = [s.strip() for s in (root / "coordinates.txt").read_text().splitlines() if s.strip() and not s.startswith("#")]
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    lock = dict(pool.map(fetch, coordinates))
lock_path.write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n")

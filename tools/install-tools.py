"""Install pinned Kubernetes tooling into the repository, never system-wide."""
import io
from pathlib import Path
import tarfile
import urllib.request

root=Path(__file__).resolve().parents[1]
target=root/".tools/bin";target.mkdir(parents=True,exist_ok=True)
urls={"minikube":"https://storage.googleapis.com/minikube/releases/v1.39.0/minikube-linux-amd64",
      "kubectl":"https://dl.k8s.io/release/v1.36.4/bin/linux/amd64/kubectl",
      "helm":"https://get.helm.sh/helm-v4.3.0-linux-amd64.tar.gz"}
for name,url in urls.items():
    path=target/name
    if path.exists():continue
    data=urllib.request.urlopen(url,timeout=120).read()
    if name=="helm":data=tarfile.open(fileobj=io.BytesIO(data)).extractfile("linux-amd64/helm").read()
    path.write_bytes(data);path.chmod(0o755)
    print(f"Installed {name}")

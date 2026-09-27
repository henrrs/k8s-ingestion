#!/usr/bin/env bash
set -euo pipefail

# One-time host preparation. This script intentionally owns only operating
# system prerequisites; project/cluster resources are handled by
# bootstrap-oracle-e2e.sh and remain reproducible inside the repository.

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
current_user="${SUDO_USER:-$USER}"
repo_owner="$(stat -c '%U' "$repo_root")"
if [ "$current_user" = "root" ] && [ "$repo_owner" != "root" ]; then
  current_user="$repo_owner"
fi

if ! command -v apt-get >/dev/null 2>&1; then
  echo "Unsupported host: this installer currently expects Ubuntu/Debian." >&2
  exit 1
fi

echo "Installing Docker Engine, Python venv and rootless UID helpers..."
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  docker.io python3-venv uidmap ca-certificates
sudo systemctl enable --now docker

if ! getent group docker | cut -d: -f4 | tr ',' '\n' | grep -Fxq "$current_user"; then
  sudo usermod -aG docker "$current_user"
fi

python3 "$repo_root/tools/install-tools.py"

echo
echo "Host prerequisites installed."
echo "Docker daemon: $(sudo docker version --format '{{.Server.Version}}')"
echo "Minikube: $($repo_root/.tools/bin/minikube version --short)"
echo "kubectl: $($repo_root/.tools/bin/kubectl version --client=true 2>/dev/null | head -1)"
echo
echo "The docker group was configured for ${current_user}."
echo "Return to: $repo_root"
echo "The bootstrap can enter the new group automatically; run:"
echo "  bash tools/bootstrap-oracle-e2e.sh"

#!/usr/bin/env bash
# tune_os.sh — Apply OS-level settings required for 1M+ connections.
#
# Run once on every gateway node (and k6 agent) before load testing.
# Requires root / sudo.
#
# Usage:
#   sudo bash bench/scripts/tune_os.sh

set -euo pipefail

echo "==> Raising file-descriptor limits"
# Allow each process to open up to 2M file descriptors.
ulimit -n 2000000 2>/dev/null || echo "  (ulimit failed — check /etc/security/limits.conf)"

# Persist across reboots (add to /etc/security/limits.conf if not already present).
for line in \
    "* soft nofile 2000000" \
    "* hard nofile 2000000"; do
  grep -qxF "$line" /etc/security/limits.conf 2>/dev/null || echo "$line" | tee -a /etc/security/limits.conf
done

echo "==> Tuning kernel network parameters"
sysctl -w net.core.somaxconn=65535
sysctl -w net.core.netdev_max_backlog=65536
sysctl -w net.ipv4.tcp_max_syn_backlog=65536
sysctl -w net.ipv4.ip_local_port_range="1024 65535"
sysctl -w fs.file-max=2500000

# Persist
cat >> /etc/sysctl.d/99-turbocable.conf <<'SYSCTL'
net.core.somaxconn         = 65535
net.core.netdev_max_backlog = 65536
net.ipv4.tcp_max_syn_backlog = 65536
net.ipv4.ip_local_port_range = 1024 65535
fs.file-max                = 2500000
SYSCTL
sysctl -p /etc/sysctl.d/99-turbocable.conf

echo "==> Done. Verify with:"
echo "    ulimit -n          # should be 2000000"
echo "    sysctl net.core.somaxconn  # should be 65535"

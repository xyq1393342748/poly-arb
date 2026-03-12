#!/usr/bin/env bash

set -euo pipefail

sysctl -w net.ipv4.tcp_quickack=1
sysctl -w net.ipv4.tcp_low_latency=1
sysctl -w net.ipv4.tcp_rmem="4096 16384 65536"
sysctl -w net.ipv4.tcp_wmem="4096 16384 65536"
sysctl -w net.ipv4.tcp_fastopen=3
sysctl -w net.ipv4.tcp_slow_start_after_idle=0

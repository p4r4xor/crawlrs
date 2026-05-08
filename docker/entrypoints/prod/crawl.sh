#!/bin/bash
# Production entrypoint. Hands off to the binary with whatever args the
# K8s manifest passed (chart's StatefulSet spec.containers[0].args).
#
# `exec` is load-bearing: tini (PID 1) needs the crawler at PID 2 so
# SIGTERM propagates to the binary's graceful-shutdown path (mark
# /readyz unhealthy -> drain workers -> flush stores -> exit).
#
# Reserved as the hook point for env-var setup (e.g. secret-file
# unwrap) without baking that into the binary. Empty for now.

set -euo pipefail

exec crawlrs "$@"

#!/usr/bin/env bash
# Generate Python client stubs (for the Pi) from proto/.
#
#   pip install grpcio-tools
#   proto/gen-python.sh <output-dir>      # set PYTHON=... to pick an interpreter
#
# Output lands in <output-dir>/skycam/v1/. The Pi imports it as
#   from skycam.v1 import skycam_pb2, skycam_pb2_grpc
set -euo pipefail

out="${1:?usage: proto/gen-python.sh <output-dir>}"
root="$(cd "$(dirname "$0")" && pwd)"

mkdir -p "$out"
"${PYTHON:-python}" -m grpc_tools.protoc \
  -I "$root" \
  --python_out="$out" \
  --pyi_out="$out" \
  --grpc_python_out="$out" \
  "$root/skycam/v1/skycam.proto"

# Make the generated dirs importable as packages.
touch "$out/skycam/__init__.py" "$out/skycam/v1/__init__.py"
echo "generated Python stubs in $out/skycam/v1"

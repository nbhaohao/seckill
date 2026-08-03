#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2

export PATH="$(go env GOPATH)/bin:$PATH"
protoc \
  --proto_path=proto \
  --go_out=. \
  --go_opt=module=github.com/nbhaohao/go-seckill \
  --go-grpc_out=. \
  --go-grpc_opt=module=github.com/nbhaohao/go-seckill \
  proto/order/v1/order.proto \
  proto/inventory/v1/inventory.proto

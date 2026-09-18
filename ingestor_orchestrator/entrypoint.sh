#!/bin/sh
# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

set -eu

go_pid=""
api_pid=""

terminate() {
    status="${1:-0}"
    trap - INT TERM EXIT
    [ -n "$api_pid" ] && kill "$api_pid" 2>/dev/null || true
    [ -n "$go_pid" ] && kill "$go_pid" 2>/dev/null || true
    [ -n "$api_pid" ] && wait "$api_pid" 2>/dev/null || true
    [ -n "$go_pid" ] && wait "$go_pid" 2>/dev/null || true
    exit "$status"
}

trap 'terminate $?' EXIT
trap 'exit 143' INT TERM

/usr/local/bin/orchestrator-go &
go_pid=$!
/usr/local/bin/orchestrator-go wait http://127.0.0.1:8081/ready

"$@" &
api_pid=$!

while kill -0 "$go_pid" 2>/dev/null && kill -0 "$api_pid" 2>/dev/null; do
    sleep 1
done

if kill -0 "$go_pid" 2>/dev/null; then
    wait "$api_pid" || status=$?
else
    wait "$go_pid" || status=$?
fi
terminate "${status:-0}"

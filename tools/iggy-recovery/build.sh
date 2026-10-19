#!/bin/sh
# Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
# Licensed under the GNU Affero General Public License, version 3 or later.
set -eu

if [ "$#" -lt 1 ]; then
    echo 'Usage: sh tools/iggy-recovery/build.sh IMAGE [buildx options, e.g. --load or --push]' >&2
    exit 2
fi

revision=d2f970661f44da60cc6cd0510d1c999a2a70bda3
destination=$1
shift
exec docker buildx build \
    --platform "${IGGY_BUILD_PLATFORM:-linux/arm64}" \
    --label "org.opencontainers.image.source=https://github.com/apache/iggy" \
    --label "org.opencontainers.image.revision=${revision}" \
    --tag "$destination" \
    --file Dockerfile \
    "$@" \
    "https://github.com/apache/iggy.git#${revision}"

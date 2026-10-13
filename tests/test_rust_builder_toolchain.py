# Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
# Licensed under the GNU Affero General Public License, version 3 or later.

import re
import tomllib
from pathlib import Path

import pytest


@pytest.mark.parametrize("dockerfile", ["Dockerfile", "Dockerfile.builder.dev"])
def test_rust_builder_image_matches_pinned_toolchain(dockerfile):
    workspace = Path(__file__).resolve().parents[1] / "ckan-ingestor-rs"
    toolchain = tomllib.loads((workspace / "rust-toolchain.toml").read_text())
    channel = toolchain["toolchain"]["channel"]
    assert re.fullmatch(r"nightly-\d{4}-\d{2}-\d{2}", channel)

    expected_image = f"rustlang/rust:nightly-slim-{channel.removeprefix('nightly-')}"
    images = re.findall(r"^FROM\s+(\S+)", (workspace / dockerfile).read_text(), re.M)
    rust_images = [image for image in images if image.startswith("rustlang/rust:")]
    assert rust_images == [expected_image]

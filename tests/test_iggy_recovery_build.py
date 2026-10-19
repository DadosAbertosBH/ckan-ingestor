# Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
# Licensed under the GNU Affero General Public License, version 3 or later.

import subprocess
from pathlib import Path


def test_recovery_image_build_rejects_a_missing_destination():
    script = Path("tools/iggy-recovery/build.sh")
    assert script.is_file(), "the candidate broker needs a reproducible build entrypoint"
    result = subprocess.run(["sh", str(script)], capture_output=True, text=True)
    assert result.returncode == 2
    assert "IMAGE" in result.stderr


def test_recovery_image_source_is_immutable_and_has_provenance():
    script = Path("tools/iggy-recovery/build.sh")
    assert script.is_file(), "the candidate broker needs a reproducible build entrypoint"
    text = script.read_text()
    assert "d2f970661f44da60cc6cd0510d1c999a2a70bda3" in text
    assert "org.opencontainers.image.revision=" in text
    assert "https://github.com/apache/iggy.git#${revision}" in text
    assert "--platform" in text

#!/usr/bin/env python3
"""Drop-in shim for scitrera-repo-tools `generate-ci-gha`.

Copy this file into any repo's `scripts/` directory (or run it from anywhere)
to generate GitHub Actions workflows from that repo's `versions.yaml`.

Resolution order:

  1. Package already importable in the current Python  ->  call directly.

  2. `uvx` (or `uv`) on PATH  ->  run via uvx with no persistent install.
     Set `REPO_TOOLS_SOURCE` to override the package source. Default:
     `git+https://github.com/scitrera/repo-tools.git`.

  3. Otherwise  ->  print install instructions and exit 1.

Usage:
    python scripts/generate-ci-gha.py            # write missing, diff drift, exit 1 on drift
    python scripts/generate-ci-gha.py --force    # overwrite drift
    python scripts/generate-ci-gha.py --check    # never write; CI-friendly drift detector

All flags pass through to `generate-ci-gha`.
"""

#  Copyright (c) 2026. Scitrera LLC. Licensed under 3-clause BSD license
#  (see LICENSE file at https://github.com/scitrera/repo-tools/blob/main/LICENSE)

from __future__ import annotations

import os
import shutil
import sys
from typing import List

DEFAULT_SOURCE = "git+https://github.com/scitrera/repo-tools.git"


def _try_uvx(args: List[str]) -> None:
    """Replace the current process with a uvx invocation."""
    uv = shutil.which("uvx") or shutil.which("uv")
    if uv is None:
        return
    source = os.environ.get("REPO_TOOLS_SOURCE", DEFAULT_SOURCE)
    name = os.path.basename(uv)
    if name == "uv":
        cmd = [uv, "tool", "run", "--from", source, "generate-ci-gha", *args]
    else:
        cmd = [uv, "--from", source, "generate-ci-gha", *args]
    os.execvp(cmd[0], cmd)  # never returns


def _try_import(args: List[str]) -> bool:
    try:
        from scitrera_repo_tools.ci_gen_gha.cli import main as ci_main
    except ImportError:
        return False
    sys.argv = ["generate-ci-gha", *args]
    ci_main()
    return True


def main(argv: List[str]) -> int:
    if _try_import(argv):
        return 0
    _try_uvx(argv)
    source = os.environ.get("REPO_TOOLS_SOURCE", DEFAULT_SOURCE)
    sys.stderr.write(
        "scitrera-repo-tools is not available.\n"
        "Install one of:\n"
        f"  uv tool install --from {source} scitrera-repo-tools\n"
        f"  pip install {source}\n"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

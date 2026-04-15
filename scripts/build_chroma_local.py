#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PIPELINE_SCRIPT = Path(__file__).with_name('sql_vector_pipeline.py')


def main() -> int:
    command = [sys.executable, str(PIPELINE_SCRIPT), 'rebuild', *sys.argv[1:]]
    return subprocess.call(command)


if __name__ == '__main__':
    raise SystemExit(main())

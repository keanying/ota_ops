#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""便捷入口：等价于 ``python -m ctrip.cli``。

示例::

    python run.py login --account-id A001 --no-headless
    python run.py collect --job all
    python run.py collect --job all --sink both
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ctrip.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.logger import log


def main() -> int:
    log.info("windows-console-smoke: 启动阶段 -> 中文日志链检测")
    log.info("windows-console-smoke: 运行阶段 -> emoji 检测 ✅")
    log.warning("windows-console-smoke: 异常阶段 -> 特殊字符 ñöç 漢字 العربية")
    log.error("windows-console-smoke: 关闭阶段 -> 控制台输出链收尾")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

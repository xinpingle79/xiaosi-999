from loguru import logger
import sys
import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent


def _runtime_dir():
    runtime_root = (os.environ.get("FB_RPA_RUNTIME_DIR") or "").strip()
    if runtime_root:
        base = Path(runtime_root).expanduser()
    else:
        base = BASE_DIR / "runtime"
    base.mkdir(parents=True, exist_ok=True)
    return base

def setup_logger():
    log_dir = _runtime_dir() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 配置日志格式
    logger.remove()  # 移除默认配置
    
    # 输出到终端
    logger.add(sys.stdout, colorize=True, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")
    
    # 输出到文件
    logger.add(str(log_dir / "runtime_{time}.log"), rotation="500 MB", encoding="utf-8", enqueue=True)
    
    return logger

# 初始化
log = setup_logger()

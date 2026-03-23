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


def _console_sink(message):
    text = str(message)
    stream = sys.stdout or getattr(sys, "__stdout__", None)
    fallback = getattr(sys, "__stdout__", None) or stream
    if stream is None and fallback is None:
        return
    if os.name == "nt" and stream is not None and hasattr(stream, "reconfigure"):
        try:
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
        except Exception:
            pass
    try:
        stream.write(text)
    except UnicodeEncodeError:
        encoding = getattr(stream, "encoding", None) or getattr(fallback, "encoding", None) or "utf-8"
        safe_text = text.encode(encoding, errors="backslashreplace").decode(
            encoding,
            errors="ignore",
        )
        try:
            (stream or fallback).write(safe_text)
        except Exception:
            if fallback is not None:
                fallback.write(text.encode("utf-8", errors="backslashreplace").decode("utf-8"))
    except Exception:
        try:
            if fallback is not None:
                fallback.write(text.encode("utf-8", errors="backslashreplace").decode("utf-8"))
        except Exception:
            return
    try:
        (stream or fallback).flush()
    except Exception:
        try:
            if fallback is not None:
                fallback.flush()
        except Exception:
            pass

def setup_logger():
    log_dir = _runtime_dir() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 配置日志格式
    logger.remove()  # 移除默认配置
    
    # 输出到终端
    logger.add(
        _console_sink,
        colorize=(os.name != "nt"),
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    )
    
    # 输出到文件
    logger.add(str(log_dir / "runtime_{time}.log"), rotation="500 MB", encoding="utf-8")
    
    return logger

# 初始化
log = setup_logger()

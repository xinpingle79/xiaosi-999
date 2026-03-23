#!/usr/bin/env python3
from __future__ import annotations

import fnmatch
import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CHECK_DIRS = ("dist", "Output")
RELEASE_ROOT = "dist/FB_RPA_Client"
FORBIDDEN_TRACKED_PATTERNS = (
    "runtime/*",
    "runtime/**",
    "*.db",
    "*.db-wal",
    "*.db-shm",
    "*.sqlite",
    "*.sqlite3",
    "*.sqlite-wal",
    "*.sqlite-shm",
    "*.log",
    "*.cookies",
    "*.session",
    "*.session-*",
    ".DS_Store",
    "__MACOSX/*",
)
FORBIDDEN_RELEASE_PATTERNS = (
    "runtime/*",
    "runtime/**",
    ".git/*",
    ".git/**",
    "__MACOSX/*",
    "__MACOSX/**",
    "*.db",
    "*.db-wal",
    "*.db-shm",
    "*.sqlite",
    "*.sqlite3",
    "*.sqlite-wal",
    "*.sqlite-shm",
    "*.log",
    "*.cookies",
    "*.session",
    "*.session-*",
    ".DS_Store",
    "config/server.yaml",
    "config/client.yaml",
    "config/messages.server.yaml",
)
EXPECTED_RELEASE_FILES = {
    "dist/FB_RPA_Client/FB_RPA_Client.exe",
    "dist/FB_RPA_Client/FB_RPA_Worker.exe",
    "dist/FB_RPA_Client/FB_RPA_Main.exe",
    "dist/FB_RPA_Client/config/client.example.yaml",
    "dist/FB_RPA_Client/config/messages.yaml",
}
SENSITIVE_RULES = (
    (
        "yaml_password_literal",
        re.compile(r'^\s*password\s*:\s*("?)([^"\n]*)\1\s*$', re.IGNORECASE),
        {
            "",
            "CHANGE_ME",
            "REPLACE_ME",
            "YOUR_PASSWORD_HERE",
            "your-password-here",
        },
    ),
    (
        "yaml_api_token_literal",
        re.compile(r'^\s*api_token\s*:\s*("?)([^"\n]*)\1\s*$', re.IGNORECASE),
        {
            "",
            "CHANGE_ME",
            "REPLACE_ME",
            "YOUR_API_TOKEN_HERE",
            "your-api-token-here",
        },
    ),
    (
        "yaml_agent_token_literal",
        re.compile(r'^\s*agent_token\s*:\s*("?)([^"\n]*)\1\s*$', re.IGNORECASE),
        {
            "",
            "CHANGE_ME",
            "REPLACE_ME",
            "YOUR_AGENT_TOKEN_HERE",
            "your-agent-token-here",
        },
    ),
    (
        "preview_token_literal",
        re.compile(r'value="([A-Za-z0-9_-]{16,})"'),
        {
            "",
            "CHANGE_ME",
            "REPLACE_ME",
            "REDACTED",
        },
    ),
)
SENSITIVE_TEXT_FILE_SUFFIXES = {
    ".py",
    ".js",
    ".json",
    ".html",
    ".md",
    ".txt",
    ".toml",
    ".yaml",
    ".yml",
}


def _match_any(path_text: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatch(path_text, pattern) for pattern in patterns)


def _collect_tracked_forbidden(root: Path) -> list[str]:
    git_index = root / ".git" / "index"
    if not git_index.exists():
        return []
    import subprocess

    proc = subprocess.run(
        ["git", "-C", str(root), "ls-files"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "git ls-files failed")
    return [
        rel
        for rel in proc.stdout.splitlines()
        if rel and _match_any(rel, FORBIDDEN_TRACKED_PATTERNS)
    ]


def _collect_release_forbidden(root: Path) -> list[str]:
    hits: list[str] = []
    for dirname in CHECK_DIRS:
        candidate = root / dirname
        if not candidate.exists():
            continue
        for path in candidate.rglob("*"):
            if path.is_dir():
                continue
            rel = path.relative_to(root).as_posix()
            if _match_any(rel, FORBIDDEN_RELEASE_PATTERNS):
                hits.append(rel)
    return sorted(set(hits))


def _collect_release_unexpected(root: Path) -> list[str]:
    candidate = root / RELEASE_ROOT
    if not candidate.exists():
        return []
    hits: list[str] = []
    for path in candidate.rglob("*"):
        if path.is_dir():
            continue
        rel = path.relative_to(root).as_posix()
        if rel not in EXPECTED_RELEASE_FILES:
            hits.append(rel)
    return sorted(set(hits))


def _iter_tracked_text_files(root: Path):
    import subprocess

    proc = subprocess.run(
        ["git", "-C", str(root), "ls-files"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "git ls-files failed")
    for rel in proc.stdout.splitlines():
        if not rel:
            continue
        path = root / rel
        if path.suffix.lower() not in SENSITIVE_TEXT_FILE_SUFFIXES:
            continue
        if not path.exists() or not path.is_file():
            continue
        yield rel, path


def _value_is_placeholder(value: str, placeholders: set[str]) -> bool:
    normalized = str(value or "").strip().strip('"').strip("'")
    if not normalized:
        return True
    if normalized in placeholders:
        return True
    return normalized.startswith(("DEMO-", "REDACTED-", "PLACEHOLDER-"))


def _collect_tracked_sensitive_literals(root: Path) -> list[str]:
    hits: list[str] = []
    for rel, path in _iter_tracked_text_files(root):
        suffix = path.suffix.lower()
        preview_mode = path.name == "client_ui_preview.html" or "preview" in rel.lower()
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue
        for line_no, line in enumerate(lines, start=1):
            for rule_name, pattern, placeholders in SENSITIVE_RULES:
                if rule_name.startswith("yaml_") and suffix not in {".yaml", ".yml"}:
                    continue
                if rule_name == "preview_token_literal" and not preview_mode:
                    continue
                match = pattern.search(line)
                if not match:
                    continue
                value = match.group(match.lastindex or 0)
                if _value_is_placeholder(value, placeholders):
                    continue
                hits.append(f"{rel}:{line_no}:{rule_name}")
    return hits


def main() -> int:
    tracked_hits = _collect_tracked_forbidden(REPO_ROOT)
    release_hits = _collect_release_forbidden(REPO_ROOT)
    release_unexpected = _collect_release_unexpected(REPO_ROOT)
    sensitive_hits = _collect_tracked_sensitive_literals(REPO_ROOT)

    if not tracked_hits and not release_hits and not release_unexpected and not sensitive_hits:
        print("release_preflight: OK")
        return 0

    print("release_preflight: FAILED")
    if tracked_hits:
        print("tracked_forbidden:")
        for item in tracked_hits:
            print(f"  - {item}")
    if release_hits:
        print("release_outputs_forbidden:")
        for item in release_hits:
            print(f"  - {item}")
    if release_unexpected:
        print("release_outputs_unexpected:")
        for item in release_unexpected:
            print(f"  - {item}")
    if sensitive_hits:
        print("tracked_sensitive_literals:")
        for item in sensitive_hits:
            print(f"  - {item}")
    return 1


if __name__ == "__main__":
    sys.exit(main())

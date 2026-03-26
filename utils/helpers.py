import json
import os
import sqlite3
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

from utils.logger import log


BASE_DIR = Path(__file__).resolve().parent.parent
ENV_STOP_FLAG = "FB_RPA_STOP_FLAG"
ENV_PAUSE_FLAG = "FB_RPA_PAUSE_FLAG"


def _runtime_dir():
    runtime_root = (os.environ.get("FB_RPA_RUNTIME_DIR") or "").strip()
    if runtime_root:
        base = Path(runtime_root).expanduser()
    else:
        base = BASE_DIR / "runtime"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _runtime_path(*parts):
    return _runtime_dir().joinpath(*parts)


def runtime_flag_active(env_name):
    flag_file = (os.environ.get(str(env_name or "")) or "").strip()
    return bool(flag_file and os.path.exists(flag_file))


def wait_if_runtime_paused(
    pause_env=ENV_PAUSE_FLAG,
    stop_env=ENV_STOP_FLAG,
    poll_seconds=0.1,
    on_pause=None,
    on_resume=None,
):
    if not runtime_flag_active(pause_env):
        return 0.0, False
    if callable(on_pause):
        try:
            on_pause()
        except Exception:
            pass
    paused_start = time.time()
    poll_seconds = max(0.03, float(poll_seconds or 0.03))
    while runtime_flag_active(pause_env):
        if stop_env and runtime_flag_active(stop_env):
            return max(0.0, time.time() - paused_start), True
        time.sleep(poll_seconds)
    paused_seconds = max(0.0, time.time() - paused_start)
    # `stop` may clear the pause flag first. Re-check once here so a stop request
    # does not get misclassified as a resume.
    if stop_env and runtime_flag_active(stop_env):
        return paused_seconds, True
    if callable(on_resume):
        try:
            on_resume()
        except Exception:
            pass
    return paused_seconds, False


def run_pause_aware_action(
    action,
    timeout_ms,
    pause_env=ENV_PAUSE_FLAG,
    stop_env=None,
    slice_ms=220,
    retry_interval=0.05,
    on_pause=None,
    on_resume=None,
):
    timeout_ms = max(180, int(timeout_ms or 180))
    slice_ms = max(80, min(int(slice_ms or 80), timeout_ms))
    retry_interval = max(0.01, float(retry_interval or 0.01))
    deadline = time.time() + timeout_ms / 1000
    last_error = None
    while time.time() < deadline:
        paused_seconds, stopped = wait_if_runtime_paused(
            pause_env=pause_env,
            stop_env=stop_env,
            poll_seconds=0.1,
            on_pause=on_pause,
            on_resume=on_resume,
        )
        if paused_seconds:
            deadline += paused_seconds
        if stopped:
            raise TimeoutError("pause_aware_action_stopped")
        remaining_ms = max(80, int((deadline - time.time()) * 1000))
        try:
            return action(min(slice_ms, remaining_ms))
        except Exception as exc:
            last_error = exc
        time.sleep(retry_interval)
    if last_error is not None:
        raise last_error
    raise TimeoutError("pause_aware_action_timeout")


def _is_uuid_code(value):
    if not value:
        return False
    try:
        uuid.UUID(str(value))
        return True
    except Exception:
        return False


def _generate_uuid_code(existing=None):
    existing = existing or set()
    while True:
        code = str(uuid.uuid4()).upper()
        if code not in existing:
            return code


class Database:
    LEGACY_GROUP_SCOPE = "__legacy_global__"

    def __init__(self, db_path=None):
        if not db_path or db_path == "data/processed_users.db":
            db_path = str(_runtime_path("data", "server.db"))
        elif not os.path.isabs(db_path):
            db_path = str(_runtime_path(db_path))
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self._init_db()

    def _init_db(self):
        self._migrate_sent_history()
        self._migrate_send_claims()
        self._migrate_run_cursor()
        self._migrate_group_status()
        self._migrate_message_templates()
        self._migrate_user_accounts()
        self._migrate_agent_registry()
        self._migrate_device_configs()
        self._migrate_agent_tasks()
        self._migrate_sessions()
        self.conn.commit()

    def _migrate_sent_history(self):
        columns = self._fetch_table_columns("sent_history")
        pk_rows = self.conn.execute("PRAGMA table_info(sent_history)").fetchall() if columns else []
        pk_map = {row["name"]: row["pk"] for row in pk_rows}
        if not columns:
            self._create_sent_history_table("sent_history")
            self._ensure_sent_history_indexes()
            return

        expected_columns = {
            "scope_id",
            "group_id",
            "profile_url",
            "user_id",
            "account_id",
            "status",
            "created_at",
            "machine_id",
        }
        if (
            expected_columns.issubset(columns)
            and pk_map.get("scope_id") == 1
            and pk_map.get("group_id") == 2
            and pk_map.get("profile_url") == 3
        ):
            self._ensure_sent_history_indexes()
            return

        profile_column = "profile_url" if "profile_url" in columns else "url"
        legacy_scope = self.LEGACY_GROUP_SCOPE.replace("'", "''")
        group_column = (
            f"COALESCE(NULLIF(group_id, ''), '{legacy_scope}') AS group_id"
            if "group_id" in columns
            else f"'{legacy_scope}' AS group_id"
        )
        user_id_column = "user_id" if "user_id" in columns else "NULL AS user_id"
        account_column = "account_id" if "account_id" in columns else "NULL AS account_id"
        scope_column = "scope_id" if "scope_id" in columns else "NULL AS scope_id"
        status_column = "status" if "status" in columns else "1 AS status"
        created_column = "created_at" if "created_at" in columns else "NULL AS created_at"
        machine_id_column = "machine_id" if "machine_id" in columns else "NULL AS machine_id"

        rows = self.conn.execute(
            f"""
            SELECT
                {group_column},
                {profile_column} AS profile_url,
                {user_id_column},
                {account_column},
                {scope_column},
                {status_column},
                {created_column},
                {machine_id_column}
            FROM sent_history
            """
        ).fetchall()

        deduped_rows = self._dedupe_sent_history(rows)

        self.conn.execute("DROP TABLE IF EXISTS sent_history_new")
        self._create_sent_history_table("sent_history_new")
        for row in deduped_rows:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO sent_history_new
                (scope_id, account_id, group_id, profile_url, user_id, status, created_at, machine_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["scope_id"],
                    row["account_id"],
                    row["group_id"],
                    row["profile_url"],
                    row["user_id"],
                    row["status"],
                    row["created_at"],
                    row["machine_id"],
                ),
            )

        self.conn.execute("DROP TABLE sent_history")
        self.conn.execute("ALTER TABLE sent_history_new RENAME TO sent_history")
        self._ensure_sent_history_indexes()

    def _create_sent_history_table(self, table_name):
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                scope_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                profile_url TEXT NOT NULL,
                user_id TEXT,
                status INTEGER,
                created_at DATETIME,
                machine_id TEXT,
                PRIMARY KEY (scope_id, group_id, profile_url)
            )
            """
        )

    def _dedupe_sent_history(self, rows):
        normalized_rows = {}
        for row in rows:
            group_id = self.normalize_group_id(row["group_id"], allow_legacy=True)
            if not group_id:
                continue
            profile_url = self.normalize_profile_url(row["profile_url"])
            if not profile_url:
                continue

            user_id = row["user_id"] or self.extract_user_id(profile_url)
            account_id = self.normalize_account_scope(row["account_id"])
            scope_id = self.normalize_legacy_scope_id(row["scope_id"], account_id=account_id)
            candidate = {
                "scope_id": scope_id,
                "account_id": account_id,
                "group_id": group_id,
                "profile_url": profile_url,
                "user_id": user_id,
                "status": 1 if row["status"] is None else row["status"],
                "created_at": row["created_at"] or "1970-01-01 00:00:00",
                "machine_id": str(row["machine_id"] or "").strip() or None,
            }
            key = (
                f"{scope_id}|{group_id}|user:{user_id}"
                if user_id
                else f"{scope_id}|{group_id}|url:{profile_url}"
            )
            current = normalized_rows.get(key)
            if not current or candidate["created_at"] >= current["created_at"]:
                normalized_rows[key] = candidate
        return list(normalized_rows.values())

    def _ensure_sent_history_indexes(self):
        self.conn.execute("DROP INDEX IF EXISTS idx_sent_history_user_id")
        self.conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_sent_history_group_user_id
            ON sent_history(scope_id, group_id, user_id)
            """
        )

    def _migrate_send_claims(self):
        columns = self._fetch_table_columns("send_claims")
        pk_rows = self.conn.execute("PRAGMA table_info(send_claims)").fetchall() if columns else []
        pk_map = {row["name"]: row["pk"] for row in pk_rows}
        expected_columns = {"group_id", "profile_url", "user_id", "account_id", "scope_id", "claimed_at"}
        if (
            columns
            and expected_columns.issubset(columns)
            and pk_map.get("scope_id") == 1
            and pk_map.get("group_id") == 2
            and pk_map.get("profile_url") == 3
        ):
            self._ensure_send_claim_indexes()
            return

        rows = []
        if columns:
            rows = self.conn.execute(
                """
                SELECT group_id, profile_url, user_id, account_id,
                       {scope_column}, claimed_at
                FROM send_claims
                """.format(scope_column="scope_id" if "scope_id" in columns else "NULL AS scope_id")
            ).fetchall()
            self.conn.execute("DROP TABLE send_claims")
        self._create_send_claims_table("send_claims")
        for row in rows:
            normalized_group_id = self.normalize_group_id(row["group_id"], allow_legacy=True)
            normalized_profile_url = self.normalize_profile_url(row["profile_url"])
            if not normalized_group_id or not normalized_profile_url:
                continue
            normalized_user_id = row["user_id"] or self.extract_user_id(normalized_profile_url)
            normalized_account_id = self.normalize_account_scope(row["account_id"])
            normalized_scope_id = self.normalize_legacy_scope_id(row["scope_id"], account_id=normalized_account_id)
            self.conn.execute(
                """
                INSERT OR REPLACE INTO send_claims
                (scope_id, account_id, group_id, profile_url, user_id, claimed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_scope_id,
                    normalized_account_id,
                    normalized_group_id,
                    normalized_profile_url,
                    normalized_user_id,
                    row["claimed_at"],
                ),
            )
        self._ensure_send_claim_indexes()

    def _create_send_claims_table(self, table_name):
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                scope_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                profile_url TEXT NOT NULL,
                user_id TEXT,
                claimed_at DATETIME NOT NULL,
                PRIMARY KEY (scope_id, group_id, profile_url)
            )
            """
        )

    def _ensure_send_claim_indexes(self):
        self.conn.execute("DROP INDEX IF EXISTS idx_send_claims_user_id")
        self.conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_send_claims_group_user_id
            ON send_claims(scope_id, group_id, user_id)
            """
        )

    def _migrate_device_configs(self):
        columns = self._fetch_table_columns("device_configs")
        if not columns:
            self._create_device_configs_table("device_configs")
            self._ensure_device_configs_indexes()
            return

        expected = {
            "id",
            "owner",
            "name",
            "machine_id",
            "bit_api",
            "api_token",
            "status",
            "created_at",
            "updated_at",
        }
        missing = expected.difference(columns)
        for column in missing:
            if column == "name":
                self.conn.execute("ALTER TABLE device_configs ADD COLUMN name TEXT")
            elif column == "machine_id":
                self.conn.execute("ALTER TABLE device_configs ADD COLUMN machine_id TEXT")
            elif column == "bit_api":
                self.conn.execute("ALTER TABLE device_configs ADD COLUMN bit_api TEXT")
            elif column == "api_token":
                self.conn.execute("ALTER TABLE device_configs ADD COLUMN api_token TEXT")
            elif column == "status":
                self.conn.execute("ALTER TABLE device_configs ADD COLUMN status INTEGER")
            elif column == "created_at":
                self.conn.execute("ALTER TABLE device_configs ADD COLUMN created_at DATETIME")
            elif column == "updated_at":
                self.conn.execute("ALTER TABLE device_configs ADD COLUMN updated_at DATETIME")
        self._ensure_device_configs_indexes()

    def _create_device_configs_table(self, table_name):
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner TEXT NOT NULL,
                name TEXT,
                machine_id TEXT,
                bit_api TEXT,
                api_token TEXT,
                status INTEGER DEFAULT 1,
                created_at DATETIME,
                updated_at DATETIME
            )
            """
        )

    def _ensure_device_configs_indexes(self):
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_device_configs_owner ON device_configs(owner)"
        )

    def _migrate_run_cursor(self):
        info_rows = self.conn.execute("PRAGMA table_info(run_cursor)").fetchall()
        if not info_rows:
            self._create_run_cursor_table("run_cursor")
            return

        columns = {row["name"] for row in info_rows}
        pk_map = {row["name"]: row["pk"] for row in info_rows}
        expected_columns = {
            "group_id",
            "profile_url",
            "user_id",
            "name",
            "group_user_url",
            "account_id",
            "scope_id",
            "updated_at",
        }
        if (
            expected_columns.issubset(columns)
            and pk_map.get("scope_id") == 1
            and pk_map.get("group_id") == 2
        ):
            return

        account_column = "account_id" if "account_id" in columns else "NULL AS account_id"
        scope_column = "scope_id" if "scope_id" in columns else "NULL AS scope_id"
        rows = self.conn.execute(
            f"""
            SELECT
                group_id,
                profile_url,
                user_id,
                name,
                group_user_url,
                {account_column},
                {scope_column},
                updated_at
            FROM run_cursor
            """
        ).fetchall()
        deduped_rows = self._dedupe_run_cursor(rows)

        self.conn.execute("DROP TABLE IF EXISTS run_cursor_new")
        self._create_run_cursor_table("run_cursor_new")
        for row in deduped_rows:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO run_cursor_new
                (scope_id, account_id, group_id, profile_url, user_id, name, group_user_url, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["scope_id"],
                    row["account_id"],
                    row["group_id"],
                    row["profile_url"],
                    row["user_id"],
                    row["name"],
                    row["group_user_url"],
                    row["updated_at"],
                ),
            )

        self.conn.execute("DROP TABLE run_cursor")
        self.conn.execute("ALTER TABLE run_cursor_new RENAME TO run_cursor")

    def _migrate_user_accounts(self):
        columns = self._fetch_table_columns("user_accounts")
        if not columns:
            self._create_user_accounts_table()
            self._ensure_user_accounts_indexes()
            return

        expected_columns = {
            "id",
            "username",
            "password_hash",
            "bit_api",
            "api_token",
            "bridge_url",
            "role",
            "status",
            "created_at",
            "send_interval_min",
            "send_interval_max",
            "max_devices",
            "expire_at",
            "activation_code",
        }
        missing = expected_columns.difference(columns)
        for column in missing:
            if column == "status":
                self.conn.execute(
                    "ALTER TABLE user_accounts ADD COLUMN status INTEGER DEFAULT 1"
                )
            elif column == "created_at":
                self.conn.execute(
                    "ALTER TABLE user_accounts ADD COLUMN created_at DATETIME"
                )
            elif column == "password_hash":
                self.conn.execute(
                    "ALTER TABLE user_accounts ADD COLUMN password_hash TEXT"
                )
            elif column == "bit_api":
                self.conn.execute("ALTER TABLE user_accounts ADD COLUMN bit_api TEXT")
            elif column == "api_token":
                self.conn.execute("ALTER TABLE user_accounts ADD COLUMN api_token TEXT")
            elif column == "bridge_url":
                self.conn.execute("ALTER TABLE user_accounts ADD COLUMN bridge_url TEXT")
            elif column == "role":
                self.conn.execute(
                    "ALTER TABLE user_accounts ADD COLUMN role TEXT DEFAULT 'user'"
                )
            elif column == "send_interval_min":
                self.conn.execute(
                    "ALTER TABLE user_accounts ADD COLUMN send_interval_min INTEGER"
                )
            elif column == "send_interval_max":
                self.conn.execute(
                    "ALTER TABLE user_accounts ADD COLUMN send_interval_max INTEGER"
                )
            elif column == "max_devices":
                self.conn.execute(
                    "ALTER TABLE user_accounts ADD COLUMN max_devices INTEGER"
                )
            elif column == "expire_at":
                self.conn.execute(
                    "ALTER TABLE user_accounts ADD COLUMN expire_at REAL"
                )
            elif column == "activation_code":
                self.conn.execute(
                    "ALTER TABLE user_accounts ADD COLUMN activation_code TEXT"
                )
            # id/username handled by initial create; ignore if missing
        self._ensure_user_accounts_indexes()

    def _create_user_accounts_table(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT,
                bit_api TEXT,
                api_token TEXT,
                activation_code TEXT,
                bridge_url TEXT,
                send_interval_min INTEGER,
                send_interval_max INTEGER,
                max_devices INTEGER,
                expire_at REAL,
                role TEXT DEFAULT 'user',
                status INTEGER DEFAULT 1,
                created_at DATETIME NOT NULL
            )
            """
        )

    def _ensure_user_accounts_indexes(self):
        try:
            self.conn.execute(
                """
                UPDATE user_accounts
                SET activation_code = NULL
                WHERE activation_code IS NOT NULL AND trim(activation_code) = ''
                """
            )
            self.conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_user_accounts_activation_code
                ON user_accounts(activation_code)
                """
            )
            self.conn.commit()
        except sqlite3.Error as exc:
            log.warning(f"创建 activation_code 唯一索引失败: {exc}")

    def _migrate_agent_registry(self):
        columns = self._fetch_table_columns("agent_registry")
        if not columns:
            self._create_agent_registry_table()
            return
        expected = {
            "machine_id",
            "label",
            "bridge_port",
            "bridge_url",
            "last_seen",
            "created_at",
            "owner",
            "status",
            "token",
            "token_expires_at",
            "client_version",
        }
        missing = expected.difference(columns)
        for column in missing:
            if column == "label":
                self.conn.execute("ALTER TABLE agent_registry ADD COLUMN label TEXT")
            elif column == "bridge_port":
                self.conn.execute("ALTER TABLE agent_registry ADD COLUMN bridge_port INTEGER")
            elif column == "bridge_url":
                self.conn.execute("ALTER TABLE agent_registry ADD COLUMN bridge_url TEXT")
            elif column == "last_seen":
                self.conn.execute("ALTER TABLE agent_registry ADD COLUMN last_seen REAL")
            elif column == "created_at":
                self.conn.execute("ALTER TABLE agent_registry ADD COLUMN created_at REAL")
            elif column == "owner":
                self.conn.execute("ALTER TABLE agent_registry ADD COLUMN owner TEXT")
            elif column == "status":
                self.conn.execute("ALTER TABLE agent_registry ADD COLUMN status INTEGER DEFAULT 1")
            elif column == "token":
                self.conn.execute("ALTER TABLE agent_registry ADD COLUMN token TEXT")
            elif column == "token_expires_at":
                self.conn.execute("ALTER TABLE agent_registry ADD COLUMN token_expires_at REAL")
            elif column == "client_version":
                self.conn.execute("ALTER TABLE agent_registry ADD COLUMN client_version TEXT")

    def _create_agent_registry_table(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_registry (
                machine_id TEXT NOT NULL PRIMARY KEY,
                label TEXT,
                bridge_port INTEGER,
                bridge_url TEXT,
                last_seen REAL,
                created_at REAL,
                owner TEXT,
                status INTEGER DEFAULT 1,
                token TEXT,
                token_expires_at REAL,
                client_version TEXT
            )
            """
        )

    def _migrate_agent_tasks(self):
        columns = self._fetch_table_columns("agent_tasks")
        if not columns:
            self._create_agent_tasks_table()
            return
        expected = {
            "id",
            "owner",
            "action",
            "window_token",
            "status",
            "assigned_machine_id",
            "payload",
            "error",
            "exit_code",
            "created_at",
            "updated_at",
        }
        missing = expected.difference(columns)
        for column in missing:
            if column == "owner":
                self.conn.execute("ALTER TABLE agent_tasks ADD COLUMN owner TEXT")
            elif column == "action":
                self.conn.execute("ALTER TABLE agent_tasks ADD COLUMN action TEXT")
            elif column == "window_token":
                self.conn.execute("ALTER TABLE agent_tasks ADD COLUMN window_token TEXT")
            elif column == "status":
                self.conn.execute("ALTER TABLE agent_tasks ADD COLUMN status TEXT")
            elif column == "assigned_machine_id":
                self.conn.execute(
                    "ALTER TABLE agent_tasks ADD COLUMN assigned_machine_id TEXT"
                )
            elif column == "payload":
                self.conn.execute("ALTER TABLE agent_tasks ADD COLUMN payload TEXT")
            elif column == "error":
                self.conn.execute("ALTER TABLE agent_tasks ADD COLUMN error TEXT")
            elif column == "exit_code":
                self.conn.execute("ALTER TABLE agent_tasks ADD COLUMN exit_code INTEGER")
            elif column == "created_at":
                self.conn.execute("ALTER TABLE agent_tasks ADD COLUMN created_at REAL")
            elif column == "updated_at":
                self.conn.execute("ALTER TABLE agent_tasks ADD COLUMN updated_at REAL")

    def _create_agent_tasks_table(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner TEXT,
                action TEXT,
                window_token TEXT,
                status TEXT,
                assigned_machine_id TEXT,
                payload TEXT,
                error TEXT,
                exit_code INTEGER,
                created_at REAL,
                updated_at REAL
            )
            """
        )

    def enqueue_agent_task(self, owner, action, window_token=None, machine_id=None, payload=None):
        now = time.time()
        payload_text = json.dumps(payload or {}, ensure_ascii=False)
        cursor = self.conn.execute(
            """
            INSERT INTO agent_tasks
            (owner, action, window_token, status, assigned_machine_id, payload, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                owner,
                action,
                window_token,
                "pending",
                machine_id,
                payload_text,
                now,
                now,
            ),
        )
        self.conn.commit()
        return cursor.lastrowid

    def claim_agent_task(self, machine_id):
        row = self.conn.execute(
            """
            SELECT *
            FROM agent_tasks
            WHERE status = 'pending' AND assigned_machine_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (machine_id,),
        ).fetchone()
        if not row:
            return None
        now = time.time()
        self.conn.execute(
            """
            UPDATE agent_tasks
            SET status = ?, error = ?, updated_at = ?
            WHERE status = 'pending' AND assigned_machine_id = ? AND id <> ?
            """,
            ("failed", "stale_queue_cleanup", now, machine_id, row["id"]),
        )
        self.conn.execute(
            "UPDATE agent_tasks SET status = ?, updated_at = ? WHERE id = ?",
            ("running", now, row["id"]),
        )
        self.conn.commit()
        return dict(row)

    def update_agent_task(self, task_id, status, error=None, exit_code=None):
        now = time.time()
        self.conn.execute(
            """
            UPDATE agent_tasks
            SET status = ?, error = ?, exit_code = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, error, exit_code, now, task_id),
        )
        self.conn.commit()

    def count_agent_tasks(self, status=None, owner=None):
        conditions = []
        params = []
        if status:
            conditions.append("status = ?")
            params.append(status)
        if owner:
            conditions.append("owner = ?")
            params.append(owner)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        row = self.conn.execute(
            f"SELECT COUNT(1) AS total FROM agent_tasks {where}", params
        ).fetchone()
        return int(row["total"] or 0)

    def _migrate_message_templates(self):
        columns = self._fetch_table_columns("message_templates")
        if columns:
            expected = {"username", "templates", "updated_at"}
            if expected.issubset(columns) and "probe_templates" not in columns:
                return
            legacy_rows = self.conn.execute(
                """
                SELECT username, templates, updated_at
                FROM message_templates
                """
            ).fetchall()
            self.conn.execute("DROP TABLE IF EXISTS message_templates")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS message_templates (
                    username TEXT NOT NULL PRIMARY KEY,
                    templates TEXT,
                    updated_at DATETIME
                )
                """
            )
            if legacy_rows:
                self.conn.executemany(
                    """
                    INSERT OR REPLACE INTO message_templates
                    (username, templates, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    [
                        (row["username"], row["templates"], row["updated_at"])
                        for row in legacy_rows
                    ],
                )
            self.conn.commit()
            return
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS message_templates (
                username TEXT NOT NULL PRIMARY KEY,
                templates TEXT,
                updated_at DATETIME
            )
            """
        )

    def _migrate_sessions(self):
        columns = self._fetch_table_columns("sessions")
        if columns:
            expected = {"token", "username", "role", "created_at"}
            if expected.issubset(columns):
                return
            self.conn.execute("DROP TABLE IF EXISTS sessions")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT NOT NULL PRIMARY KEY,
                username TEXT NOT NULL,
                role TEXT NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )

    def get_message_templates(self, username):
        username = (username or "").strip()
        if not username:
            return None
        row = self.conn.execute(
            """
            SELECT templates
            FROM message_templates
            WHERE username = ?
            """,
            (username,),
        ).fetchone()
        if not row:
            return None
        try:
            templates = json.loads(row["templates"]) if row["templates"] else []
        except Exception:
            templates = []
        return {"templates": templates}

    def set_message_templates(self, username, templates):
        username = (username or "").strip()
        if not username:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        templates_payload = json.dumps(templates or [], ensure_ascii=False)
        self.conn.execute(
            """
            INSERT OR REPLACE INTO message_templates
            (username, templates, updated_at)
            VALUES (?, ?, ?)
            """,
            (username, templates_payload, now),
        )
        self.conn.commit()

    def delete_message_templates(self, username):
        username = (username or "").strip()
        if not username:
            return
        self.conn.execute(
            "DELETE FROM message_templates WHERE username = ?",
            (username,),
        )
        self.conn.commit()

    def save_session(self, token, username, role, created_at=None):
        token = (token or "").strip()
        username = (username or "").strip()
        if not token or not username:
            return
        if created_at is None:
            created_at = time.time()
        self.conn.execute(
            """
            INSERT OR REPLACE INTO sessions (token, username, role, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (token, username, role or "user", float(created_at)),
        )
        self.conn.commit()

    def get_session(self, token):
        token = (token or "").strip()
        if not token:
            return None
        row = self.conn.execute(
            "SELECT token, username, role, created_at FROM sessions WHERE token = ?",
            (token,),
        ).fetchone()
        return dict(row) if row else None

    def delete_session(self, token):
        token = (token or "").strip()
        if not token:
            return
        self.conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
        self.conn.commit()

    def delete_sessions_by_username(self, username):
        username = (username or "").strip()
        if not username:
            return
        self.conn.execute("DELETE FROM sessions WHERE username = ?", (username,))
        self.conn.commit()

    def count_user_accounts(self):
        row = self.conn.execute(
            "SELECT COUNT(1) AS total FROM user_accounts"
        ).fetchone()
        return int(row["total"] or 0) if row else 0

    def migrate_activation_codes_to_uuid(self):
        rows = self.conn.execute(
            """
            SELECT id, activation_code
            FROM user_accounts
            WHERE activation_code IS NOT NULL AND activation_code != ''
            """
        ).fetchall()
        existing = set()
        for row in rows:
            code = (row["activation_code"] or "").strip()
            if code:
                existing.add(code.upper())
        updated = 0
        for row in rows:
            code = (row["activation_code"] or "").strip()
            if not _is_uuid_code(code):
                new_code = _generate_uuid_code(existing)
                self.conn.execute(
                    "UPDATE user_accounts SET activation_code = ? WHERE id = ?",
                    (new_code, int(row["id"])),
                )
                existing.add(new_code)
                updated += 1
        if updated:
            self.conn.commit()
        return {"total": len(rows), "updated": updated}

    def list_user_accounts(self):
        rows = self.conn.execute(
            """
            SELECT id, username, activation_code, role, status, created_at,
                   max_devices, expire_at
            FROM user_accounts
            ORDER BY created_at DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def get_user_account(self, username):
        row = self.conn.execute(
            """
            SELECT id, username, password_hash, activation_code, role, status, created_at,
                   max_devices, expire_at
            FROM user_accounts
            WHERE username = ?
            """,
            (username,),
        ).fetchone()
        return dict(row) if row else None

    def get_user_account_by_activation_code(self, activation_code):
        activation_code = (activation_code or "").strip()
        if not activation_code:
            return None
        row = self.conn.execute(
            """
            SELECT id, username, password_hash, activation_code, role, status, created_at,
                   max_devices, expire_at
            FROM user_accounts
            WHERE activation_code = ?
            LIMIT 1
            """,
            (activation_code,),
        ).fetchone()
        return dict(row) if row else None

    def add_user_account(
        self,
        username,
        password_hash,
        role="user",
        max_devices=None,
        expire_at=None,
        activation_code=None,
    ):
        activation_code = (activation_code or "").strip() or None
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.conn.execute(
            """
            INSERT INTO user_accounts (username, password_hash, activation_code, role, status, created_at, max_devices, expire_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (username, password_hash, activation_code, role, 1, now, max_devices, expire_at),
        )
        self.conn.commit()

    def update_user_account(
        self,
        original_username,
        username,
        password_hash,
        role=None,
        max_devices=None,
        expire_at=None,
        activation_code=None,
    ):
        if activation_code is not None:
            activation_code = (activation_code or "").strip() or None
        fields = ["username = ?"]
        # bridge_url is a legacy column kept only for schema compatibility and is
        # intentionally excluded from the active update path.
        params = [username]
        if password_hash is not None:
            fields.append("password_hash = ?")
            params.append(password_hash)
        if role is not None:
            fields.append("role = ?")
            params.append(role)
        if max_devices is not None:
            fields.append("max_devices = ?")
            params.append(int(max_devices))
        if expire_at is not None:
            fields.append("expire_at = ?")
            params.append(expire_at)
        if activation_code is not None:
            fields.append("activation_code = ?")
            params.append(activation_code)
        params.append(original_username)
        self.conn.execute(
            f"UPDATE user_accounts SET {', '.join(fields)} WHERE username = ?",
            tuple(params),
        )
        if username != original_username:
            self.conn.execute(
                "UPDATE message_templates SET username = ? WHERE username = ?",
                (username, original_username),
            )
        self.conn.commit()

    def list_device_configs(self, owner):
        owner = (owner or "").strip()
        if not owner:
            return []
        rows = self.conn.execute(
            """
            SELECT id, owner, name, machine_id, bit_api, api_token, status, created_at, updated_at
            FROM device_configs
            WHERE owner = ?
            ORDER BY id ASC
            """,
            (owner,),
        ).fetchall()
        return [dict(row) for row in rows]

    def list_all_device_configs(self):
        rows = self.conn.execute(
            """
            SELECT id, owner, name, machine_id, bit_api, api_token, status, created_at, updated_at
            FROM device_configs
            ORDER BY owner ASC, id ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def add_device_config(self, owner, name, machine_id, bit_api, api_token, status=1):
        owner = (owner or "").strip()
        if not owner:
            return None
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.conn.execute(
            """
            INSERT INTO device_configs (owner, name, machine_id, bit_api, api_token, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (owner, name, machine_id, bit_api, api_token, int(status), now, now),
        )
        self.conn.commit()
        row = self.conn.execute("SELECT last_insert_rowid() AS id").fetchone()
        return int(row["id"]) if row else None

    def update_device_config(self, config_id, owner, name, machine_id, bit_api, api_token, status=1):
        owner = (owner or "").strip()
        if not owner:
            return False
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.conn.execute(
            """
            UPDATE device_configs
            SET name = ?, machine_id = ?, bit_api = ?, api_token = ?, status = ?, updated_at = ?
            WHERE id = ? AND owner = ?
            """,
            (name, machine_id, bit_api, api_token, int(status), now, int(config_id), owner),
        )
        self.conn.commit()
        return self.conn.total_changes > 0

    def delete_device_config(self, config_id, owner):
        owner = (owner or "").strip()
        if not owner:
            return False
        self.conn.execute(
            "DELETE FROM device_configs WHERE id = ? AND owner = ?",
            (int(config_id), owner),
        )
        self.conn.commit()
        return self.conn.total_changes > 0

    def set_user_activation_code(self, username, activation_code):
        username = (username or "").strip()
        if not username:
            return
        activation_code = (activation_code or "").strip() or None
        self.conn.execute(
            "UPDATE user_accounts SET activation_code = ? WHERE username = ?",
            (activation_code, username),
        )
        self.conn.commit()

    def upsert_agent(
        self,
        machine_id,
        label,
        last_seen,
        owner=None,
        status=1,
        token=None,
        token_expires_at=None,
        client_version=None,
    ):
        machine_id = (machine_id or "").strip()
        if not machine_id:
            return
        now = float(last_seen or time.time())
        existing = self.get_agent(machine_id)
        if existing:
            if owner is None:
                owner = existing.get("owner")
            if status is None:
                status = existing.get("status", 1)
            if token is None:
                token = existing.get("token")
            if token_expires_at is None:
                token_expires_at = existing.get("token_expires_at")
            if client_version is None:
                client_version = existing.get("client_version")
        if status is None:
            status = 1
        if existing:
            self.conn.execute(
                """
                UPDATE agent_registry
                SET label = ?,
                    last_seen = ?,
                    owner = ?,
                    status = ?,
                    token = ?,
                    token_expires_at = ?,
                    client_version = ?
                WHERE machine_id = ?
                """,
                (
                    label,
                    now,
                    owner,
                    int(status),
                    token,
                    float(token_expires_at) if token_expires_at is not None else None,
                    client_version,
                    machine_id,
                ),
            )
        else:
            self.conn.execute(
                """
                INSERT INTO agent_registry
                (machine_id, label, last_seen, created_at, owner, status, token, token_expires_at, client_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    machine_id,
                    label,
                    now,
                    now,
                    owner,
                    int(status),
                    token,
                    float(token_expires_at) if token_expires_at is not None else None,
                    client_version,
                ),
            )
        self.conn.commit()

    def get_agent(self, machine_id):
        machine_id = (machine_id or "").strip()
        if not machine_id:
            return None
        row = self.conn.execute(
            """
            SELECT machine_id, label, last_seen, created_at,
                   owner, status, token, token_expires_at, client_version
            FROM agent_registry
            WHERE machine_id = ?
            """,
            (machine_id,),
        ).fetchone()
        return dict(row) if row else None

    def get_agent_by_token(self, token):
        token = (token or "").strip()
        if not token:
            return None
        row = self.conn.execute(
            """
            SELECT machine_id, label, last_seen, created_at,
                   owner, status, token, token_expires_at, client_version
            FROM agent_registry
            WHERE token = ?
            """,
            (token,),
        ).fetchone()
        return dict(row) if row else None

    def list_agents(self):
        rows = self.conn.execute(
            """
            SELECT machine_id, label, last_seen, created_at,
                   owner, status, token_expires_at, client_version
            FROM agent_registry
            ORDER BY created_at DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def get_latest_agent_for_owner(self, owner, include_disabled=False):
        owner = (owner or "").strip()
        if not owner:
            return None
        sql = """
            SELECT machine_id, label, last_seen, created_at,
                   owner, status, token, token_expires_at, client_version
            FROM agent_registry
            WHERE owner = ?
        """
        params = [owner]
        if not include_disabled:
            sql += " AND status = 1"
        sql += " ORDER BY last_seen DESC, created_at DESC LIMIT 1"
        row = self.conn.execute(sql, tuple(params)).fetchone()
        return dict(row) if row else None

    def get_online_agent_for_owner(self, owner, heartbeat_ttl):
        owner = (owner or "").strip()
        if not owner:
            return None
        cutoff = float(time.time()) - max(float(heartbeat_ttl or 0), 0.0)
        row = self.conn.execute(
            """
            SELECT machine_id, label, last_seen, created_at,
                   owner, status, token, token_expires_at, client_version
            FROM agent_registry
            WHERE owner = ?
              AND status = 1
              AND COALESCE(last_seen, 0) >= ?
            ORDER BY last_seen DESC, created_at DESC
            LIMIT 1
            """,
            (owner, cutoff),
        ).fetchone()
        return dict(row) if row else None

    def count_agents_for_owner(self, owner, include_disabled=False):
        owner = (owner or "").strip()
        if not owner:
            return 0
        if include_disabled:
            row = self.conn.execute(
                "SELECT COUNT(1) AS total FROM agent_registry WHERE owner = ?",
                (owner,),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT COUNT(1) AS total FROM agent_registry WHERE owner = ? AND status = 1",
                (owner,),
            ).fetchone()
        return int(row["total"] or 0) if row else 0

    def update_agent_token(self, machine_id, token, token_expires_at):
        machine_id = (machine_id or "").strip()
        if not machine_id:
            return
        self.conn.execute(
            """
            UPDATE agent_registry
            SET token = ?, token_expires_at = ?
            WHERE machine_id = ?
            """,
            (token, float(token_expires_at) if token_expires_at is not None else None, machine_id),
        )
        self.conn.commit()

    def set_agent_status(self, machine_id, status):
        machine_id = (machine_id or "").strip()
        if not machine_id:
            return
        self.conn.execute(
            "UPDATE agent_registry SET status = ? WHERE machine_id = ?",
            (int(status), machine_id),
        )
        self.conn.commit()

    def unbind_agent(self, machine_id, owner=None):
        machine_id = (machine_id or "").strip()
        if not machine_id:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if owner:
            self.conn.execute(
                "DELETE FROM agent_registry WHERE machine_id = ? AND owner = ?",
                (machine_id, owner),
            )
            self.conn.execute(
                """
                UPDATE device_configs
                SET machine_id = NULL, updated_at = ?
                WHERE owner = ? AND machine_id = ?
                """,
                (now, owner, machine_id),
            )
        else:
            self.conn.execute(
                "DELETE FROM agent_registry WHERE machine_id = ?",
                (machine_id,),
            )
            self.conn.execute(
                """
                UPDATE device_configs
                SET machine_id = NULL, updated_at = ?
                WHERE machine_id = ?
                """,
                (now, machine_id),
            )
        self.conn.commit()

    def mark_agent_seen(self, machine_id, last_seen=None, client_version=None):
        machine_id = (machine_id or "").strip()
        if not machine_id:
            return
        now = float(last_seen or time.time())
        if client_version:
            self.conn.execute(
                "UPDATE agent_registry SET last_seen = ?, client_version = ? WHERE machine_id = ?",
                (now, client_version, machine_id),
            )
        else:
            self.conn.execute(
                "UPDATE agent_registry SET last_seen = ? WHERE machine_id = ?",
                (now, machine_id),
            )
        self.conn.commit()

    def set_user_account_status(self, username, status):
        self.conn.execute(
            "UPDATE user_accounts SET status = ? WHERE username = ?",
            (int(status), username),
        )
        self.conn.commit()

    def delete_user_account(self, username):
        username = (username or "").strip()
        if not username:
            return
        self.conn.execute("DELETE FROM user_accounts WHERE username = ?", (username,))
        self.conn.execute("DELETE FROM message_templates WHERE username = ?", (username,))
        self.conn.execute("DELETE FROM sessions WHERE username = ?", (username,))
        self.conn.execute("DELETE FROM agent_registry WHERE owner = ?", (username,))
        self.conn.commit()

    def set_user_password_hash(self, username, password_hash):
        self.conn.execute(
            "UPDATE user_accounts SET password_hash = ? WHERE username = ?",
            (password_hash, username),
        )
        self.conn.commit()

    def set_user_send_interval(self, username, min_seconds, max_seconds):
        return

    def seed_user_accounts_from_sent_history(self):
        if self.count_user_accounts() > 0:
            return
        rows = self.conn.execute(
            """
            SELECT account_id, MIN(created_at) AS created_at
            FROM sent_history
            WHERE account_id IS NOT NULL AND account_id != ''
            GROUP BY account_id
            ORDER BY MIN(created_at) DESC
            """
        ).fetchall()
        for row in rows:
            username = row["account_id"]
            created_at = row["created_at"] or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.conn.execute(
                """
                INSERT OR IGNORE INTO user_accounts
                (username, password_hash, role, status, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (username, "", "user", 1, created_at),
            )
        self.conn.commit()

    def is_account_disabled(self, username):
        total = self.count_user_accounts()
        if total == 0:
            return False
        row = self.conn.execute(
            "SELECT status FROM user_accounts WHERE username = ?",
            (username,),
        ).fetchone()
        if not row:
            return True
        return int(row["status"] or 0) == 0

    def set_user_role(self, username, role):
        self.conn.execute(
            "UPDATE user_accounts SET role = ? WHERE username = ?",
            (role, username),
        )
        self.conn.commit()

    def _create_run_cursor_table(self, table_name):
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                scope_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                profile_url TEXT NOT NULL,
                user_id TEXT,
                name TEXT,
                group_user_url TEXT,
                updated_at DATETIME,
                PRIMARY KEY (scope_id, group_id)
            )
            """
        )

    def _dedupe_run_cursor(self, rows):
        deduped_rows = {}
        for row in rows:
            group_id = self.normalize_group_id(row["group_id"])
            profile_url = self.normalize_profile_url(row["profile_url"])
            if not group_id or not profile_url:
                continue

            account_id = self.normalize_account_scope(row["account_id"])
            scope_id = self.normalize_legacy_scope_id(row["scope_id"], account_id=account_id)
            candidate = {
                "scope_id": scope_id,
                "account_id": account_id,
                "group_id": group_id,
                "profile_url": profile_url,
                "user_id": row["user_id"] or self.extract_user_id(profile_url),
                "name": row["name"],
                "group_user_url": row["group_user_url"],
                "updated_at": row["updated_at"] or "1970-01-01 00:00:00",
            }
            current = deduped_rows.get((scope_id, group_id))
            if not current or candidate["updated_at"] >= current["updated_at"]:
                deduped_rows[(scope_id, group_id)] = candidate
        return list(deduped_rows.values())

    def _fetch_table_columns(self, table_name):
        rows = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {row["name"] for row in rows}

    def _migrate_group_status(self):
        columns = self._fetch_table_columns("group_status")
        pk_rows = self.conn.execute("PRAGMA table_info(group_status)").fetchall() if columns else []
        pk_map = {row["name"]: row["pk"] for row in pk_rows}
        rows = []
        if columns:
            expected = {"group_id", "done_at", "account_id", "scope_id"}
            if (
                expected.issubset(columns)
                and pk_map.get("scope_id") == 1
                and pk_map.get("group_id") == 2
            ):
                return
            rows = self.conn.execute(
                """
                SELECT group_id, done_at, account_id,
                       {scope_column}
                FROM group_status
                """.format(scope_column="scope_id" if "scope_id" in columns else "NULL AS scope_id")
            ).fetchall()
            self.conn.execute("DROP TABLE IF EXISTS group_status")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS group_status (
                scope_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                done_at DATETIME,
                PRIMARY KEY (scope_id, group_id)
            )
            """
        )
        for row in rows:
            normalized_group_id = self.normalize_group_id(row["group_id"], allow_legacy=True)
            if not normalized_group_id:
                continue
            normalized_account_id = self.normalize_account_scope(row["account_id"])
            normalized_scope_id = self.normalize_legacy_scope_id(row["scope_id"], account_id=normalized_account_id)
            self.conn.execute(
                """
                INSERT OR REPLACE INTO group_status (scope_id, account_id, group_id, done_at)
                VALUES (?, ?, ?, ?)
                """,
                (normalized_scope_id, normalized_account_id, normalized_group_id, row["done_at"]),
            )

    def mark_group_done(self, group_id, account_id=None, scope_id=None, done_at=None):
        normalized_group_id = self.normalize_group_id(group_id)
        if not normalized_group_id:
            return
        normalized_account_id = self.normalize_account_scope(account_id)
        normalized_scope_id = self.normalize_scope_id(scope_id)
        if not normalized_scope_id:
            return
        done_at = done_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.conn.execute(
            """
            INSERT OR REPLACE INTO group_status
            (scope_id, account_id, group_id, done_at)
            VALUES (?, ?, ?, ?)
            """,
            (normalized_scope_id, normalized_account_id, normalized_group_id, done_at),
        )
        self.conn.commit()

    def is_group_done(self, group_id, account_id=None, scope_id=None, ttl_hours=0):
        normalized_group_id = self.normalize_group_id(group_id)
        if not normalized_group_id:
            return False
        normalized_scope_id = self.normalize_scope_id(scope_id)
        if not normalized_scope_id:
            return False
        row = self.conn.execute(
            "SELECT done_at FROM group_status WHERE scope_id = ? AND group_id = ?",
            (normalized_scope_id, normalized_group_id),
        ).fetchone()
        if not row:
            return False
        if ttl_hours is None:
            return True
        try:
            ttl_hours = float(ttl_hours)
        except Exception:
            ttl_hours = 0
        if ttl_hours <= 0:
            return True
        try:
            done_at = datetime.strptime(row["done_at"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return True
        if done_at + timedelta(hours=ttl_hours) <= datetime.now():
            self.conn.execute(
                "DELETE FROM group_status WHERE scope_id = ? AND group_id = ?",
                (normalized_scope_id, normalized_group_id),
            )
            self.conn.commit()
            return False
        return True

    def normalize_group_id(self, group_id, allow_legacy=False):
        raw = str(group_id or "").strip()
        if raw:
            return raw
        return self.LEGACY_GROUP_SCOPE if allow_legacy else None

    def normalize_account_scope(self, account_id):
        return str(account_id or "").strip()

    def normalize_scope_id(self, scope_id=None):
        raw = str(scope_id or "").strip()
        if raw:
            return raw
        return None

    def normalize_legacy_scope_id(self, scope_id=None, account_id=None):
        normalized_scope_id = self.normalize_scope_id(scope_id)
        if normalized_scope_id:
            return normalized_scope_id
        return self.normalize_account_scope(account_id)

    def normalize_profile_url(self, url):
        raw = (url or "").strip()
        if not raw:
            return None

        full_url = urljoin("https://www.facebook.com", raw)
        parsed = urlparse(full_url)
        if "facebook.com" not in parsed.netloc:
            return None

        path = parsed.path.rstrip("/")
        if path == "/profile.php":
            user_id = parse_qs(parsed.query).get("id", [None])[0]
            if not user_id:
                return None
            return f"https://www.facebook.com/profile.php?id={user_id}"

        segments = [segment for segment in path.split("/") if segment]
        if len(segments) >= 4 and segments[0] == "groups" and segments[2] == "user":
            return f"https://www.facebook.com/profile.php?id={segments[3]}"
        if len(segments) == 2 and segments[0] == "user":
            return f"https://www.facebook.com/profile.php?id={segments[1]}"
        if len(segments) == 1:
            return f"https://www.facebook.com/{segments[0]}"
        return f"https://www.facebook.com{path}"

    def extract_user_id(self, profile_url):
        normalized_url = self.normalize_profile_url(profile_url)
        if not normalized_url:
            return None

        parsed = urlparse(normalized_url)
        if parsed.path == "/profile.php":
            return parse_qs(parsed.query).get("id", [None])[0]

        segments = [segment for segment in parsed.path.split("/") if segment]
        if len(segments) == 2 and segments[0] == "user":
            return segments[1]
        return None

    def is_sent(self, group_id, account_id=None, profile_url=None, user_id=None, scope_id=None):
        normalized_group_id = self.normalize_group_id(group_id)
        normalized_scope_id = self.normalize_scope_id(scope_id)
        normalized_url = self.normalize_profile_url(profile_url) if profile_url else None
        normalized_user_id = user_id or self.extract_user_id(normalized_url)
        if not normalized_scope_id or not normalized_group_id or (not normalized_url and not normalized_user_id):
            return False

        conditions, params = self._build_target_conditions(normalized_url, normalized_user_id)
        query = (
            "SELECT 1 FROM sent_history "
            f"WHERE scope_id = ? AND group_id = ? AND status = 1 "
            f"AND ({' OR '.join(conditions)}) LIMIT 1"
        )
        row = self.conn.execute(query, tuple([normalized_scope_id, normalized_group_id] + params)).fetchone()
        return row is not None

    def try_claim_target(self, group_id, profile_url, account_id, user_id=None, ttl_hours=6, scope_id=None):
        normalized_group_id = self.normalize_group_id(group_id)
        normalized_account_id = self.normalize_account_scope(account_id)
        normalized_scope_id = self.normalize_scope_id(scope_id)
        normalized_url = self.normalize_profile_url(profile_url)
        normalized_user_id = user_id or self.extract_user_id(normalized_url)
        if not normalized_scope_id or not normalized_group_id or (not normalized_url and not normalized_user_id):
            return False

        conditions, params = self._build_target_conditions(normalized_url, normalized_user_id)
        if not conditions:
            return False

        now = datetime.now()
        expire_before = (now - timedelta(hours=max(1, int(ttl_hours)))).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        now_text = now.strftime("%Y-%m-%d %H:%M:%S")

        try:
            self.conn.execute("BEGIN IMMEDIATE")
            self.conn.execute(
                "DELETE FROM send_claims WHERE claimed_at < ?",
                (expire_before,),
            )
            already_sent = self.conn.execute(
                "SELECT 1 FROM sent_history "
                f"WHERE scope_id = ? AND group_id = ? AND status = 1 "
                f"AND ({' OR '.join(conditions)}) LIMIT 1",
                tuple([normalized_scope_id, normalized_group_id] + params),
            ).fetchone()
            if already_sent:
                self.conn.commit()
                return False

            self.conn.execute(
                """
                INSERT INTO send_claims (scope_id, account_id, group_id, profile_url, user_id, claimed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_scope_id,
                    normalized_account_id,
                    normalized_group_id,
                    normalized_url,
                    normalized_user_id,
                    now_text,
                ),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            self.conn.rollback()
            return False
        except Exception as exc:
            self.conn.rollback()
            log.error(f"抢占发送目标失败: {exc}")
            return False

    def release_claim(self, group_id, account_id=None, profile_url=None, user_id=None, scope_id=None):
        normalized_group_id = self.normalize_group_id(group_id)
        normalized_scope_id = self.normalize_scope_id(scope_id)
        normalized_url = self.normalize_profile_url(profile_url) if profile_url else None
        normalized_user_id = user_id or self.extract_user_id(normalized_url)
        conditions, params = self._build_target_conditions(normalized_url, normalized_user_id)
        if not normalized_scope_id or not normalized_group_id or not conditions:
            return

        try:
            self.conn.execute(
                f"DELETE FROM send_claims WHERE scope_id = ? AND group_id = ? AND ({' OR '.join(conditions)})",
                tuple([normalized_scope_id, normalized_group_id] + params),
            )
            self.conn.commit()
        except Exception as exc:
            log.error(f"释放发送占位失败: {exc}")

    def mark_done(self, group_id, profile_url, account_id, success=True, user_id=None, machine_id=None, scope_id=None):
        try:
            normalized_group_id = self.normalize_group_id(group_id)
            normalized_url = self.normalize_profile_url(profile_url)
            normalized_user_id = user_id or self.extract_user_id(normalized_url)
            normalized_account_id = self.normalize_account_scope(account_id)
            normalized_scope_id = self.normalize_scope_id(scope_id)
            normalized_machine_id = str(machine_id or "").strip() or None
            if not normalized_scope_id or not normalized_group_id or not normalized_url:
                raise ValueError("invalid_profile_url")

            status = 1 if success else 0
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.release_claim(
                group_id=normalized_group_id,
                account_id=normalized_account_id,
                profile_url=normalized_url,
                user_id=normalized_user_id,
                scope_id=normalized_scope_id,
            )
            conditions, params = self._build_target_conditions(normalized_url, normalized_user_id)
            existing_success = self.conn.execute(
                "SELECT 1 FROM sent_history "
                f"WHERE scope_id = ? AND group_id = ? AND status = 1 "
                f"AND ({' OR '.join(conditions)}) LIMIT 1",
                tuple([normalized_scope_id, normalized_group_id] + params),
            ).fetchone()
            if existing_success and not success:
                log.debug(
                    f"保留已有成功记录，不用失败状态覆盖: {normalized_group_id} -> {normalized_url}"
                )
                return
            self.conn.execute(
                f"DELETE FROM sent_history WHERE scope_id = ? AND group_id = ? AND ({' OR '.join(conditions)})",
                tuple([normalized_scope_id, normalized_group_id] + params),
            )
            self.conn.execute(
                """
                INSERT OR REPLACE INTO sent_history
                (scope_id, account_id, group_id, profile_url, user_id, status, created_at, machine_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_scope_id,
                    normalized_account_id,
                    normalized_group_id,
                    normalized_url,
                    normalized_user_id,
                    status,
                    now,
                    normalized_machine_id,
                ),
            )
            self.conn.commit()
            log.debug(f"数据已入库: {normalized_group_id} -> {normalized_url}")
        except Exception as exc:
            log.error(f"数据库写入失败: {exc}")

    def get_cursor(self, group_id, account_id=None, scope_id=None):
        try:
            normalized_group_id = self.normalize_group_id(group_id)
            normalized_scope_id = self.normalize_scope_id(scope_id)
            if not normalized_scope_id or not normalized_group_id:
                return None
            row = self.conn.execute(
                """
                SELECT scope_id, account_id, group_id, profile_url, user_id, name, group_user_url, updated_at
                FROM run_cursor
                WHERE scope_id = ? AND group_id=?
                """,
                (normalized_scope_id, normalized_group_id),
            ).fetchone()
            return dict(row) if row else None
        except Exception as exc:
            log.error(f"读取断点失败: {exc}")
            return None

    def save_cursor(self, group_id, target, account_id=None, scope_id=None):
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            normalized_group_id = self.normalize_group_id(group_id)
            normalized_account_id = self.normalize_account_scope(account_id)
            normalized_scope_id = self.normalize_scope_id(scope_id)
            profile_url = self.normalize_profile_url(target.get("profile_url"))
            user_id = target.get("user_id") or self.extract_user_id(profile_url)
            if not normalized_scope_id or not normalized_group_id or not profile_url:
                raise ValueError("invalid_cursor_profile_url")

            self.conn.execute(
                """
                INSERT OR REPLACE INTO run_cursor
                (scope_id, account_id, group_id, profile_url, user_id, name, group_user_url, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_scope_id,
                    normalized_account_id,
                    normalized_group_id,
                    profile_url,
                    user_id,
                    target.get("name"),
                    target.get("group_user_url"),
                    now,
                ),
            )
            self.conn.commit()
        except Exception as exc:
            log.error(f"保存断点失败: {exc}")

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def cleanup_old_history(self, history_days=15, claim_days=3):
        try:
            history_days = int(history_days)
        except Exception:
            history_days = 0
        try:
            claim_days = int(claim_days)
        except Exception:
            claim_days = 0
        removed = {"sent_history": 0, "send_claims": 0}
        try:
            if history_days > 0:
                cutoff = (
                    datetime.now() - timedelta(days=history_days)
                ).strftime("%Y-%m-%d %H:%M:%S")
                cur = self.conn.execute(
                    "DELETE FROM sent_history WHERE created_at IS NOT NULL AND created_at < ?",
                    (cutoff,),
                )
                removed["sent_history"] = cur.rowcount if cur else 0
            if claim_days > 0:
                cutoff = (
                    datetime.now() - timedelta(days=claim_days)
                ).strftime("%Y-%m-%d %H:%M:%S")
                cur = self.conn.execute(
                    "DELETE FROM send_claims WHERE claimed_at < ?",
                    (cutoff,),
                )
                removed["send_claims"] = cur.rowcount if cur else 0
            self.conn.execute("PRAGMA optimize")
            self.conn.commit()
        except Exception as exc:
            log.error(f"清理历史数据失败: {exc}")
        return removed

    def count_send_claims(self):
        try:
            row = self.conn.execute("SELECT COUNT(1) AS total FROM send_claims").fetchone()
            return int(row["total"] or 0) if row else 0
        except Exception as exc:
            log.error(f"统计排队中数量失败: {exc}")
            return 0

    def _build_target_conditions(self, normalized_url, normalized_user_id):
        conditions = []
        params = []
        if normalized_url:
            conditions.append("profile_url = ?")
            params.append(normalized_url)
        if normalized_user_id:
            conditions.append("user_id = ?")
            params.append(normalized_user_id)
        return conditions, params

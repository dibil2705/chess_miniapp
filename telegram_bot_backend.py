import hashlib
import hmac
import json
import mimetypes
import os
import re
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from dotenv import load_dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path=ENV_PATH, override=False)

TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("\ufeffTELEGRAM_BOT_TOKEN") or "").strip()
BOT_TOKEN = TOKEN or ""
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5.4-nano")
COACH_RULES_PATH = os.environ.get("COACH_RULES_PATH", "ai_coach_rules.txt")
STOCKFISH_INTERNAL_BASE = os.environ.get("STOCKFISH_INTERNAL_BASE", "http://127.0.0.1:8080").rstrip("/")
STOCKFISH_PROXY_TIMEOUT_SECONDS = int(os.environ.get("STOCKFISH_PROXY_TIMEOUT_SECONDS", "20"))
MINI_APP_URL = os.environ.get(
    "MINI_APP_URL",
    "https://t.me/chess_every_day_bot/app?startapp=test&mode=fullscreen",
)
DATABASE_PATH = os.environ.get("ANALYTICS_DB", "analytics.sqlite3")
MONITOR_STATE_PATH = os.environ.get("MONITOR_STATE_PATH", "monitor_state.json")
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("TELEGRAM_BACKEND_PORT") or os.environ.get("PORT", "12315"))
STATIC_ROOT = os.path.abspath(os.environ.get("STATIC_ROOT", os.getcwd()))
INITDATA_MAX_AGE_SECONDS = int(os.environ.get("INITDATA_MAX_AGE_SECONDS", "86400"))
TELEGRAM_POLL_TIMEOUT = int(os.environ.get("TELEGRAM_POLL_TIMEOUT", "25"))
TELEGRAM_REQUEST_TIMEOUT = TELEGRAM_POLL_TIMEOUT + 15
MSK = timezone(timedelta(hours=3))
DAY_MS = 24 * 60 * 60 * 1000
DAILY_RESET_UTC_OFFSET_MS = 4 * 60 * 60 * 1000


def msk_now():
    return datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")


def current_window_start_ms():
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return ((now_ms - DAILY_RESET_UTC_OFFSET_MS) // DAY_MS) * DAY_MS + DAILY_RESET_UTC_OFFSET_MS


def _load_sent_weekly_preview():
    if not os.path.exists(MONITOR_STATE_PATH):
        return None
    try:
        with open(MONITOR_STATE_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return None
    weekly = data.get("weekly_broadcast") if isinstance(data, dict) else None
    if not isinstance(weekly, dict):
        return None
    if str(weekly.get("status") or "") != "sent":
        return None
    preview = weekly.get("preview")
    if not isinstance(preview, dict):
        return None
    puzzles = preview.get("puzzles")
    if not isinstance(puzzles, list) or not puzzles:
        return None
    if not isinstance(puzzles[0], dict):
        return None
    return preview


def _build_global_weekly_state(preview):
    puzzles = preview.get("puzzles") if isinstance(preview.get("puzzles"), list) else []
    if not puzzles:
        return None
    primary = puzzles[0] if isinstance(puzzles[0], dict) else None
    if not primary:
        return None
    fen = str(primary.get("fen") or "")
    if not fen:
        return None
    now_ms = int(time.time() * 1000)
    active = "b" if (len(fen.split(" ")) > 1 and fen.split(" ")[1] == "b") else "w"
    puzzle_set = {
        "weekKey": str(preview.get("week_key") or ""),
        "createdAt": now_ms,
        "puzzles": [p for p in puzzles[:3] if isinstance(p, dict)],
    }
    return {
        "version": 1,
        "savedAt": now_ms,
        "quotaResetAt": now_ms,
        "quota": {
            "windowStart": current_window_start_ms(),
            "started": 0,
            "solved": 0,
            "bonusActivated": False,
            "dayDone": False,
            "bonusUnlocked": False,
        },
        "puzzle": {
            "puzzleData": primary,
            "puzzleMode": True,
            "puzzleSolutionMoves": [],
            "puzzleMoveIndex": 0,
            "puzzleSolved": False,
            "puzzleStartFen": fen,
            "puzzlePlayerColor": active,
            "puzzleSolutionTargetFen": None,
            "puzzleLoadedAt": now_ms,
            "boardFen": fen,
            "puzzleLockedAfterError": False,
            "puzzleErrorCount": 0,
            "weeklyPuzzleSet": puzzle_set,
        },
    }


def _extract_weekly_set_week_key(state):
    root = state.get("state") if isinstance(state, dict) else state
    if not isinstance(root, dict):
        return ""
    puzzle = root.get("puzzle") if isinstance(root.get("puzzle"), dict) else {}
    weekly_set = puzzle.get("weeklyPuzzleSet") if isinstance(puzzle.get("weeklyPuzzleSet"), dict) else {}
    return str(weekly_set.get("weekKey") or "").strip()


def _should_apply_weekly_seed(app_state, seeded_state):
    target_week_key = _extract_weekly_set_week_key(seeded_state)
    if not target_week_key:
        return False
    current_week_key = _extract_weekly_set_week_key(app_state)
    return current_week_key != target_week_key


def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    with get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                solved_tasks_count INTEGER NOT NULL DEFAULT 0,
                app_opens_count INTEGER NOT NULL DEFAULT 0,
                total_time_seconds INTEGER NOT NULL DEFAULT 0,
                average_session_seconds REAL NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS app_opens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                opened_at TEXT NOT NULL,
                platform TEXT,
                user_agent TEXT,
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_seconds INTEGER,
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                event_name TEXT NOT NULL,
                event_data TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
            );

            CREATE TABLE IF NOT EXISTS user_state (
                telegram_id INTEGER PRIMARY KEY,
                state_json TEXT NOT NULL,
                current_puzzle_json TEXT,
                settings_json TEXT,
                quota_json TEXT,
                history_json TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
            );

            CREATE TABLE IF NOT EXISTS ai_request_totals (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                requests_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ai_request_users (
                user_key TEXT PRIMARY KEY,
                telegram_id INTEGER,
                requests_count INTEGER NOT NULL DEFAULT 0,
                first_request_at TEXT NOT NULL,
                last_request_at TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
            );
            """
        )
        ensure_user_stat_columns(conn)
        ensure_user_state_columns(conn)
        ensure_ai_request_total_row(conn)
        rebuild_user_counters(conn)


def ensure_user_stat_columns(conn):
    existing = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(users)").fetchall()
    }
    columns = {
        "solved_tasks_count": "INTEGER NOT NULL DEFAULT 0",
        "app_opens_count": "INTEGER NOT NULL DEFAULT 0",
        "total_time_seconds": "INTEGER NOT NULL DEFAULT 0",
        "average_session_seconds": "REAL NOT NULL DEFAULT 0",
    }
    for name, definition in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE users ADD COLUMN {name} {definition}")


def ensure_user_state_columns(conn):
    existing = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(user_state)").fetchall()
    }
    columns = {
        "current_puzzle_json": "TEXT",
        "settings_json": "TEXT",
        "quota_json": "TEXT",
        "history_json": "TEXT",
    }
    for name, definition in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE user_state ADD COLUMN {name} {definition}")


def ensure_ai_request_total_row(conn):
    conn.execute(
        """
        INSERT OR IGNORE INTO ai_request_totals (id, requests_count, updated_at)
        VALUES (1, 0, ?)
        """,
        (msk_now(),),
    )


def normalize_ai_user_key(raw_value):
    token = str(raw_value or "").strip()
    if not token:
        return "anonymous"
    return token[:120]


def increment_ai_request_counters(conn, user_key, telegram_id=None):
    now = msk_now()
    normalized_user_key = normalize_ai_user_key(user_key)
    ensure_ai_request_total_row(conn)
    conn.execute(
        """
        UPDATE ai_request_totals
        SET requests_count = requests_count + 1,
            updated_at = ?
        WHERE id = 1
        """,
        (now,),
    )
    conn.execute(
        """
        INSERT INTO ai_request_users (
            user_key,
            telegram_id,
            requests_count,
            first_request_at,
            last_request_at
        )
        VALUES (?, ?, 1, ?, ?)
        ON CONFLICT(user_key) DO UPDATE SET
            requests_count = ai_request_users.requests_count + 1,
            telegram_id = COALESCE(excluded.telegram_id, ai_request_users.telegram_id),
            last_request_at = excluded.last_request_at
        """,
        (normalized_user_key, telegram_id, now, now),
    )


def update_user_counters(conn, telegram_id):
    stats = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM events WHERE telegram_id = ? AND event_name = 'puzzle_completed') AS solved_tasks_count,
            (SELECT COUNT(*) FROM app_opens WHERE telegram_id = ?) AS app_opens_count,
            COALESCE((SELECT SUM(duration_seconds) FROM sessions WHERE telegram_id = ? AND duration_seconds IS NOT NULL), 0) AS total_time_seconds,
            COALESCE((SELECT AVG(duration_seconds) FROM sessions WHERE telegram_id = ? AND duration_seconds IS NOT NULL), 0) AS average_session_seconds
        """,
        (telegram_id, telegram_id, telegram_id, telegram_id),
    ).fetchone()
    conn.execute(
        """
        UPDATE users
        SET
            solved_tasks_count = ?,
            app_opens_count = ?,
            total_time_seconds = ?,
            average_session_seconds = ?
        WHERE telegram_id = ?
        """,
        (
            int(stats["solved_tasks_count"] or 0),
            int(stats["app_opens_count"] or 0),
            int(stats["total_time_seconds"] or 0),
            float(stats["average_session_seconds"] or 0),
            telegram_id,
        ),
    )


def rebuild_user_counters(conn):
    rows = conn.execute("SELECT telegram_id FROM users").fetchall()
    for row in rows:
        update_user_counters(conn, row["telegram_id"])


def save_user(conn, user):
    now = msk_now()
    conn.execute(
        """
        INSERT INTO users (
            telegram_id, username, first_name, last_name, first_seen_at, last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            last_seen_at = excluded.last_seen_at
        """,
        (
            int(user["id"]),
            user.get("username"),
            user.get("first_name"),
            user.get("last_name"),
            now,
            now,
        ),
    )


def record_app_open(conn, telegram_id, platform=None, user_agent=None):
    conn.execute(
        """
        INSERT INTO app_opens (telegram_id, opened_at, platform, user_agent)
        VALUES (?, ?, ?, ?)
        """,
        (telegram_id, msk_now(), platform, user_agent),
    )
    update_user_counters(conn, telegram_id)


def start_session(conn, telegram_id):
    cursor = conn.execute(
        """
        INSERT INTO sessions (telegram_id, started_at)
        VALUES (?, ?)
        """,
        (telegram_id, msk_now()),
    )
    return cursor.lastrowid


def end_session(conn, telegram_id, session_id):
    ended_at = msk_now()
    conn.execute(
        """
        UPDATE sessions
        SET
            ended_at = ?,
            duration_seconds = MAX(0, CAST(strftime('%s', ?) - strftime('%s', started_at) AS INTEGER))
        WHERE id = ? AND telegram_id = ? AND ended_at IS NULL
        """,
        (ended_at, ended_at, session_id, telegram_id),
    )
    update_user_counters(conn, telegram_id)


def record_event(conn, telegram_id, event_name, event_data=None):
    conn.execute(
        """
        INSERT INTO events (telegram_id, event_name, event_data, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            telegram_id,
            str(event_name),
            json.dumps(event_data or {}, ensure_ascii=False),
            msk_now(),
        ),
    )
    if str(event_name) == "puzzle_completed":
        update_user_counters(conn, telegram_id)


def decode_json_value(value, fallback=None):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def encode_json_value(value):
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


def extract_settings_state(state):
    settings = dict((state or {}).get("settings") or {})
    if "palette" in (state or {}):
        settings["palette"] = state.get("palette")
    if "soundEnabled" in (state or {}):
        settings["soundEnabled"] = state.get("soundEnabled")
    return settings or None


def hydrate_state_components(row, state):
    state = dict(state or {})
    current_puzzle = decode_json_value(row["current_puzzle_json"])
    settings = decode_json_value(row["settings_json"])
    quota = decode_json_value(row["quota_json"])
    history = decode_json_value(row["history_json"])

    if current_puzzle:
        state["puzzle"] = current_puzzle
    if settings:
        state["settings"] = settings
        if "palette" in settings:
            state["palette"] = settings["palette"]
        if "soundEnabled" in settings:
            state["soundEnabled"] = settings["soundEnabled"]
    if quota:
        state["quota"] = quota
    if history:
        state["history"] = history
    return state


def get_user_state(conn, telegram_id):
    row = conn.execute(
        """
        SELECT
            state_json,
            current_puzzle_json,
            settings_json,
            quota_json,
            history_json,
            updated_at
        FROM user_state
        WHERE telegram_id = ?
        """,
        (telegram_id,),
    ).fetchone()
    if not row:
        return None
    state = decode_json_value(row["state_json"], {})
    state = hydrate_state_components(row, state)
    return {
        "state": state,
        "updated_at": row["updated_at"],
    }


def get_state_saved_at(state):
    try:
        return int((state or {}).get("savedAt") or 0)
    except (TypeError, ValueError):
        return 0


def has_valid_puzzle_state(state):
    puzzle = (state or {}).get("puzzle")
    return isinstance(puzzle, dict) and bool(puzzle.get("puzzleData")) and bool(puzzle.get("boardFen"))


def merge_user_state(existing_state, new_state):
    merged = dict(existing_state or {})
    for key, value in (new_state or {}).items():
        if key == "quota":
            existing_reset_at = int((existing_state or {}).get("quotaResetAt") or 0)
            incoming_reset_at = int((new_state or {}).get("quotaResetAt") or 0)
            if existing_reset_at and incoming_reset_at < existing_reset_at:
                continue
        if key == "puzzle" and not has_valid_puzzle_state({"puzzle": value}):
            continue
        if key == "history" and value is None:
            continue
        if key == "palette" and not value:
            continue
        if key == "soundEnabled" and not isinstance(value, bool):
            continue
        merged[key] = value
    return merged


def save_user_state(conn, telegram_id, state, force=False):
    existing = get_user_state(conn, telegram_id)
    if not force and existing and get_state_saved_at(existing.get("state")) > get_state_saved_at(state):
        return False
    state_to_save = merge_user_state(existing.get("state") if existing else {}, state)
    current_puzzle = state_to_save.get("puzzle") if has_valid_puzzle_state(state_to_save) else None
    settings = extract_settings_state(state_to_save)
    quota = state_to_save.get("quota") if isinstance(state_to_save.get("quota"), dict) else None
    history = state_to_save.get("history") if state_to_save.get("history") else None
    conn.execute(
        """
        INSERT INTO user_state (
            telegram_id,
            state_json,
            current_puzzle_json,
            settings_json,
            quota_json,
            history_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            state_json = excluded.state_json,
            current_puzzle_json = excluded.current_puzzle_json,
            settings_json = excluded.settings_json,
            quota_json = excluded.quota_json,
            history_json = excluded.history_json,
            updated_at = excluded.updated_at
        """,
        (
            telegram_id,
            encode_json_value(state_to_save or {}),
            encode_json_value(current_puzzle),
            encode_json_value(settings),
            encode_json_value(quota),
            encode_json_value(history),
            msk_now(),
        ),
    )
    return True


def reset_daily_quota(conn, telegram_id):
    existing = get_user_state(conn, telegram_id)
    state = dict(existing.get("state") if existing else {})
    state["savedAt"] = int(time.time() * 1000)
    state["quotaResetAt"] = state["savedAt"]
    state["quota"] = {
        "windowStart": current_window_start_ms(),
        "started": 0,
        "solved": 0,
        "bonusActivated": False,
        "dayDone": False,
        "bonusUnlocked": False,
    }
    return save_user_state(conn, telegram_id, state, force=True)


def user_from_message(message):
    sender = message.get("from") or {}
    if not sender.get("id"):
        return None
    return {
        "id": sender["id"],
        "username": sender.get("username"),
        "first_name": sender.get("first_name"),
        "last_name": sender.get("last_name"),
    }


def record_bot_message(message):
    user = user_from_message(message)
    if not user:
        return
    telegram_id = int(user["id"])
    text = message.get("text", "")
    with get_db() as conn:
        save_user(conn, user)
        if text.startswith("/start"):
            record_event(
                conn,
                telegram_id,
                "bot_start",
                {"text": text, "chat_id": message.get("chat", {}).get("id")},
            )
            print(f"Recorded bot_start: telegram_id={telegram_id}")


def get_user_stats(conn, telegram_id):
    user = conn.execute(
        """
        SELECT
            telegram_id,
            first_seen_at,
            last_seen_at,
            solved_tasks_count,
            app_opens_count,
            total_time_seconds,
            average_session_seconds
        FROM users
        WHERE telegram_id = ?
        """,
        (telegram_id,),
    ).fetchone()
    return {
        "telegram_id": telegram_id,
        "solved_tasks_count": int(user["solved_tasks_count"] or 0) if user else 0,
        "app_opens_count": int(user["app_opens_count"] or 0) if user else 0,
        "total_time_seconds": int(user["total_time_seconds"] or 0) if user else 0,
        "average_session_seconds": float(user["average_session_seconds"] or 0) if user else 0,
        "first_seen_at": user["first_seen_at"] if user else None,
        "last_seen_at": user["last_seen_at"] if user else None,
    }


def normalize_platform(platform):
    value = (platform or "").strip().lower()
    if value in {"ios", "iphone", "ipad"}:
        return "iOS"
    if value in {"android"}:
        return "Android"
    if value in {"tdesktop", "desktop", "macos", "windows", "linux"}:
        return "Desktop"
    if value in {"web", "weba", "webk"}:
        return "Web"
    return platform or None


def validate_init_data(init_data):
    if not BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN is not set")

    parsed = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
    received_hash = parsed.pop("hash", "")
    if not received_hash:
        raise ValueError("initData hash is missing")

    data_check_string = "\n".join(f"{key}={parsed[key]}" for key in sorted(parsed))
    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    calculated_hash = hmac.new(
        secret_key,
        data_check_string.encode(),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(calculated_hash, received_hash):
        raise ValueError("initData hash is invalid")

    auth_date = int(parsed.get("auth_date", "0") or "0")
    if INITDATA_MAX_AGE_SECONDS > 0 and time.time() - auth_date > INITDATA_MAX_AGE_SECONDS:
        raise ValueError("initData is too old")

    user = json.loads(parsed.get("user", "{}"))
    if not user.get("id"):
        raise ValueError("initData user is missing")

    return parsed, user


def json_response(handler, status, payload):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    safe_http_write(
        handler,
        status,
        body,
        content_type="application/json; charset=utf-8",
        extra_headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type, ngrok-skip-browser-warning",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        },
    )


def text_response(handler, status, text):
    body = text.encode("utf-8")
    safe_http_write(handler, status, body, content_type="text/plain; charset=utf-8")


def safe_http_write(handler, status, body, content_type="application/octet-stream", extra_headers=None):
    try:
        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(body)))
        if isinstance(extra_headers, dict):
            for key, value in extra_headers.items():
                handler.send_header(str(key), str(value))
        handler.end_headers()
        handler.wfile.write(body)
        return True
    except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
        # РљР»РёРµРЅС‚ СѓР¶Рµ Р·Р°РєСЂС‹Р» СЃРѕРєРµС‚ (РЅР°РїСЂРёРјРµСЂ, РѕС‚РјРµРЅРёР» Р·Р°РїСЂРѕСЃ/РїРµСЂРµР·Р°РіСЂСѓР·РёР» СЃС‚СЂР°РЅРёС†Сѓ).
        return False


def proxy_stockfish_request(handler, method, endpoint_path, payload=None):
    target_url = f"{STOCKFISH_INTERNAL_BASE}{endpoint_path}"
    body = None
    headers = {
        "Content-Type": "application/json",
        "ngrok-skip-browser-warning": "1",
    }
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    request = urllib.request.Request(target_url, data=body, headers=headers, method=method)
    cors_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type, ngrok-skip-browser-warning",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    }

    try:
        with urllib.request.urlopen(request, timeout=STOCKFISH_PROXY_TIMEOUT_SECONDS) as response:
            response_body = response.read()
            content_type = response.headers.get("Content-Type") or "application/json; charset=utf-8"
            safe_http_write(
                handler,
                response.getcode(),
                response_body,
                content_type=content_type,
                extra_headers=cors_headers,
            )
            return
    except urllib.error.HTTPError as err:
        error_body = err.read() or b""
        content_type = err.headers.get("Content-Type") if err.headers else None
        if not error_body:
            error_body = json.dumps(
                {
                    "ok": False,
                    "error": f"Stockfish upstream HTTP {err.code}",
                    "upstream": target_url,
                },
                ensure_ascii=False,
            ).encode("utf-8")
            content_type = "application/json; charset=utf-8"
        safe_http_write(
            handler,
            err.code,
            error_body,
            content_type=content_type or "application/json; charset=utf-8",
            extra_headers=cors_headers,
        )
        return
    except Exception as err:
        print(f"Stockfish proxy error ({method} {endpoint_path}): {err}")
        json_response(
            handler,
            502,
            {
                "ok": False,
                "error": "Stockfish upstream unavailable",
                "detail": str(err),
                "upstream": target_url,
            },
        )


def resolve_static_path(request_path):
    parsed = urllib.parse.urlparse(request_path)
    path = urllib.parse.unquote(parsed.path or "/")
    if path == "/":
        path = "/index.html"
    path = path.lstrip("/")
    if not path or path.startswith("api/"):
        return None
    full_path = os.path.abspath(os.path.join(STATIC_ROOT, path))
    try:
        if os.path.commonpath([STATIC_ROOT, full_path]) != STATIC_ROOT:
            return None
    except ValueError:
        return None
    if not os.path.isfile(full_path):
        return None
    return full_path


def compact_analysis_line(line):
    if not isinstance(line, dict):
        return {}
    score = line.get("score") if isinstance(line.get("score"), dict) else {}
    pv_value = line.get("pv_san") or line.get("pv") or line.get("pv_uci") or ""
    pv_moves = compact_pv_moves(pv_value)
    return {
        "move": line.get("best_move_uci") or line.get("bestmove") or line.get("move") or "",
        "pv": pv_value,
        "pv_first_moves": pv_moves,
        "score_cp": line.get("score_cp") if line.get("score_cp") is not None else line.get("cp"),
        "mate": line.get("mate"),
        "score_side": line.get("score_side") or score.get("score_side") or score.get("side") or line.get("side"),
    }


def compact_pv_moves(pv, max_moves=6):
    if isinstance(pv, list):
        tokens = [str(item) for item in pv]
    else:
        tokens = str(pv or "").replace("\n", " ").split()
    cleaned = []
    for token in tokens:
        token = token.strip()
        if not token or token.endswith(".") or token.replace(".", "").isdigit():
            continue
        cleaned.append(token)
        if len(cleaned) >= max_moves:
            break
    return cleaned


def is_capture_token(move):
    return "x" in str(move or "")


def build_tactical_context(move_details, current_line):
    if not move_details:
        return {}
    pv_moves = current_line.get("pv_first_moves") if isinstance(current_line, dict) else []
    first_reply = pv_moves[1] if len(pv_moves or []) > 1 else ""
    new_attacked_pieces = move_details.get("new_attacked_pieces") or []
    non_king_targets = [
        item for item in new_attacked_pieces
        if isinstance(item, dict) and str(item.get("piece") or "").lower() not in ("РєРѕСЂРѕР»СЏ", "king")
    ]
    return {
        "capture_is_exchange": bool(move_details.get("captured_piece") and is_capture_token(first_reply)),
        "reply_is_capture": is_capture_token(first_reply),
        "reply_is_check": "+" in str(first_reply or "") or "#" in str(first_reply or ""),
        "is_fork": len(non_king_targets) >= 2,
        "target_count": len(new_attacked_pieces),
        "was_in_check_before": bool(move_details.get("was_in_check_before")),
        "resolved_check": bool(move_details.get("resolved_check")),
        "defended_by_king": bool(move_details.get("defended_by_king")),
    }


def build_safety_policy_context():
    return {
        "do_not_treat_defended_as_safe": True,
        "bad_defender_reasons": [
            "pinned_defender",
            "overloaded_defender",
            "defends_king_or_mate_square",
            "distracted_by_stronger_threat",
            "recapture_loses_more_valuable_piece",
            "move_illegal_due_to_check_mate_or_open_line",
        ],
        "compare_before_after": [
            "new_threat",
            "weakened_piece_or_square",
            "new_vulnerable_piece",
            "defender_stopped_working",
            "critical_square",
            "first_line_verdict",
        ],
        "forcing_moves_order": [
            "checks",
            "captures",
            "mate_threats",
            "attacks_on_more_valuable_piece",
            "zwischenzug",
        ],
        "main_motif_priority": [
            "mate_threat",
            "queen_or_decisive_material_win",
            "check_defense",
            "failed_capture_tactical_refutation",
            "hidden_attack_or_bad_defender",
            "positional_improvement",
        ],
    }


def build_motif_focus_context(tactical_context, move_details, current_mate, eval_change):
    motifs = []
    if current_mate and current_mate.get("mating_side"):
        motifs.append("РјР°С‚РѕРІР°СЏ СѓРіСЂРѕР·Р°")
    if tactical_context.get("was_in_check_before") and tactical_context.get("resolved_check"):
        motifs.append("Р·Р°С‰РёС‚Р° РѕС‚ С€Р°С…Р°")
    if tactical_context.get("is_fork"):
        motifs.append("РІРёР»РєР°")
    discovered = move_details.get("discovered_attack") if isinstance(move_details, dict) else {}
    if isinstance(discovered, dict) and discovered.get("attacker_piece"):
        motifs.append("СЃРєСЂС‹С‚Р°СЏ Р°С‚Р°РєР°")
        if discovered.get("defense_state") == "overloaded":
            motifs.append("РїРµСЂРµРіСЂСѓР·РєР° Р·Р°С‰РёС‚РЅРёРєР°")
        elif discovered.get("defense_state") == "undefended":
            motifs.append("Р±РµР·Р·Р°С‰РёС‚РЅР°СЏ С„РёРіСѓСЂР°")
    if move_details.get("shared_pressure") if isinstance(move_details, dict) else False:
        motifs.append("СЃРѕРІРјРµСЃС‚РЅРѕРµ РґР°РІР»РµРЅРёРµ")
    if move_details.get("captured_piece") and eval_change and eval_change.get("kind") == "worsened":
        motifs.append("Р»РѕРІСѓС€РєР° РІР·СЏС‚РёСЏ")
    if tactical_context.get("reply_is_capture") and move_details.get("captured_piece"):
        motifs.append("С„РѕСЂСЃРёСЂРѕРІР°РЅРЅС‹Р№ СЂР°Р·РјРµРЅ")
    unique = []
    for motif in motifs:
        if motif not in unique:
            unique.append(motif)
    return unique[:5]


def extract_response_text(data):
    if not isinstance(data, dict):
        return ""
    if isinstance(data.get("output_text"), str):
        return data["output_text"].strip()
    parts = []
    for item in data.get("output") or []:
        for content in item.get("content") or []:
            text = content.get("text") if isinstance(content, dict) else None
            if isinstance(text, str):
                parts.append(text)
    return " ".join(parts).strip()


def load_coach_rules():
    try:
        with open(COACH_RULES_PATH, "r", encoding="utf-8") as rules_file:
            rules = rules_file.read().strip()
            if rules:
                return rules
    except OSError as err:
        print("AI coach rules file error:", err)
    return (
        "РўС‹ С€Р°С…РјР°С‚РЅС‹Р№ С‚СЂРµРЅРµСЂ РІ СЌРєСЂР°РЅРµ Р°РЅР°Р»РёР·Р°. РћС‚РІРµС‡Р°Р№ СЃС‚СЂРѕРіРѕ РѕРґРЅРёРј РїСЂРµРґР»РѕР¶РµРЅРёРµРј РЅР° СЂСѓСЃСЃРєРѕРј СЏР·С‹РєРµ. "
        "РџРёС€Рё, С‡С‚Рѕ РїСЂРѕРёР·РѕС€Р»Рѕ РІ РїРѕР·РёС†РёРё РёР»Рё РїРѕСЃР»Рµ С…РѕРґР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ."
    )


def first_sentence(text):
    text = " ".join(str(text or "").split())
    for index, char in enumerate(text):
        if char in ".!?" and (index == len(text) - 1 or text[index + 1].isspace()):
            return text[: index + 1].strip()
    return text


def shorten_coach_comment(text, max_words=16):
    text = first_sentence(text).replace("**", "").replace("`", "")
    text = " ".join(text.split()).strip()
    for separator in (":", ";", " вЂ” ", " - "):
        if separator in text:
            head = text.split(separator, 1)[0].strip()
            if len(head.split()) >= 4:
                text = head
                break
    words = text.rstrip(".!?").split()
    if len(words) > max_words:
        text = " ".join(words[:max_words])
    text = text.strip(" ,;:-")
    if text and text[-1] not in ".!?":
        text += "."
    return text


def build_forced_mate_context(mate, side):
    if mate is None:
        return None
    try:
        mate_in = int(mate)
    except (TypeError, ValueError):
        return None
    if mate_in == 0:
        return None
    normalized_side = normalize_score_side(side)
    side_to_move = normalized_side or "side_to_move"
    opponent = "black" if side_to_move == "white" else "white" if side_to_move == "black" else "opponent"
    return {
        "moves": abs(mate_in),
        "mating_side": side_to_move if mate_in > 0 else opponent,
        "defending_side": opponent if mate_in > 0 else side_to_move,
    }


def side_label(side):
    return "Р‘РµР»С‹Рµ" if side == "white" else "Р§РµСЂРЅС‹Рµ" if side == "black" else "РЎС‚РѕСЂРѕРЅР°"


def side_genitive(side):
    return "Р±РµР»С‹С…" if side == "white" else "С‡РµСЂРЅС‹С…" if side == "black" else "СЃС‚РѕСЂРѕРЅС‹"


def piece_label(piece):
    piece = str(piece or "").upper()
    return {
        "P": "РїРµС€РєСѓ",
        "N": "РєРѕРЅСЏ",
        "B": "СЃР»РѕРЅР°",
        "R": "Р»Р°РґСЊСЋ",
        "Q": "С„РµСЂР·СЏ",
        "K": "РєРѕСЂРѕР»СЏ",
    }.get(piece, "")


def piece_nominative(piece):
    piece = str(piece or "").upper()
    return {
        "P": "РїРµС€РєР°",
        "N": "РєРѕРЅСЊ",
        "B": "СЃР»РѕРЅ",
        "R": "Р»Р°РґСЊСЏ",
        "Q": "С„РµСЂР·СЊ",
        "K": "РєРѕСЂРѕР»СЊ",
    }.get(piece, "")


def piece_instrumental(piece_name):
    name = str(piece_name or "").lower()
    return {
        "РїРµС€РєР°": "РїРµС€РєРѕР№",
        "РєРѕРЅСЊ": "РєРѕРЅРµРј",
        "СЃР»РѕРЅ": "СЃР»РѕРЅРѕРј",
        "Р»Р°РґСЊСЏ": "Р»Р°РґСЊРµР№",
        "С„РµСЂР·СЊ": "С„РµСЂР·РµРј",
        "РєРѕСЂРѕР»СЊ": "РєРѕСЂРѕР»РµРј",
    }.get(name, "С„РёРіСѓСЂРѕР№")


def piece_accusative(piece_name):
    name = str(piece_name or "").lower()
    return {
        "РїРµС€РєР°": "РїРµС€РєСѓ",
        "РєРѕРЅСЊ": "РєРѕРЅСЏ",
        "СЃР»РѕРЅ": "СЃР»РѕРЅР°",
        "Р»Р°РґСЊСЏ": "Р»Р°РґСЊСЋ",
        "С„РµСЂР·СЊ": "С„РµСЂР·СЏ",
        "РєРѕСЂРѕР»СЊ": "РєРѕСЂРѕР»СЏ",
    }.get(name, "С„РёРіСѓСЂСѓ")


def piece_dative(piece_name):
    name = str(piece_name or "").lower()
    return {
        "РїРµС€РєР°": "РїРµС€РєРµ",
        "РєРѕРЅСЊ": "РєРѕРЅСЋ",
        "СЃР»РѕРЅ": "СЃР»РѕРЅСѓ",
        "Р»Р°РґСЊСЏ": "Р»Р°РґСЊРµ",
        "С„РµСЂР·СЊ": "С„РµСЂР·СЋ",
        "РєРѕСЂРѕР»СЊ": "РєРѕСЂРѕР»СЋ",
    }.get(name, "С„РёРіСѓСЂРµ")


def normalize_piece_cases(text):
    comment = str(text or "")
    pieces = ("РєРѕСЂРѕР»СЊ", "С„РµСЂР·СЊ", "Р»Р°РґСЊСЏ", "СЃР»РѕРЅ", "РєРѕРЅСЊ", "РїРµС€РєР°")
    for piece in pieces:
        acc = piece_accusative(piece)
        dat = piece_dative(piece)
        ins = piece_instrumental(piece)
        comment = re.sub(rf"\bРЅР°\s+{piece}\b", f"РЅР° {acc}", comment, flags=re.IGNORECASE)
        comment = re.sub(rf"\bС…РѕРґ\s+{piece}\b", f"С…РѕРґ {ins}", comment, flags=re.IGNORECASE)
        comment = re.sub(rf"\bР°С‚Р°РєСѓСЋС‚\s+{piece}\b", f"Р°С‚Р°РєСѓСЋС‚ {acc}", comment, flags=re.IGNORECASE)
        comment = re.sub(rf"\bРЅСѓР¶РЅР° Р·Р°С‰РёС‚Р°\s+{piece}\b", f"РЅСѓР¶РЅР° Р·Р°С‰РёС‚Р° {dat}", comment, flags=re.IGNORECASE)
    comment = re.sub(r"\s{2,}", " ", comment).strip()
    return comment


def piece_gives_check(piece_name):
    return f"{piece_name} РґР°Р»Р° С€Р°С…" if piece_name in ("Р»Р°РґСЊСЏ", "РїРµС€РєР°") else f"{piece_name} РґР°Р» С€Р°С…"


def mate_support_phrase(summary):
    lower = str(summary or "").lower()
    if lower.startswith("РїРµС€РєР°"):
        return f"{summary.capitalize()} Рё РїРѕРґРґРµСЂР¶РёРІР°РµС‚ РјР°С‚РѕРІСѓСЋ Р°С‚Р°РєСѓ."
    if lower.startswith("Р»Р°РґСЊСЏ"):
        return f"{summary.capitalize()} Рё РїРѕРґРєР»СЋС‡Р°РµС‚СЃСЏ Рє РјР°С‚РѕРІРѕР№ Р°С‚Р°РєРµ."
    return f"{summary.capitalize()} Рё РїРѕРґРґРµСЂР¶РёРІР°РµС‚ РјР°С‚РѕРІСѓСЋ Р°С‚Р°РєСѓ."


def move_past_verb(piece_name):
    name = str(piece_name or "").lower()
    if name in ("РїРµС€РєР°", "Р»Р°РґСЊСЏ"):
        return "СЃРґРµР»Р°Р»Р°"
    return "СЃРґРµР»Р°Р»"


def piece_developed_verb(piece_name):
    name = str(piece_name or "").lower()
    if name in ("РїРµС€РєР°", "Р»Р°РґСЊСЏ"):
        return "СЂР°Р·РІРёР»Р°СЃСЊ"
    return "СЂР°Р·РІРёР»СЃСЏ"


def square_rank(square):
    token = str(square or "")
    if len(token) < 2 or token[1] not in "12345678":
        return None
    return int(token[1])


def is_initial_square_move(move, piece_name):
    side = normalize_score_side(move.get("side"))
    from_sq = str(move.get("from") or "")
    if not side or len(from_sq) < 2:
        return False
    initial_map = {
        ("white", "РєРѕРЅСЊ"): {"b1", "g1"},
        ("black", "РєРѕРЅСЊ"): {"b8", "g8"},
        ("white", "СЃР»РѕРЅ"): {"c1", "f1"},
        ("black", "СЃР»РѕРЅ"): {"c8", "f8"},
        ("white", "Р»Р°РґСЊСЏ"): {"a1", "h1"},
        ("black", "Р»Р°РґСЊСЏ"): {"a8", "h8"},
        ("white", "С„РµСЂР·СЊ"): {"d1"},
        ("black", "С„РµСЂР·СЊ"): {"d8"},
        ("white", "РєРѕСЂРѕР»СЊ"): {"e1"},
        ("black", "РєРѕСЂРѕР»СЊ"): {"e8"},
    }
    return from_sq in initial_map.get((side, piece_name), set())


def rook_on_open_file(move, board_snapshot):
    if piece_nominative(move.get("movingPiece")) != "Р»Р°РґСЊСЏ":
        return False
    to_sq = str(move.get("to") or "")
    if len(to_sq) < 2:
        return False
    side = normalize_score_side(move.get("side"))
    if not side:
        return False
    file_ch = to_sq[0]
    own_pawn = "P" if side == "white" else "p"
    for item in board_snapshot or []:
        if not isinstance(item, dict):
            continue
        piece = str(item.get("piece") or "")
        square = str(item.get("square") or "")
        if len(square) < 2:
            continue
        if square[0] == file_ch and piece == own_pawn:
            return False
    return True


def pawn_ready_to_promote(move, board_snapshot):
    if piece_nominative(move.get("movingPiece")) != "РїРµС€РєР°":
        return False
    to_sq = str(move.get("to") or "")
    rank = square_rank(to_sq)
    side = normalize_score_side(move.get("side"))
    if rank is None or not side:
        return False
    close_rank = (side == "white" and rank == 7) or (side == "black" and rank == 2)
    if not close_rank:
        return False
    profile = build_board_profile(board_snapshot)
    return int(profile.get("total_non_king_pieces") or 0) <= 8


def build_move_summary(move, board_snapshot=None):
    if not isinstance(move, dict):
        return ""
    piece = piece_nominative(move.get("movingPiece"))
    if not piece:
        return "СЃРґРµР»Р°РЅ С…РѕРґ"
    captured = piece_label(move.get("capturedPiece"))
    label = str(move.get("label") or move.get("san") or "").lower()
    attacks = move.get("newAttackedPieces") if isinstance(move.get("newAttackedPieces"), list) else []
    move_verb = move_past_verb(piece)
    if captured:
        if "x" in label:
            if piece == "РєРѕСЂРѕР»СЊ":
                return f"{piece} СЃСЉРµР» {captured} Рё СѓРїСЂРѕСЃС‚РёР» РїРѕР·РёС†РёСЋ.".strip()
            if piece == "С„РµСЂР·СЊ" and captured == "С„РµСЂР·СЏ":
                return f"{piece} СЃСЉРµР» {captured} Рё СѓРїСЂРѕСЃС‚РёР» РїРѕР·РёС†РёСЋ.".strip()
            if attacks:
                return f"{piece} СЃСЉРµР» {captured} Рё РЅР°С‡РёРЅР°РµС‚СЃСЏ СЂР°Р·РјРµРЅ.".strip()
            return f"{piece} СЃСЉРµР» {captured}".strip()
        return f"{piece} СЃСЉРµР» {captured}".strip()
    if len(attacks) == 1:
        target = attacks[0] if isinstance(attacks[0], dict) else {}
        target_name = piece_label(target.get("piece")) or target.get("piece_name")
        target_piece = str(target.get("piece") or "").upper()
        if target_piece == "K":
            return piece_gives_check(piece).strip()
        if target_name:
            if piece == "РїРµС€РєР°":
                return f"{piece} РЅР°РїР°Р»Р° РЅР° {target_name}".strip()
            verb = "РЅР°РїР°Р»Р°" if piece in ("РїРµС€РєР°", "Р»Р°РґСЊСЏ") else "РЅР°РїР°Р»"
            return f"{piece} {verb} РЅР° {target_name}".strip()
    non_king_attacks = [item for item in attacks if isinstance(item, dict) and str(item.get("piece") or "").upper() != "K"]
    if len(non_king_attacks) >= 2:
        return f"{piece} СЃРґРµР»Р°Р» РІРёР»РєСѓ".strip()
    if len(attacks) >= 2:
        target = non_king_attacks[0] if non_king_attacks else (attacks[0] if isinstance(attacks[0], dict) else {})
        target_name = piece_label(target.get("piece")) or target.get("piece_name")
        if target_name:
            if any(isinstance(item, dict) and str(item.get("piece") or "").upper() == "K" for item in attacks):
                check_verb = "РґР°Р»Р°" if piece in ("РїРµС€РєР°", "Р»Р°РґСЊСЏ") else "РґР°Р»"
                attack_verb = "РЅР°РїР°Р»Р°" if piece in ("РїРµС€РєР°", "Р»Р°РґСЊСЏ") else "РЅР°РїР°Р»"
                return f"{piece} {check_verb} С€Р°С… Рё {attack_verb} РЅР° {target_name}".strip()
            return f"{piece} СЃРѕР·РґР°Р» РґРІРѕР№РЅСѓСЋ СѓРіСЂРѕР·Сѓ".strip()
    if is_initial_square_move(move, piece):
        if piece == "Р»Р°РґСЊСЏ" and rook_on_open_file(move, board_snapshot):
            return "Р»Р°РґСЊСЏ СЂР°Р·РІРёР»Р°СЃСЊ Рё РІС‹С€Р»Р° РЅР° РѕС‚РєСЂС‹С‚СѓСЋ Р»РёРЅРёСЋ, РµР№ С‚Р°Рј РєРѕРјС„РѕСЂС‚РЅРµРµ"
        return f"{piece} {piece_developed_verb(piece)}".strip()
    if piece == "Р»Р°РґСЊСЏ" and rook_on_open_file(move, board_snapshot):
        return "Р»Р°РґСЊСЏ РІС‹С€Р»Р° РЅР° РѕС‚РєСЂС‹С‚СѓСЋ Р»РёРЅРёСЋ Рё С‚РµРїРµСЂСЊ РґР°РІРёС‚ РїРѕ РІРµСЂС‚РёРєР°Р»Рё"
    if pawn_ready_to_promote(move, board_snapshot):
        return "РїРµС€РєР° РїСЂРѕРґРІРёРЅСѓР»Р°СЃСЊ Рё РіРѕС‚РѕРІРёС‚СЃСЏ Рє РїСЂРµРІСЂР°С‰РµРЅРёСЋ РІ С„РµСЂР·СЏ"
    return f"{piece} {move_verb} С…РѕРґ".strip()


def compact_move_details(move, board_snapshot=None):
    if not isinstance(move, dict):
        return {}
    return {
        "uci": move.get("uci") or "",
        "side": normalize_score_side(move.get("side")),
        "piece": piece_label(move.get("movingPiece")),
        "moving_piece_name": piece_nominative(move.get("movingPiece")),
        "from": str(move.get("from") or ""),
        "to": str(move.get("to") or ""),
        "captured_piece": piece_label(move.get("capturedPiece")),
        "promotion": piece_label(move.get("promotionPiece")),
        "summary": build_move_summary(move, board_snapshot),
        "was_in_check_before": bool(move.get("wasInCheckBefore")),
        "resolved_check": bool(move.get("resolvedCheck")),
        "defended_by_king": bool(move.get("defendedByKing")),
        "shared_pressure": [
            {
                "moved_piece": str(item.get("moved_piece") or ""),
                "ally_pieces": [str(piece or "") for piece in (item.get("ally_pieces") or [])[:2]],
                "target_square": str(item.get("target_square") or ""),
                "target_piece": str(item.get("target_piece") or ""),
            }
            for item in (move.get("sharedPressure") if isinstance(move.get("sharedPressure"), list) else [])[:2]
            if isinstance(item, dict)
        ],
        "discovered_attack": (
            {
                "opener_piece": str((move.get("discoveredAttack") or {}).get("opener_piece") or ""),
                "attacker_piece": str((move.get("discoveredAttack") or {}).get("attacker_piece") or ""),
                "attacker_square": str((move.get("discoveredAttack") or {}).get("attacker_square") or ""),
                "target_piece": str((move.get("discoveredAttack") or {}).get("target_piece") or ""),
                "target_square": str((move.get("discoveredAttack") or {}).get("target_square") or ""),
                "defense_state": str((move.get("discoveredAttack") or {}).get("defense_state") or ""),
            }
            if isinstance(move.get("discoveredAttack"), dict)
            else {}
        ),
        "attacked_pieces": [
            {
                "piece": piece_label(item.get("piece")) or item.get("piece_name"),
                "square": item.get("square") or "",
            }
            for item in (move.get("attackedPieces") if isinstance(move.get("attackedPieces"), list) else [])[:3]
            if isinstance(item, dict)
        ],
        "new_attacked_pieces": [
            {
                "piece": piece_label(item.get("piece")) or item.get("piece_name"),
                "square": item.get("square") or "",
            }
            for item in (move.get("newAttackedPieces") if isinstance(move.get("newAttackedPieces"), list) else [])[:3]
            if isinstance(item, dict)
        ],
    }


def compact_board_snapshot(snapshot):
    if not isinstance(snapshot, list):
        return []
    items = []
    for item in snapshot:
      if not isinstance(item, dict):
          continue
      square = str(item.get("square") or "").strip()
      piece = str(item.get("piece") or "").strip()
      if not square or not piece:
          continue
      items.append({
          "square": square,
          "piece": piece,
          "color": item.get("color") or "",
          "piece_name": item.get("piece_name") or piece_label(piece),
      })
    return items


PIECE_VALUES = {
    "P": 1,
    "N": 3,
    "B": 3,
    "R": 5,
    "Q": 9,
    "K": 0,
}


def build_board_profile(board_snapshot):
    profile = {
        "white": {"material": 0, "pieces": 0, "queens": 0, "rooks": 0, "minors": 0, "pawns": 0},
        "black": {"material": 0, "pieces": 0, "queens": 0, "rooks": 0, "minors": 0, "pawns": 0},
        "total_non_king_pieces": 0,
        "phase": "middlegame",
    }
    for item in board_snapshot or []:
        if not isinstance(item, dict):
            continue
        color = item.get("color")
        piece = str(item.get("piece") or "").upper()
        if color not in profile or piece not in PIECE_VALUES:
            continue
        profile[color]["pieces"] += 1
        profile[color]["material"] += PIECE_VALUES[piece]
        profile["total_non_king_pieces"] += 1 if piece != "K" else 0
        if piece == "Q":
            profile[color]["queens"] += 1
        elif piece == "R":
            profile[color]["rooks"] += 1
        elif piece in ("B", "N"):
            profile[color]["minors"] += 1
        elif piece == "P":
            profile[color]["pawns"] += 1
    non_pawn_material = (
        profile["white"]["material"] - profile["white"]["pawns"] +
        profile["black"]["material"] - profile["black"]["pawns"]
    )
    if non_pawn_material <= 12:
        profile["phase"] = "endgame"
    elif non_pawn_material <= 24:
        profile["phase"] = "middlegame"
    else:
        profile["phase"] = "opening"
    return profile


def build_strategy_context(board_profile, current_eval, current_mate, player_to_advise):
    if current_mate and current_mate.get("mating_side"):
        return {
            "kind": "attack_king",
            "hint": "СѓСЃРёР»РёРІР°С‚СЊ Р°С‚Р°РєСѓ РЅР° РєРѕСЂРѕР»СЏ",
        }

    if not isinstance(board_profile, dict) or not current_eval:
        return {}

    advantage_side = current_eval.get("advantage_side")
    pawns = float(current_eval.get("pawns") or 0)
    phase = board_profile.get("phase") or "middlegame"
    queens_total = int(board_profile.get("white", {}).get("queens", 0) + board_profile.get("black", {}).get("queens", 0))
    rooks_total = int(board_profile.get("white", {}).get("rooks", 0) + board_profile.get("black", {}).get("rooks", 0))
    total_non_king = int(board_profile.get("total_non_king_pieces") or 0)

    if advantage_side and pawns >= 2.5:
        if phase == "endgame" or total_non_king <= 10:
            return {
                "kind": "convert_endgame",
                "hint": "РІРµСЃС‚Рё Рє РІС‹РёРіСЂР°РЅРЅРѕРјСѓ СЌРЅРґС€РїРёР»СЋ",
            }
        if queens_total > 0 or rooks_total > 0:
            return {
                "kind": "simplify",
                "hint": "РјРµРЅСЏС‚СЊ С‚СЏР¶РµР»С‹Рµ С„РёРіСѓСЂС‹ Рё СѓРїСЂРѕС‰Р°С‚СЊ РїРѕР·РёС†РёСЋ",
            }
        return {
            "kind": "improve",
            "hint": "СѓРєСЂРµРїР»СЏС‚СЊ РїРµСЂРµРІРµСЃ Рё РїРµСЂРµРІРѕРґРёС‚СЊ РµРіРѕ РІ РІС‹РёРіСЂС‹С€",
        }

    if advantage_side:
        return {
            "kind": "press",
            "hint": "СѓСЃРёР»РёРІР°С‚СЊ Р»СѓС‡С€РёРµ С„РёРіСѓСЂС‹ Рё РЅРµ РґР°РІР°С‚СЊ РєРѕРЅС‚СЂРёРіСЂСѓ",
        }

    if player_to_advise in ("white", "black"):
        return {
            "kind": "develop",
            "hint": "СЂР°Р·РІРёРІР°С‚СЊ С„РёРіСѓСЂС‹ Рё СѓР»СѓС‡С€Р°С‚СЊ РєРѕСЂРѕР»СЏ",
        }

    return {}


def move_word(count):
    count = abs(int(count or 0))
    if count % 10 == 1 and count % 100 != 11:
        return "С…РѕРґ"
    if count % 10 in (2, 3, 4) and count % 100 not in (12, 13, 14):
        return "С…РѕРґР°"
    return "С…РѕРґРѕРІ"


def build_forced_mate_context_from_line(line, fallback_side):
    if not isinstance(line, dict):
        return None
    return build_forced_mate_context(line.get("mate"), line.get("score_side") or fallback_side)


def normalize_score_side(side):
    if side in ("w", "white"):
        return "white"
    if side in ("b", "black"):
        return "black"
    return None


def opposite_side(side):
    if side == "white":
        return "black"
    if side == "black":
        return "white"
    return None


def line_score_cp(line):
    if not isinstance(line, dict):
        return None
    score = line.get("score") if isinstance(line.get("score"), dict) else {}
    explicit_pawns = line.get("score_pawns")
    if explicit_pawns is None:
        explicit_pawns = line.get("pawns")
    if explicit_pawns is None:
        explicit_pawns = score.get("pawns")
    if explicit_pawns is not None:
        try:
            return int(round(float(explicit_pawns) * 100))
        except (TypeError, ValueError):
            return None
    value = line.get("score_cp")
    if value is None:
        value = score.get("cp")
    if value is None:
        value = score.get("centipawns")
    if value is None:
        value = line.get("cp")
    try:
        numeric = float(value)
        if abs(numeric) <= 20 and not numeric.is_integer():
            return int(round(numeric * 100))
        return int(round(numeric))
    except (TypeError, ValueError):
        return None


def build_evaluation_context(line, fallback_side):
    cp = line_score_cp(line)
    if cp is None:
        return None
    score_side = normalize_score_side(line.get("score_side") or fallback_side)
    if not score_side:
        return None
    advantage_side = score_side if cp > 0 else opposite_side(score_side) if cp < 0 else None
    if not advantage_side:
        return {
            "cp": 0,
            "pawns": 0,
            "advantage_side": None,
            "advantage_label": "СЂР°РІРЅРѕ",
        }
    abs_cp = abs(cp)
    if abs_cp >= 700:
        label = "СЂРµС€Р°СЋС‰РµРµ РїСЂРµРёРјСѓС‰РµСЃС‚РІРѕ"
    elif abs_cp >= 350:
        label = "Р±РѕР»СЊС€РѕРµ РїСЂРµРёРјСѓС‰РµСЃС‚РІРѕ"
    elif abs_cp >= 150:
        label = "Р·Р°РјРµС‚РЅРѕРµ РїСЂРµРёРјСѓС‰РµСЃС‚РІРѕ"
    else:
        label = "РЅРµР±РѕР»СЊС€РѕРµ РїСЂРµРёРјСѓС‰РµСЃС‚РІРѕ"
    return {
        "cp": cp,
        "pawns": round(abs_cp / 100, 1),
        "advantage_side": advantage_side,
        "advantage_label": label,
    }


def build_blunder_context(previous_eval, current_eval, moving_side, previous_best_move=""):
    mover = normalize_score_side(moving_side)
    if not previous_eval or not current_eval or not mover:
        return None
    opponent = opposite_side(mover)
    previous_for_mover = previous_eval["pawns"] if previous_eval.get("advantage_side") == mover else -previous_eval["pawns"] if previous_eval.get("advantage_side") == opponent else 0
    current_for_mover = current_eval["pawns"] if current_eval.get("advantage_side") == mover else -current_eval["pawns"] if current_eval.get("advantage_side") == opponent else 0
    swing = round(previous_for_mover - current_for_mover, 1)
    if swing < 1.2:
        return None
    if current_eval.get("advantage_side") == opponent and (current_eval.get("pawns", 0) >= 1.5 or swing >= 2.5):
        severity = "Р·РµРІРѕРє"
    elif swing >= 4.0:
        severity = "СЃРµСЂСЊРµР·РЅР°СЏ РѕС€РёР±РєР°"
    else:
        severity = "РЅРµС‚РѕС‡РЅРѕСЃС‚СЊ"
    return {
        "kind": "blunder",
        "side": mover,
        "swing_pawns": swing,
        "severity": severity,
        "now_advantage_side": current_eval.get("advantage_side"),
        "best_move": previous_best_move or "",
    }


def side_eval_value(evaluation, side):
    if not evaluation or not side:
        return None
    if evaluation.get("advantage_side") == side:
        return evaluation.get("pawns", 0)
    if evaluation.get("advantage_side") == opposite_side(side):
        return -evaluation.get("pawns", 0)
    return 0


def build_eval_change_context(previous_eval, current_eval, moving_side):
    mover = normalize_score_side(moving_side)
    if not previous_eval or not current_eval or not mover:
        return None
    before = side_eval_value(previous_eval, mover)
    after = side_eval_value(current_eval, mover)
    if before is None or after is None:
        return None
    delta = round(after - before, 1)
    if abs(delta) < 0.5:
        kind = "stable"
    elif previous_eval.get("advantage_side") == current_eval.get("advantage_side") and abs(previous_eval.get("pawns", 0) - current_eval.get("pawns", 0)) < 0.75:
        kind = "advantage_preserved"
    elif delta > 0:
        kind = "improved"
    else:
        kind = "worsened"
    return {
        "side": mover,
        "delta_pawns": delta,
        "kind": kind,
        "previous_advantage_side": previous_eval.get("advantage_side"),
        "current_advantage_side": current_eval.get("advantage_side"),
        "previous_pawns": previous_eval.get("pawns"),
        "current_pawns": current_eval.get("pawns"),
        "major_worsening": delta <= -1.5,
        "major_improvement": delta >= 1.5,
    }


def build_check_defense_comment(move_details, eval_change):
    if not isinstance(move_details, dict):
        return ""
    if not move_details.get("was_in_check_before") or not move_details.get("resolved_check"):
        return ""
    if move_details.get("defended_by_king"):
        return ""
    piece = move_details.get("moving_piece_name") or "С„РёРіСѓСЂР°"
    piece_instr = piece_instrumental(piece)
    side = side_label(move_details.get("side"))
    quality = "С…РѕСЂРѕС€РµРµ СЂРµС€РµРЅРёРµ"
    if eval_change and eval_change.get("kind") == "improved":
        quality = "С‚РѕС‡РЅРѕРµ СЂРµС€РµРЅРёРµ"
    elif eval_change and eval_change.get("kind") == "worsened":
        return f"{side} Р·Р°С‰РёС‚РёР»РёСЃСЊ РѕС‚ С€Р°С…Р° С…РѕРґРѕРј {piece_instr}, РЅРѕ СЌС‚Рѕ РЅРµС‚РѕС‡РЅРѕРµ СЂРµС€РµРЅРёРµ."
    return f"{side} Р·Р°С‰РёС‚РёР»РёСЃСЊ РѕС‚ С€Р°С…Р° С…РѕРґРѕРј {piece_instr}, СЌС‚Рѕ {quality}."


def build_king_escape_from_check_comment(move_details, eval_change):
    if not isinstance(move_details, dict):
        return ""
    if not move_details.get("was_in_check_before") or not move_details.get("resolved_check"):
        return ""
    if not move_details.get("defended_by_king"):
        return ""
    side = side_label(move_details.get("side"))
    if eval_change and eval_change.get("kind") == "worsened":
        return f"{side} РєРѕСЂРѕР»СЊ РѕС‚РѕС€РµР» РѕС‚ С€Р°С…Р°, РЅРѕ Р±РµР·РѕРїР°СЃРЅРѕСЃС‚СЊ СѓР»СѓС‡С€РёС‚СЊ РЅРµ СѓРґР°Р»РѕСЃСЊ."
    if eval_change and eval_change.get("kind") == "improved":
        return f"{side} РєРѕСЂРѕР»СЊ РѕС‚РѕС€РµР» РѕС‚ С€Р°С…Р° Рё СЃРґРµР»Р°Р» РїРѕР·РёС†РёСЋ РєРѕСЂРѕР»СЏ Р±РµР·РѕРїР°СЃРЅРµРµ."
    return f"{side} РєРѕСЂРѕР»СЊ РѕС‚РѕС€РµР» РѕС‚ С€Р°С…Р° Рё СѓРґРµСЂР¶Р°Р» РїСЂРёРµРјР»РµРјСѓСЋ Р±РµР·РѕРїР°СЃРЅРѕСЃС‚СЊ."


def build_mate_defense_comment(coach_event, move_details):
    if not isinstance(coach_event, dict):
        return ""
    previous_mate = coach_event.get("previous_forced_mate") or {}
    current_mate = coach_event.get("current_forced_mate") or {}
    moving_side = normalize_score_side(coach_event.get("moving_side"))
    if not previous_mate.get("mating_side"):
        return ""
    if current_mate.get("mating_side"):
        return ""
    defending_side = previous_mate.get("defending_side")
    if moving_side and defending_side and moving_side != defending_side:
        return ""
    piece_instr = piece_instrumental((move_details or {}).get("moving_piece_name"))
    side = side_label(moving_side or defending_side)
    return f"{side} Р·Р°С‰РёС‚РёР»РёСЃСЊ РѕС‚ РјР°С‚Р° С…РѕРґРѕРј {piece_instr}."


def is_king_move_token(move_token):
    token = str(move_token or "").strip()
    if not token:
        return False
    token = token.lstrip(".вЂ¦")
    return token.startswith("K") or token.startswith("Рљ")


def build_single_king_escape_comment(move_details, current_line, current_mate):
    if not isinstance(move_details, dict) or not isinstance(current_line, dict) or not isinstance(current_mate, dict):
        return ""
    summary = str(move_details.get("summary") or "").strip()
    if "С€Р°С…" not in summary.lower():
        return ""
    pv_moves = current_line.get("pv_first_moves") or []
    if not pv_moves or not is_king_move_token(pv_moves[0]):
        return ""
    try:
        mate_moves = int(current_mate.get("moves") or 0)
    except (TypeError, ValueError):
        mate_moves = 0
    if mate_moves <= 0 or mate_moves > 4:
        return ""
    defending_side = current_mate.get("defending_side")
    if defending_side in ("white", "black"):
        return f"{summary.capitalize()}, Сѓ РєРѕСЂРѕР»СЏ {side_genitive(defending_side)} РѕСЃС‚Р°Р»РѕСЃСЊ РµРґРёРЅСЃС‚РІРµРЅРЅРѕРµ РїРѕР»Рµ РѕС‚С…РѕРґР°."
    return f"{summary.capitalize()}, Сѓ РєРѕСЂРѕР»СЏ РѕСЃС‚Р°Р»РѕСЃСЊ РµРґРёРЅСЃС‚РІРµРЅРЅРѕРµ РїРѕР»Рµ РѕС‚С…РѕРґР°."


def find_side_king_square(board_snapshot, side):
    king_piece = "K" if side == "white" else "k" if side == "black" else ""
    if not king_piece:
        return ""
    for item in board_snapshot or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("piece") or "") == king_piece:
            return str(item.get("square") or "")
    return ""


def same_diagonal(square_a, square_b):
    if len(str(square_a)) < 2 or len(str(square_b)) < 2:
        return False
    return abs(ord(square_a[0]) - ord(square_b[0])) == abs(int(square_a[1]) - int(square_b[1]))


def describe_checkmate_type(move_details, board_snapshot, defending_side):
    piece = str((move_details or {}).get("moving_piece_name") or "").strip()
    piece_instr = piece_instrumental(piece)
    to_sq = str((move_details or {}).get("to") or "")
    king_sq = find_side_king_square(board_snapshot, defending_side)
    if piece in ("Р»Р°РґСЊСЏ", "С„РµСЂР·СЊ") and len(king_sq) >= 2 and king_sq[1] in ("1", "8"):
        return f"РјР°С‚ {piece_instr} РїРѕ РїРѕСЃР»РµРґРЅРµР№ РіРѕСЂРёР·РѕРЅС‚Р°Р»Рё"
    if piece in ("Р»Р°РґСЊСЏ", "С„РµСЂР·СЊ") and len(to_sq) >= 2 and len(king_sq) >= 2 and (to_sq[0] == king_sq[0] or to_sq[1] == king_sq[1]):
        return f"Р»РёРЅРµР№РЅС‹Р№ РјР°С‚ {piece_instr}"
    if piece in ("СЃР»РѕРЅ", "С„РµСЂР·СЊ") and same_diagonal(to_sq, king_sq):
        return f"РјР°С‚ {piece_instr} РїРѕ РґРёР°РіРѕРЅР°Р»Рё"
    if piece == "РєРѕРЅСЊ":
        return "РјР°С‚ РєРѕРЅРµРј"
    if piece == "РїРµС€РєР°":
        return "РјР°С‚ РїРµС€РєРѕР№"
    if piece:
        return f"РјР°С‚ {piece_instr}"
    return "РјР°С‚"


def build_checkmate_comment(coach_event, move_details=None, board_snapshot=None):
    current_mate = coach_event.get("current_forced_mate") or {}
    mating_side = current_mate.get("mating_side")
    defending_side = current_mate.get("defending_side")
    if not mating_side:
        return ""
    mate_type = describe_checkmate_type(move_details or {}, board_snapshot or [], defending_side)
    has_brilliant_idea = bool(
        (move_details or {}).get("discovered_attack")
        or (move_details or {}).get("shared_pressure")
        or (move_details or {}).get("captured_piece")
    )
    quality = "СЌС‚Рѕ Р±Р»РµСЃС‚СЏС‰РёР№ С…РѕРґ" if has_brilliant_idea else "СЌС‚Рѕ Р»СѓС‡С€РёР№ С…РѕРґ"
    return f"{side_label(mating_side)} РїРѕСЃС‚Р°РІРёР»Рё {mate_type}, {quality}."


def build_deterministic_coach_comment(coach_event, move_details=None, board_snapshot=None):
    if not isinstance(coach_event, dict):
        return ""
    kind = coach_event.get("kind")
    current_mate = coach_event.get("current_forced_mate") or {}
    previous_mate = coach_event.get("previous_forced_mate") or {}
    if kind == "checkmate" and current_mate.get("mating_side"):
        return build_checkmate_comment(coach_event, move_details, board_snapshot)
    if kind == "mate_appeared" and current_mate.get("moves"):
        moves = int(current_mate["moves"])
        return f"{side_label(current_mate.get('mating_side'))} С„РѕСЂСЃРёСЂСѓСЋС‚ РјР°С‚ РІ {moves} {move_word(moves)}."
    if kind == "mate_in_one" and current_mate.get("mating_side"):
        return f"РЈ {side_genitive(current_mate.get('mating_side'))} РѕСЃС‚Р°Р»СЃСЏ РїРѕСЃР»РµРґРЅРёР№ С…РѕРґ РґРѕ РјР°С‚Р°."
    if kind == "missed_mate" and previous_mate.get("mating_side"):
        return f"{side_label(previous_mate.get('mating_side'))} СѓРїСѓСЃС‚РёР»Рё РјР°С‚РѕРІСѓСЋ РІРѕР·РјРѕР¶РЅРѕСЃС‚СЊ."
    return ""


def validate_comment_against_eval(comment, current_eval, current_mate=None, eval_change=None):
    comment = str(comment or "").strip()
    if current_mate and current_mate.get("mating_side"):
        lower = comment.lower()
        if "СЂР°РІРЅ" in lower or "0.00" in lower:
            return f"{side_label(current_mate.get('mating_side'))} СЃРѕС…СЂР°РЅСЏСЋС‚ С„РѕСЂСЃРёСЂРѕРІР°РЅРЅСѓСЋ РјР°С‚РѕРІСѓСЋ СѓРіСЂРѕР·Сѓ."
        return comment
    if not current_eval or not current_eval.get("advantage_side"):
        return comment

    advantage_side = current_eval["advantage_side"]
    lower = comment.lower()
    same_side_had_advantage = bool(eval_change and eval_change.get("previous_advantage_side") == advantage_side)
    moving_side = normalize_score_side(eval_change.get("side")) if eval_change else None
    white_positive = "Р±РµР»" in lower and any(word in lower for word in ("РІС‹РёРіСЂ", "Р°С‚Р°Рє", "РёРЅРёС†РёР°С‚РёРІ", "РґР°РІ", "РїСЂРµРёРјСѓС‰"))
    black_positive = ("С‡РµСЂРЅ" in lower or "С‡С‘СЂРЅ" in lower) and any(word in lower for word in ("РІС‹РёРіСЂ", "Р°С‚Р°Рє", "РёРЅРёС†РёР°С‚РёРІ", "РґР°РІ", "РїСЂРµРёРјСѓС‰"))

    if moving_side and moving_side != advantage_side and any(word in lower for word in ("СЃРѕС…СЂР°РЅРёР»", "СЃРѕС…СЂР°РЅРёР»Р°", "СЃРѕС…СЂР°РЅРёР»Рё", "СѓСЃРёР»РёР»", "СѓСЃРёР»РёР»Р°", "СѓСЃРёР»РёР»Рё")):
        return f"РџРµСЂРµРІРµСЃ РѕСЃС‚Р°Р»СЃСЏ Сѓ {side_genitive(advantage_side)}."

    if advantage_side == "black" and white_positive:
        if moving_side == "white":
            return f"РџРµСЂРµРІРµСЃ РѕСЃС‚Р°Р»СЃСЏ Сѓ {side_genitive(advantage_side)}."
        verb = "СѓСЃРёР»РёР»Рё" if eval_change and eval_change.get("kind") == "improved" else "СЃРѕС…СЂР°РЅРёР»Рё"
        return f"Р§РµСЂРЅС‹Рµ {verb} {current_eval['advantage_label']}."
    if advantage_side == "white" and black_positive:
        if moving_side == "black":
            return f"РџРµСЂРµРІРµСЃ РѕСЃС‚Р°Р»СЃСЏ Сѓ {side_genitive(advantage_side)}."
        verb = "СѓСЃРёР»РёР»Рё" if eval_change and eval_change.get("kind") == "improved" else "СЃРѕС…СЂР°РЅРёР»Рё"
        return f"Р‘РµР»С‹Рµ {verb} {current_eval['advantage_label']}."

    if same_side_had_advantage and any(word in lower for word in ("РїРѕР»СѓС‡", "Р·Р°Р±СЂР°Р»", "Р·Р°Р±СЂР°Р»Р°", "РёРЅРёС†РёР°С‚РёРІ", "РїСЂРµРёРјСѓС‰")):
        verb = "СѓСЃРёР»РёР»Рё" if eval_change and eval_change.get("kind") == "improved" else "СЃРѕС…СЂР°РЅРёР»Рё"
        return f"{side_label(advantage_side)} {verb} {current_eval['advantage_label']}."
    if eval_change and eval_change.get("kind") in ("stable", "advantage_preserved") and "РїРѕР»СѓС‡" in lower and "РїСЂРµРёРјСѓС‰" in lower:
        return f"{side_label(advantage_side)} СЃРѕС…СЂР°РЅРёР»Рё {current_eval['advantage_label']}."
    return comment


def is_generic_position_comment(comment):
    lower = str(comment or "").lower()
    if not lower:
        return False
    has_generic = (
        ("СЃРѕС…СЂР°РЅРёР»" in lower and ("РїСЂРµРёРјСѓС‰" in lower or "РїРµСЂРµРІРµСЃ" in lower))
        or ("СѓСЃРёР»РёР»" in lower and ("РїСЂРµРёРјСѓС‰" in lower or "РїРµСЂРµРІРµСЃ" in lower))
        or ("РѕС‚Р±РёР»РёСЃСЊ" in lower and "РїРµСЂРµРІРµСЃ" in lower)
        or ("РїРѕР·РёС†РёСЏ СЃРѕС…СЂР°РЅРёР»Р° РїСЂРµР¶РЅРёР№ С…Р°СЂР°РєС‚РµСЂ" in lower)
    )
    has_concrete = any(
        token in lower
        for token in ("СЃСЉРµР»", "РІР·СЏР»", "РґР°Р» С€Р°С…", "РІРёР»Рє", "СЃРІСЏР·Рє", "Р·Р°С‰РёС‚РёР»", "Р·РµРІ", "РЅР°РїР°Р»")
    )
    return has_generic and not has_concrete


def should_mark_best_move(deviated, eval_change_context, user_move, current_line):
    if deviated:
        return False
    if not isinstance(eval_change_context, dict):
        return False
    if eval_change_context.get("kind") not in ("stable", "advantage_preserved", "improved"):
        return False
    best_move = str((current_line or {}).get("move") or "").strip()
    played_move = str(user_move or "").strip()
    if not best_move or not played_move or best_move != played_move:
        return False
    # Р РµРґРєРёР№, РЅРѕ СЃС‚Р°Р±РёР»СЊРЅС‹Р№ С‚СЂРёРіРіРµСЂ (РїСЂРёРјРµСЂРЅРѕ РІ 8% СЃР»СѓС‡Р°РµРІ).
    return (sum(ord(ch) for ch in played_move) % 12) == 0


def deterministic_pick(options, seed):
    pool = [str(item).strip().rstrip(".!?") for item in (options or []) if str(item or "").strip()]
    if not pool:
        return ""
    token = str(seed or "")
    digest = hashlib.sha1(token.encode("utf-8")).hexdigest()
    index = int(digest[:8], 16) % len(pool)
    return pool[index]


def normalize_comment_key(text):
    cleaned = re.sub(r"[^a-zР°-СЏС‘0-9\s]", " ", str(text or "").lower(), flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return " ".join(cleaned.split()[:8])


def comment_similarity_key_tokens(text):
    normalized = normalize_comment_key(text)
    stopwords = {
        "Рё", "РЅРѕ", "Р°", "СЌС‚Рѕ", "РїРѕСЃР»Рµ", "С…РѕРґ", "С…РѕРґР°", "С‚РµРїРµСЂСЊ", "РґР°Р»СЊС€Рµ",
        "РїРѕР·РёС†РёСЏ", "Р±РµР»С‹Рµ", "С‡РµСЂРЅС‹Рµ", "С‡С‘СЂРЅС‹Рµ", "Сѓ", "РЅР°", "РІ", "Рє", "РїРѕ",
        "РґР»СЏ", "СЃ", "Р¶Рµ", "РµС‰Рµ", "РµС‰С‘",
    }
    return {
        token
        for token in normalized.split()
        if len(token) > 3 and token not in stopwords
    }


def is_too_similar_comment(comment, previous):
    current_tokens = comment_similarity_key_tokens(comment)
    previous_tokens = comment_similarity_key_tokens(previous)
    if not current_tokens or not previous_tokens:
        return False
    overlap = len(current_tokens & previous_tokens)
    smallest = min(len(current_tokens), len(previous_tokens))
    return smallest >= 3 and (overlap / smallest) >= 0.7


def avoid_recent_comment_repetition(comment, recent_comments):
    key = normalize_comment_key(comment)
    if not key:
        return str(comment or "").strip()
    recent_keys = {normalize_comment_key(item) for item in (recent_comments or []) if str(item or "").strip()}
    similar_to_recent = any(is_too_similar_comment(comment, item) for item in (recent_comments or []))
    if key not in recent_keys and not similar_to_recent:
        return str(comment or "").strip()
    variants = [
        "РҐРѕРґ СЃРѕС…СЂР°РЅСЏРµС‚ СЂР°Р±РѕС‡РёР№ РїР»Р°РЅ, РЅРѕ РІР°Р¶РЅР° С‚РѕС‡РЅРѕСЃС‚СЊ РІ Р±Р»РёР¶Р°Р№С€РµРј РїСЂРѕРґРѕР»Р¶РµРЅРёРё.",
        "РРґРµСЏ С…РѕРґР° РїРѕРЅСЏС‚РЅР°, РґР°Р»СЊС€Рµ СЂРµС€Р°РµС‚ РєР°С‡РµСЃС‚РІРѕ РєРѕРЅРєСЂРµС‚РЅРѕРіРѕ СЂР°СЃС‡РµС‚Р°.",
        "РџРѕР·РёС†РёСЏ С‚СЂРµР±СѓРµС‚ Р°РєРєСѓСЂР°С‚РЅРѕРіРѕ РїСЂРѕРґРѕР»Р¶РµРЅРёСЏ, С‡С‚РѕР±С‹ РЅРµ РѕС‚РїСѓСЃС‚РёС‚СЊ РїРµСЂРµРІРµСЃ."
    ]
    alt = deterministic_pick(variants, f"{key}|{len(recent_keys)}")
    return alt or str(comment or "").strip()


def should_add_context_tail(comment, summary):
    comment_text = first_sentence(comment).strip().lower().rstrip(".!?")
    summary_text = str(summary or "").strip().lower().rstrip(".!?")
    if not comment_text or not summary_text:
        return False
    if comment_text == summary_text:
        return True
    if comment_text.startswith(summary_text) and len(comment_text.split()) <= len(summary_text.split()) + 2:
        return True
    return is_generic_position_comment(comment_text)


def build_context_tail(move_details, eval_change, current_eval, tactical_context, strategy_context, current_line, summary):
    mover = normalize_score_side((move_details or {}).get("side"))
    advantage_side = (current_eval or {}).get("advantage_side")
    eval_kind = (eval_change or {}).get("kind")
    delta = float((eval_change or {}).get("delta_pawns") or 0)
    strategy_kind = str((strategy_context or {}).get("kind") or "")
    is_capture = bool((move_details or {}).get("captured_piece"))
    is_exchange = bool((tactical_context or {}).get("capture_is_exchange"))
    reply_is_capture = bool((tactical_context or {}).get("reply_is_capture"))
    fork_signal = bool((tactical_context or {}).get("is_fork"))
    discovered = (move_details or {}).get("discovered_attack") if isinstance((move_details or {}).get("discovered_attack"), dict) else {}
    shared_pressure = (move_details or {}).get("shared_pressure") if isinstance((move_details or {}).get("shared_pressure"), list) else []
    cp_value = (current_eval or {}).get("cp")
    try:
        cp_abs = abs(int(cp_value))
    except (TypeError, ValueError):
        cp_abs = 0
    seed = "|".join(
        [
            str((move_details or {}).get("uci") or ""),
            str(summary or ""),
            str((current_line or {}).get("move") or ""),
            str(eval_kind or ""),
            str(round(delta, 1)),
            str(advantage_side or ""),
            strategy_kind,
        ]
    )
    candidates = []
    if discovered.get("attacker_piece") and discovered.get("target_piece"):
        target = str(discovered.get("target_piece") or "").strip()
        attacker = str(discovered.get("attacker_piece") or "").strip()
        opener = str(discovered.get("opener_piece") or "").strip()
        defense_state = str(discovered.get("defense_state") or "").strip()
        if defense_state == "undefended":
            defense_tail = "Рё С†РµР»СЊ РїРѕС‡С‚Рё Р±РµР· Р·Р°С‰РёС‚С‹"
        elif defense_state == "overloaded":
            defense_tail = "Рё С†РµР»СЊ С‚СЂСѓРґРЅРѕ СѓРґРµСЂР¶Р°С‚СЊ РёР·-Р·Р° РїРµСЂРµРіСЂСѓР·РєРё Р·Р°С‰РёС‚С‹"
        else:
            defense_tail = "Рё Р·Р°С‰РёС‚РЅРёРєР°Рј С‚СЏР¶РµР»Рѕ СѓСЃРїРµС‚СЊ"
        candidates.append(f"Рё {opener} РѕС‚РєСЂС‹Р» Р»РёРЅРёСЋ: {attacker} С‚РµРїРµСЂСЊ РґР°РІРёС‚ РЅР° {target}, {defense_tail}")
    if shared_pressure:
        sample = shared_pressure[0] if isinstance(shared_pressure[0], dict) else {}
        allies = ", ".join([str(item).strip() for item in sample.get("ally_pieces", []) if str(item).strip()])
        moved_piece = str(sample.get("moved_piece") or "").strip() or "С„РёРіСѓСЂР°"
        target_piece = str(sample.get("target_piece") or "").strip()
        if allies and target_piece:
            candidates.append(f"Рё СЌС‚Рѕ СЃРѕРІРјРµСЃС‚РЅРѕРµ РґР°РІР»РµРЅРёРµ: {moved_piece} Рё {allies} Р°С‚Р°РєСѓСЋС‚ {target_piece}")
    if fork_signal:
        if eval_kind == "worsened":
            candidates.extend(
                [
                    "Рё СЌС‚Рѕ С‚Р°РєС‚РёС‡РµСЃРєРёР№ Р·РµРІРѕРє СЃ РїРѕС‚РµСЂРµР№ РјР°С‚РµСЂРёР°Р»Р°",
                    "Рё С‚Р°РєС‚РёС‡РµСЃРєР°СЏ РѕС€РёР±РєР° СЃСЂР°Р·Сѓ РѕР±РѕСЃС‚СЂРёР»Р° РїСЂРѕР±Р»РµРјС‹",
                ]
            )
        else:
            candidates.extend(
                [
                    "Рё СЌС‚Р° С‚Р°РєС‚РёРєР° СЃС‚РµСЃРЅСЏРµС‚ С„РёРіСѓСЂС‹ СЃРѕРїРµСЂРЅРёРєР°",
                    "Рё С‚РµРїРµСЂСЊ С‚Р°РєС‚РёС‡РµСЃРєРёРµ СѓРіСЂРѕР·С‹ РїСЂРёС…РѕРґРёС‚СЃСЏ СЃС‡РёС‚Р°С‚СЊ РѕС‡РµРЅСЊ С‚РѕС‡РЅРѕ",
                ]
            )
    if is_capture and is_exchange:
        candidates.extend(
            [
                "Рё РґР°Р»СЊС€Рµ РїРѕР·РёС†РёСЏ СѓС…РѕРґРёС‚ РІ СЂР°Р·РјРµРЅ",
                "Рё РїРѕСЃР»Рµ СЌС‚РѕРіРѕ РЅР°С‡РёРЅР°РµС‚СЃСЏ С„РѕСЂСЃРёСЂРѕРІР°РЅРЅС‹Р№ СЂР°Р·РјРµРЅ",
            ]
        )
    elif is_capture and reply_is_capture:
        candidates.extend(
            [
                "РЅРѕ РґР°Р»СЊС€Рµ РЅСѓР¶РЅРѕ С‚РѕС‡РЅРѕ СЃС‡РёС‚Р°С‚СЊ РѕС‚РІРµС‚РЅС‹Рµ РІР·СЏС‚РёСЏ",
                "Рё СЃРѕРїРµСЂРЅРёРє РїРѕС‡С‚Рё РЅР°РІРµСЂРЅСЏРєР° РѕС‚РІРµС‚РёС‚ РІР·СЏС‚РёРµРј",
            ]
        )
    if eval_kind == "worsened":
        if delta <= -2.5:
            candidates.extend(
                [
                    "Рё РїРѕСЃР»Рµ СЌС‚РѕРіРѕ РїРѕР·РёС†РёСЏ СЂРµР·РєРѕ СѓС…СѓРґС€РёР»Р°СЃСЊ",
                    "Рё С…РѕРґ РѕС‚РґР°Р» РєР»СЋС‡РµРІСѓСЋ РёРЅРёС†РёР°С‚РёРІСѓ",
                ]
            )
        elif delta <= -1.5:
            candidates.extend(
                [
                    "Рё СЌС‚РѕС‚ С…РѕРґ Р·Р°РјРµС‚РЅРѕ РѕСЃР»Р°Р±РёР» РїРѕР·РёС†РёСЋ",
                    "РЅРѕ РїРѕСЃР»Рµ РЅРµРіРѕ СЃС‚Р°Р»Рѕ С‚СЏР¶РµР»РµРµ РґРµСЂР¶Р°С‚СЊ РѕР±РѕСЂРѕРЅСѓ",
                ]
            )
        else:
            candidates.append("РЅРѕ С…РѕРґ РѕРєР°Р·Р°Р»СЃСЏ РЅРµС‚РѕС‡РЅС‹Рј")
    elif eval_kind == "improved":
        if delta >= 2.0:
            candidates.extend(
                [
                    "Рё СЌС‚Рѕ СЂРµР·РєРѕ СѓСЃРёР»РёР»Рѕ РґР°РІР»РµРЅРёРµ",
                    "Рё РїРѕСЃР»Рµ РЅРµРіРѕ РїРѕР·РёС†РёСЏ СЃС‚Р°Р»Р° РЅР°РјРЅРѕРіРѕ РїСЂРѕС‰Рµ РґР»СЏ СЂРµР°Р»РёР·Р°С†РёРё",
                ]
            )
        elif delta >= 0.8:
            candidates.extend(
                [
                    "Рё СЌС‚Рѕ СѓРєСЂРµРїРёР»Рѕ РїРѕР·РёС†РёСЋ",
                    "Рё РёРЅРёС†РёР°С‚РёРІР° СЃС‚Р°Р»Р° СѓСЃС‚РѕР№С‡РёРІРµРµ",
                ]
            )
    elif eval_kind in ("stable", "advantage_preserved"):
        if strategy_kind == "convert_endgame" and mover and mover == advantage_side:
            candidates.extend(
                [
                    f"Рё {side_label(advantage_side).lower()} РІС‹РіРѕРґРЅРѕ РїРµСЂРµРІРѕРґРёС‚СЊ РІ СЌРЅРґС€РїРёР»СЊ",
                    f"Рё Сѓ {side_genitive(advantage_side)} РїР»Р°РЅ РЅР° РІС‹РёРіСЂР°РЅРЅС‹Р№ СЌРЅРґС€РїРёР»СЊ",
                ]
            )
        elif strategy_kind == "simplify" and mover and mover == advantage_side:
            candidates.extend(
                [
                    f"Рё {side_label(advantage_side).lower()} РІС‹РіРѕРґРЅРѕ РјРµРЅСЏС‚СЊ С‚СЏР¶РµР»С‹Рµ С„РёРіСѓСЂС‹",
                    f"Рё СѓРїСЂРѕС‰РµРЅРёРµ РІС‹РіРѕРґРЅРµРµ РёРјРµРЅРЅРѕ РґР»СЏ {side_genitive(advantage_side)}",
                ]
            )
        elif strategy_kind == "press" and mover and mover == advantage_side:
            candidates.extend(
                [
                    "Рё С‚РµРїРµСЂСЊ РІР°Р¶РЅРѕ РЅРµ РѕС‚РїСѓСЃРєР°С‚СЊ РґР°РІР»РµРЅРёРµ",
                    "Рё СЃРµР№С‡Р°СЃ РіР»Р°РІРЅРѕРµ РЅРµ РґР°С‚СЊ РєРѕРЅС‚СЂРёРіСЂС‹",
                ]
            )
        elif strategy_kind == "develop":
            candidates.extend(
                [
                    "Рё СЃР»РµРґСѓСЋС‰РёР№ С€Р°Рі вЂ” СЃРїРѕРєРѕР№РЅРѕ Р·Р°РІРµСЂС€РёС‚СЊ СЂР°Р·РІРёС‚РёРµ",
                    "Рё РґР°Р»СЊС€Рµ РІР°Р¶РЅРѕ Р±С‹СЃС‚СЂРµРµ РїРѕРґРєР»СЋС‡РёС‚СЊ РѕСЃС‚Р°Р»СЊРЅС‹Рµ С„РёРіСѓСЂС‹",
                ]
            )
    if advantage_side and not candidates:
        if int(hashlib.sha1(seed.encode("utf-8")).hexdigest()[:2], 16) % 4 == 0:
            if cp_abs >= 600:
                candidates.append(f"Рё Сѓ {side_genitive(advantage_side)} СѓР¶Рµ РѕС‡РµРЅСЊ РєРѕРјС„РѕСЂС‚РЅР°СЏ РїРѕР·РёС†РёСЏ")
            else:
                candidates.append(f"Рё РїРµСЂРµРІРµСЃ Сѓ {side_genitive(advantage_side)} СЃРѕС…СЂР°РЅСЏРµС‚СЃСЏ")
    if not candidates:
        candidates.extend(
            [
                "Рё С‚РµРїРµСЂСЊ РІР°Р¶РЅРѕ СѓРґРµСЂР¶Р°С‚СЊ С‚РµРјРї Рё РєРѕРѕСЂРґРёРЅР°С†РёСЋ С„РёРіСѓСЂ",
                "Рё РїРѕР·РёС†РёСЏ С‚СЂРµР±СѓРµС‚ С‚РѕС‡РЅРѕРіРѕ РїСЂРѕРґРѕР»Р¶РµРЅРёСЏ Р±РµР· Р»РёС€РЅРµРіРѕ СЂРёСЃРєР°",
            ]
        )
    return deterministic_pick(candidates, seed)


def looks_like_mojibake_fragment(text):
    s = str(text or "")
    if not s:
        return False
    latin1_noise = sum(1 for ch in s if 0x00A0 <= ord(ch) <= 0x00FF)
    rare_cyr_noise = sum(
        1
        for ch in s
        if (0x0452 <= ord(ch) <= 0x04FF) and ord(ch) not in (0x0451,)
    )
    basic_cyr = sum(1 for ch in s if (0x0410 <= ord(ch) <= 0x044F) or ord(ch) == 0x0451)
    if rare_cyr_noise >= 1 and latin1_noise >= 1:
        return True
    if latin1_noise >= 2 and basic_cyr >= 2:
        return True
    return False


def cleanup_mojibake_comment(comment):
    text = str(comment or "").strip()
    if not text:
        return text
    def _mojibake_score(value):
        sample = str(value or "")
        return sum(sample.count(ch) for ch in ("Р", "С", "Ð", "Ñ", "Ѓ", "‚", "€", "™", "љ", "ћ", "џ"))
    # Recover classic "РЈ Р±Рµ..." mojibake produced by cp1251/utf8 mismatch.
    try:
        repaired = text.encode("cp1251", errors="strict").decode("utf-8", errors="strict").strip()
        if repaired and _mojibake_score(repaired) < _mojibake_score(text):
            text = repaired
    except Exception:
        pass
    if looks_like_mojibake_fragment(text):
        try:
            repaired = text.encode("cp1251", errors="strict").decode("utf-8", errors="strict").strip()
            if repaired:
                text = repaired
        except Exception:
            pass
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if len(parts) > 1 and looks_like_mojibake_fragment(parts[0]) and not looks_like_mojibake_fragment(parts[1]):
        text = ", ".join(parts[1:])
    tokens = text.split()
    filtered = [tok for tok in tokens if not (len(tok) >= 4 and looks_like_mojibake_fragment(tok))]
    if filtered:
        text = " ".join(filtered)
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    text = re.sub(r"\s{2,}", " ", text).strip(" ,")
    return text


def polish_coach_comment(comment, move_details=None, eval_change=None, current_mate=None):
    comment = str(comment or "").strip()
    comment = re.sub(r"^(?:РѕР№|РїРѕС…РѕР¶Рµ|РЅРµРїСЂРёСЏС‚РЅРѕ|С…РѕСЂРѕС€Рѕ),\s*", "", comment, flags=re.IGNORECASE)
    replacements = {
        "РЅРµ СЃРІРµС‚РёС‚": "С‚СЂСѓРґРЅРѕ Р·Р°С‰РёС‰Р°С‚СЊСЃСЏ",
        "РІСЃС‘ РїР»РѕС…Рѕ": "РїРѕР·РёС†РёСЏ СЃРµСЂСЊС‘Р·РЅРѕ СѓС…СѓРґС€РёР»Р°СЃСЊ",
        "РїСЂРёРїР»С‹Р»": "РїРѕРїР°Р» РІ С‚СЏР¶С‘Р»СѓСЋ РїРѕР·РёС†РёСЋ",
        "РјРѕРЅРіРѕР»СЊРЅРѕРµ РєР°С‡РµСЃС‚РІРѕ": "РјР°С‚РµСЂРёР°Р»СЊРЅС‹Р№ РїРµСЂРµРІРµСЃ",
        "РјРѕРЅРіРѕР»СЊРЅРѕРµ": "РјР°С‚РµСЂРёР°Р»СЊРЅРѕРµ",
        "РЅРёС‡РµРіРѕ РЅРµ РёР·РјРµРЅРёР»РѕСЃСЊ": "РїРѕР·РёС†РёСЏ СЃРѕС…СЂР°РЅРёР»Р° РїСЂРµР¶РЅРёР№ С…Р°СЂР°РєС‚РµСЂ",
        "РІС‹РїСѓСЃС‚РёРІ": "РѕСЃС‚Р°РІРёРІ",
        "РІС‹РїСѓСЃС‚РёР»": "РѕСЃС‚Р°РІРёР»",
        "РІС‹РїСѓСЃС‚РёР»Р°": "РѕСЃС‚Р°РІРёР»Р°",
        "РЅР° РїРµС€РєР°": "РЅР° РїРµС€РєСѓ",
        "Р°С‚Р°РєРѕРІР°Р» РїРµС€РєР°": "Р°С‚Р°РєРѕРІР°Р» РїРµС€РєСѓ",
        "РїРµС€РєР° РЅР°РїР°Р»": "РїРµС€РєР° РЅР°РїР°Р»Р°",
        "РїРµС€РєРѕР№ С…РѕРґ": "РїРµС€РєРѕР№",
        "РїРѕ РїРµСЂРІРѕР№ Р»РёРЅРёРё": "",
        "СѓРїСЂРѕС‰Р°С‚СЊ С‚СЏР¶С‘Р»С‹Рµ": "СѓРїСЂРѕС‰Р°С‚СЊ РїРѕР·РёС†РёСЋ",
        "СѓРїСЂРѕС‰Р°С‚СЊ С‚СЏР¶РµР»С‹Рµ": "СѓРїСЂРѕС‰Р°С‚СЊ РїРѕР·РёС†РёСЋ",
        "РѕСЃРЅРѕРІРЅРѕР№ Р±Р°Р»Р°РЅСЃ РїРѕР·РёС†РёРё СЃРѕС…СЂР°РЅРёР»СЃСЏ": "РїРѕР·РёС†РёСЏ СЃРѕС…СЂР°РЅРёР»Р° РїСЂРµР¶РЅРёР№ С…Р°СЂР°РєС‚РµСЂ",
        "С‡РµСЂРЅС‹Рµ РѕС‚РІРµС‡Р°СЋС‚": "С‡РµСЂРЅС‹Рµ РїСЂРѕРґРѕР»Р¶Р°СЋС‚",
        "Р±РµР»С‹Рµ РѕС‚РІРµС‡Р°СЋС‚": "Р±РµР»С‹Рµ РїСЂРѕРґРѕР»Р¶Р°СЋС‚",
        "РѕС‚РІРµС‚": "РїСЂРѕРґРѕР»Р¶РµРЅРёРµ",
    }
    lower = comment.lower()
    for bad, good in replacements.items():
        if bad in lower:
            comment = comment.replace(bad, good).replace(bad.capitalize(), good.capitalize())
    comment = re.sub(r"РїРѕСЃР»Рµ С…РѕРґР°\s+[a-h][1-8]\s*[вЂ“-]\s*[a-h][1-8]", "РїРѕСЃР»Рµ С…РѕРґР°", comment, flags=re.IGNORECASE)
    comment = re.sub(r"\b[a-h][1-8]\s*[вЂ“-]\s*[a-h][1-8]\b", "С…РѕРґ", comment, flags=re.IGNORECASE)
    comment = re.sub(r"\b(С‡РµСЂРЅС‹Рµ|Р±РµР»С‹Рµ)\s+РѕС‚РІРµС‡Р°СЋС‚\s+[^\s,.;:]+", r"\1 РїСЂРѕРґРѕР»Р¶Р°СЋС‚", comment, flags=re.IGNORECASE)
    comment = re.sub(r"\s+Р°\s+РІС‹\s+", " ", comment, flags=re.IGNORECASE)
    comment = re.sub(r"\bРІС‹\s+", "", comment, flags=re.IGNORECASE)
    comment = re.sub(r"\b(С‡РµСЂРЅС‹Рµ|Р±РµР»С‹Рµ)\s+РѕС‚РІРµС‡Р°СЋС‚\b", r"\1 РїСЂРѕРґРѕР»Р¶Р°СЋС‚", comment, flags=re.IGNORECASE)
    move_side = normalize_score_side(move_details.get("side")) if isinstance(move_details, dict) else None
    if move_side == "white" and "С‡РµСЂРЅС‹Рµ РїРѕС‚РµСЂСЏР»Рё С‚РµРјРї" in comment.lower():
        comment = re.sub(r"С‡РµСЂРЅС‹Рµ РїРѕС‚РµСЂСЏР»Рё С‚РµРјРї", "С…РѕРґ С‡РµСЂРЅС‹С… РѕРєР°Р·Р°Р»СЃСЏ РЅРµС‚РѕС‡РЅС‹Рј", comment, flags=re.IGNORECASE)
    if move_side == "black" and "Р±РµР»С‹Рµ РїРѕС‚РµСЂСЏР»Рё С‚РµРјРї" in comment.lower():
        comment = re.sub(r"Р±РµР»С‹Рµ РїРѕС‚РµСЂСЏР»Рё С‚РµРјРї", "С…РѕРґ Р±РµР»С‹С… РѕРєР°Р·Р°Р»СЃСЏ РЅРµС‚РѕС‡РЅС‹Рј", comment, flags=re.IGNORECASE)
    comment = comment.replace("РҐРѕРґ С…РѕРґ", "РҐРѕРґ").replace("С…РѕРґ С…РѕРґ", "С…РѕРґ")
    comment = re.sub(r"\s{2,}", " ", comment).strip(" ,;:-")
    if move_details and move_details.get("summary"):
        summary = move_details["summary"]
        if current_mate and "СЃРѕС…СЂР°РЅСЏ" in comment.lower() and "РјР°С‚РѕРІ" in comment.lower():
            comment = mate_support_phrase(summary)
        if any(phrase in comment.lower() for phrase in ("СЃРѕС…СЂР°РЅРёР»Рё СЂРµС€Р°СЋС‰РµРµ РїСЂРµРёРјСѓС‰РµСЃС‚РІРѕ", "СЃРѕС…СЂР°РЅРёР»Рё Р±РѕР»СЊС€РѕРµ РїСЂРµРёРјСѓС‰РµСЃС‚РІРѕ", "СЃРѕС…СЂР°РЅРёР»Рё РЅРµР±РѕР»СЊС€РѕРµ РїСЂРµРёРјСѓС‰РµСЃС‚РІРѕ", "РїРѕР·РёС†РёСЏ СЃРѕС…СЂР°РЅРёР»Р° РїСЂРµР¶РЅРёР№ С…Р°СЂР°РєС‚РµСЂ", "СЃРѕС…СЂР°РЅРёР»Рё РёРЅРёС†РёР°С‚РёРІСѓ")):
            if eval_change and eval_change.get("kind") == "worsened" and eval_change.get("major_worsening"):
                comment = f"{summary.capitalize()}, РЅРѕ РїРѕР·РёС†РёСЏ РїРѕСЃР»Рµ СЌС‚РѕРіРѕ СѓС…СѓРґС€РёР»Р°СЃСЊ."
            else:
                comment = f"{summary.capitalize()}."
    if comment:
        comment = comment[0].upper() + comment[1:]
    return comment


def build_coach_comment(comment, move_details=None, eval_change=None, current_mate=None):
    comment = str(comment or "").strip()
    banned_prefixes = (
        "РѕР№, ",
        "РЅРµРїСЂРёСЏС‚РЅРѕ: ",
        "РЅРµРїСЂРёСЏС‚РЅРѕ, ",
        "РїРѕС…РѕР¶Рµ, ",
        "С…РѕСЂРѕС€Рѕ, ",
        "РІРѕС‚ СЌС‚Рѕ СѓРґР°СЂ: ",
        "РІРѕС‚ СЌС‚Рѕ СѓРґР°СЂ, ",
    )
    lower = comment.lower()
    for prefix in banned_prefixes:
        if lower.startswith(prefix):
            comment = comment[len(prefix):].lstrip()
            break
    replacements = {
        "РЅРµ СЃРІРµС‚РёС‚": "С‚СЂСѓРґРЅРѕ Р·Р°С‰РёС‰Р°С‚СЊСЃСЏ",
        "РІСЃС‘ РїР»РѕС…Рѕ": "РїРѕР·РёС†РёСЏ СЃРµСЂСЊРµР·РЅРѕ СѓС…СѓРґС€РёР»Р°СЃСЊ",
        "РїСЂРёРїР»С‹Р»": "РїРѕРїР°Р» РІ С‚СЏР¶РµР»СѓСЋ РїРѕР·РёС†РёСЋ",
        "РјРѕРЅРіРѕР»СЊРЅРѕРµ РєР°С‡РµСЃС‚РІРѕ": "РјР°С‚РµСЂРёР°Р»СЊРЅС‹Р№ РїРµСЂРµРІРµСЃ",
        "РјРѕРЅРіРѕР»СЊРЅРѕРµ": "РјР°С‚РµСЂРёР°Р»СЊРЅРѕРµ",
        "СЃСѓС‰РµСЃС‚РІРµРЅРЅРѕРіРѕ РёР·РјРµРЅРµРЅРёСЏ РѕС†РµРЅРєРё РЅРµ РїСЂРѕРёР·РѕС€Р»Рѕ": "РїСЂРµРёРјСѓС‰РµСЃС‚РІРѕ СЃРѕС…СЂР°РЅРёР»РѕСЃСЊ",
        "РЅРёС‡РµРіРѕ РЅРµ РёР·РјРµРЅРёР»РѕСЃСЊ": "РїРѕР·РёС†РёСЏ СЃРѕС…СЂР°РЅРёР»Р° РїСЂРµР¶РЅРёР№ С…Р°СЂР°РєС‚РµСЂ",
        "РІС‹РїСѓСЃС‚РёРІ": "РѕСЃС‚Р°РІРёРІ",
        "РІС‹РїСѓСЃС‚РёР»": "РѕСЃС‚Р°РІРёР»",
        "РІС‹РїСѓСЃС‚РёР»Р°": "РѕСЃС‚Р°РІРёР»Р°",
        "РЅР° РїРµС€РєР°": "РЅР° РїРµС€РєСѓ",
        "Р°С‚Р°РєРѕРІР°Р» РїРµС€РєР°": "Р°С‚Р°РєРѕРІР°Р» РїРµС€РєСѓ",
        "РїРµС€РєРѕР№ С…РѕРґ": "РїРµС€РєРѕР№",
        "РѕСЃРЅРѕРІРЅРѕР№ Р±Р°Р»Р°РЅСЃ РїРѕР·РёС†РёРё СЃРѕС…СЂР°РЅРёР»СЃСЏ": "РїРѕР·РёС†РёСЏ СЃРѕС…СЂР°РЅРёР»Р° РїСЂРµР¶РЅРёР№ С…Р°СЂР°РєС‚РµСЂ",
        "С‡РµСЂРЅС‹Рµ РѕС‚РІРµС‡Р°СЋС‚": "С‡РµСЂРЅС‹Рµ РїСЂРѕРґРѕР»Р¶Р°СЋС‚",
        "Р±РµР»С‹Рµ РѕС‚РІРµС‡Р°СЋС‚": "Р±РµР»С‹Рµ РїСЂРѕРґРѕР»Р¶Р°СЋС‚",
        "РѕС‚РІРµС‚": "РїСЂРѕРґРѕР»Р¶РµРЅРёРµ",
    }
    lower = comment.lower()
    for bad, good in replacements.items():
        if bad in lower:
            comment = comment.replace(bad, good).replace(bad.capitalize(), good.capitalize())
    comment = re.sub(r"РїРѕСЃР»Рµ С…РѕРґР°\s+[a-h][1-8]\s*[вЂ“-]\s*[a-h][1-8]", "РїРѕСЃР»Рµ С…РѕРґР°", comment, flags=re.IGNORECASE)
    comment = re.sub(r"\b[a-h][1-8]\s*[вЂ“-]\s*[a-h][1-8]\b", "С…РѕРґ", comment, flags=re.IGNORECASE)
    comment = re.sub(r"\s+Р°\s+РІС‹\s+", " ", comment, flags=re.IGNORECASE)
    comment = re.sub(r"\bРІС‹\s+", "", comment, flags=re.IGNORECASE)
    comment = re.sub(r"\b(С‡РµСЂРЅС‹Рµ|Р±РµР»С‹Рµ)\s+РѕС‚РІРµС‡Р°СЋС‚\b", r"\1 РїСЂРѕРґРѕР»Р¶Р°СЋС‚", comment, flags=re.IGNORECASE)
    comment = comment.replace("РҐРѕРґ С…РѕРґ", "РҐРѕРґ").replace("С…РѕРґ С…РѕРґ", "С…РѕРґ")
    if move_details and move_details.get("summary"):
        if current_mate and "СЃРѕС…СЂР°РЅСЏ" in comment.lower() and "РјР°С‚РѕРІ" in comment.lower():
            summary = move_details["summary"]
            comment = mate_support_phrase(summary)
        if any(phrase in comment.lower() for phrase in ("СЃРѕС…СЂР°РЅРёР»Рё СЂРµС€Р°СЋС‰РµРµ РїСЂРµРёРјСѓС‰РµСЃС‚РІРѕ", "СЃРѕС…СЂР°РЅРёР»Рё Р±РѕР»СЊС€РѕРµ РїСЂРµРёРјСѓС‰РµСЃС‚РІРѕ", "СЃРѕС…СЂР°РЅРёР»Рё РЅРµР±РѕР»СЊС€РѕРµ РїСЂРµРёРјСѓС‰РµСЃС‚РІРѕ", "РїРѕР·РёС†РёСЏ СЃРѕС…СЂР°РЅРёР»Р° РїСЂРµР¶РЅРёР№ С…Р°СЂР°РєС‚РµСЂ")):
            summary = move_details["summary"]
            if eval_change and eval_change.get("kind") == "worsened" and eval_change.get("major_worsening"):
                comment = f"{summary.capitalize()}, РЅРѕ РїРѕР·РёС†РёСЏ РїРѕСЃР»Рµ СЌС‚РѕРіРѕ СѓС…СѓРґС€РёР»Р°СЃСЊ."
            else:
                comment = f"{summary.capitalize()}."
    if comment:
        comment = comment[0].upper() + comment[1:]
    return comment


def ensure_moved_piece_mentioned(comment, move_details):
    text = str(comment or "").strip()
    if not isinstance(move_details, dict):
        return text
    piece = str(move_details.get("moving_piece_name") or "").strip().lower()
    summary = str(move_details.get("summary") or "").strip()
    if not piece:
        return text
    if piece in text.lower():
        return text
    if summary:
        head = summary[0].upper() + summary[1:] if summary else summary
        if text:
            return f"{head}, {text[0].lower() + text[1:]}"
        return f"{head}."
    return f"{piece.capitalize()} СЃРґРµР»Р°Р» С…РѕРґ."


def build_coach_comment(comment_payload):
    if not OPENAI_API_KEY:
        return "AI-комментарий недоступен: добавь OPENAI_API_KEY в .env."

    current_line = compact_analysis_line(comment_payload.get("current_line"))
    previous_line = compact_analysis_line(comment_payload.get("previous_best_line"))
    user_move = str(comment_payload.get("played_move") or "")
    board_snapshot = compact_board_snapshot(comment_payload.get("board_snapshot"))
    played_move_details = compact_move_details(comment_payload.get("played_move_details"), board_snapshot)
    deviated = bool(comment_payload.get("deviated"))
    fen = str(comment_payload.get("fen") or "")[:120]
    side = str(comment_payload.get("active_color") or "")
    previous_side = str(comment_payload.get("previous_active_color") or "")
    recent_comments = [
        str(item).strip()
        for item in (comment_payload.get("recent_comments") if isinstance(comment_payload.get("recent_comments"), list) else [])
        if str(item).strip()
    ][:4]
    coach_event = comment_payload.get("coach_event") if isinstance(comment_payload.get("coach_event"), dict) else {}
    game_status = comment_payload.get("game_status") if isinstance(comment_payload.get("game_status"), dict) else {}
    if game_status.get("kind") == "checkmate":
        coach_event = {
            **coach_event,
            "kind": "checkmate",
            "current_forced_mate": {
                "moves": 0,
                "mating_side": game_status.get("winner"),
                "defending_side": game_status.get("loser"),
            },
        }
    coach_event.setdefault("current_forced_mate", build_forced_mate_context_from_line(current_line, side))
    coach_event.setdefault("previous_forced_mate", build_forced_mate_context_from_line(previous_line, previous_side))
    current_mate = coach_event.get("current_forced_mate")
    current_eval = build_evaluation_context(current_line, side)
    previous_eval = build_evaluation_context(previous_line, previous_side)
    moving_side = coach_event.get("moving_side") or previous_side
    blunder_context = build_blunder_context(previous_eval, current_eval, moving_side, previous_line.get("move"))
    eval_change_context = build_eval_change_context(previous_eval, current_eval, moving_side)
    if blunder_context and coach_event.get("kind") == "position":
        coach_event["kind"] = "blunder"
        coach_event["blunder"] = blunder_context
    deterministic_comment = build_deterministic_coach_comment(coach_event, played_move_details, board_snapshot)
    if deterministic_comment:
        return deterministic_comment
    mate_defense_comment = build_mate_defense_comment(coach_event, played_move_details)
    if mate_defense_comment:
        return mate_defense_comment
    king_escape_comment = build_king_escape_from_check_comment(played_move_details, eval_change_context)
    if king_escape_comment:
        return king_escape_comment
    check_defense_comment = build_check_defense_comment(played_move_details, eval_change_context)
    if check_defense_comment:
        return check_defense_comment
    tactical_context = build_tactical_context(played_move_details, current_line)
    motif_focus = build_motif_focus_context(tactical_context, played_move_details, current_mate, eval_change_context)
    board_profile = build_board_profile(board_snapshot)
    player_to_advise = coach_event.get("player_to_advise") or ("black" if side == "b" else "white")
    strategy_context = build_strategy_context(
        board_profile,
        current_eval,
        current_mate,
        player_to_advise,
    )
    prompt = {
        "fen": fen,
        "side_to_move": side,
        "player_to_advise": player_to_advise,
        "board_snapshot": board_snapshot,
        "current_best_line": current_line,
        "previous_best_line": previous_line,
        "played_move": user_move,
        "played_move_details": played_move_details,
        "tactical_context": tactical_context,
        "safety_policy": build_safety_policy_context(),
        "motif_focus": motif_focus,
        "best_move_before_last_move": previous_line.get("move") or "",
        "user_deviated_from_first_line": deviated,
        "coach_event": coach_event,
        "strategy_context": strategy_context,
        "recent_comments": recent_comments,
        "evaluation": {
            "current": current_eval,
            "previous": previous_eval,
            "blunder": blunder_context,
            "change_after_last_move": eval_change_context,
            "source": "first_line",
        },
    }
    instructions = load_coach_rules()
    request_body = {
        "model": OPENAI_MODEL,
        "input": f"{instructions}\n\nР”Р°РЅРЅС‹Рµ Р°РЅР°Р»РёР·Р° JSON:\n{json.dumps(prompt, ensure_ascii=False)}",
        "max_output_tokens": 110,
    }
    request = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(request_body, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            data = json.loads(response.read().decode("utf-8"))
        comment = extract_response_text(data)
        comment = polish_coach_comment(shorten_coach_comment(comment), played_move_details, eval_change_context, current_mate)
        summary = str(played_move_details.get("summary") or "").strip().rstrip(".!?")
        if summary and is_generic_position_comment(comment):
            if eval_change_context and eval_change_context.get("kind") == "worsened" and eval_change_context.get("major_worsening"):
                comment = f"{summary.capitalize()}, РЅРѕ С…РѕРґ РѕРєР°Р·Р°Р»СЃСЏ РЅРµС‚РѕС‡РЅС‹Рј."
            else:
                comment = f"{summary.capitalize()}."
        fork_signal = bool(tactical_context.get("is_fork")) or ("РІРёР»Рє" in summary.lower())
        if fork_signal and eval_change_context and eval_change_context.get("kind") == "worsened":
            if "Р·РµРІ" not in comment.lower():
                mover = normalize_score_side(moving_side)
                comment = f"{side_label(mover)} Р·РµРІРЅСѓР»Рё РІРёР»РєСѓ, Рё РїРѕР·РёС†РёСЏ СЂРµР·РєРѕ СѓС…СѓРґС€РёР»Р°СЃСЊ."
        comment = validate_comment_against_eval(comment, current_eval, current_mate, eval_change_context)
        if summary:
            comment_lower = comment.lower()
            piece_name = str(played_move_details.get("moving_piece_name") or "").lower()
            if is_generic_position_comment(comment):
                if eval_change_context and eval_change_context.get("kind") == "worsened" and eval_change_context.get("major_worsening"):
                    comment = f"{summary.capitalize()}, РЅРѕ С…РѕРґ РѕРєР°Р·Р°Р»СЃСЏ РЅРµС‚РѕС‡РЅС‹Рј."
                else:
                    comment = f"{summary.capitalize()}."
            elif piece_name and piece_name not in comment_lower:
                tail = comment[0].lower() + comment[1:] if comment else ""
                comment = f"{summary.capitalize()}, {tail}".strip(" ,")
            if should_mark_best_move(deviated, eval_change_context, user_move, current_line) and "Р»СѓС‡С€РёР№ С…РѕРґ" not in comment.lower():
                comment = f"{summary.capitalize()}, СЌС‚Рѕ Р»СѓС‡С€РёР№ С…РѕРґ РІ РїРѕР·РёС†РёРё."
            single_escape_comment = build_single_king_escape_comment(played_move_details, current_line, current_mate or {})
            if single_escape_comment:
                comment = single_escape_comment
            elif should_add_context_tail(comment, summary):
                tail = build_context_tail(
                    played_move_details,
                    eval_change_context,
                    current_eval,
                    tactical_context,
                    strategy_context,
                    current_line,
                    summary,
                )
                if tail:
                    comment = f"{summary.capitalize()}, {tail}."
        comment = ensure_moved_piece_mentioned(comment, played_move_details)
        comment = normalize_piece_cases(comment)
        comment = avoid_recent_comment_repetition(comment, recent_comments)
        comment = shorten_coach_comment(comment, max_words=22)
        comment = cleanup_mojibake_comment(comment)
        return comment or "Позиция требует точной игры."
    except Exception as err:
        print("OpenAI coach comment error:", err)
        return "AI-комментарий временно недоступен, но первая линия всё ещё лучший ориентир."


class AnalyticsHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, ngrok-skip-browser-warning")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/stockfish/health":
            proxy_stockfish_request(self, "GET", "/health")
            return
        if parsed.path == "/health":
            json_response(self, 200, {"ok": True})
            return
        static_path = resolve_static_path(self.path)
        if static_path:
            self.serve_static_file(static_path)
            return
        json_response(self, 404, {"ok": False, "error": "Not found"})

    def do_POST(self):
        try:
            if self.path == "/analyze":
                payload = self.read_json()
                proxy_stockfish_request(self, "POST", "/analyze", payload)
                return
            if self.path == "/evaluate_move":
                payload = self.read_json()
                proxy_stockfish_request(self, "POST", "/evaluate_move", payload)
                return

            payload = self.read_json()
            if self.path == "/api/coach/comment":
                self.handle_coach_comment(payload)
                return

            init_data, user = validate_init_data(payload.get("initData", ""))
            telegram_id = int(user["id"])

            if self.path == "/api/app/open":
                self.handle_app_open(payload, user, telegram_id)
            elif self.path == "/api/session/end":
                self.handle_session_end(payload, user, telegram_id)
            elif self.path == "/api/events":
                self.handle_event(payload, user, telegram_id)
            elif self.path == "/api/state/load":
                self.handle_state_load(payload, user, telegram_id)
            elif self.path == "/api/state/save":
                self.handle_state_save(payload, user, telegram_id)
            else:
                json_response(self, 404, {"ok": False, "error": "Not found"})
        except ValueError as err:
            print(f"Bad request {self.path}: {err}")
            json_response(self, 400, {"ok": False, "error": str(err)})
        except Exception as err:
            print("Backend error:", err)
            json_response(self, 500, {"ok": False, "error": "Internal server error"})

    def read_json(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        return json.loads(raw or "{}")

    def serve_static_file(self, path):
        content_type = mimetypes.guess_type(path)[0] or "application/octet-stream"
        if path.endswith(".wasm"):
            content_type = "application/wasm"
        with open(path, "rb") as file:
            body = file.read()
        safe_http_write(
            self,
            200,
            body,
            content_type=content_type,
            extra_headers={
                "Cross-Origin-Opener-Policy": "same-origin",
                "Cross-Origin-Embedder-Policy": "require-corp",
            },
        )

    def handle_app_open(self, payload, user, telegram_id):
        platform = normalize_platform(payload.get("platform"))
        user_agent = payload.get("userAgent") or self.headers.get("User-Agent")
        with get_db() as conn:
            save_user(conn, user)
            record_app_open(conn, telegram_id, platform, user_agent)
            preview = _load_sent_weekly_preview()
            seeded_state = _build_global_weekly_state(preview) if preview else None
            if seeded_state:
                current_state = get_user_state(conn, telegram_id)
                if _should_apply_weekly_seed(current_state, seeded_state):
                    try:
                        save_user_state(conn, telegram_id, seeded_state, force=True)
                    except Exception as err:
                        print(f"Weekly seed state failed for telegram_id={telegram_id}: {err}")
            session_id = start_session(conn, telegram_id)
            stats = get_user_stats(conn, telegram_id)
            app_state = get_user_state(conn, telegram_id)
        print(f"Recorded Mini App open: telegram_id={telegram_id}, platform={platform}")
        json_response(
            self,
            200,
            {
                "ok": True,
                "session_id": session_id,
                "app_state": app_state,
                **stats,
            },
        )

    def handle_session_end(self, payload, user, telegram_id):
        session_id = int(payload.get("sessionId", "0") or "0")
        if session_id <= 0:
            raise ValueError("sessionId is required")
        with get_db() as conn:
            save_user(conn, user)
            end_session(conn, telegram_id, session_id)
        json_response(self, 200, {"ok": True})

    def handle_event(self, payload, user, telegram_id):
        event_name = payload.get("eventName")
        if not event_name:
            raise ValueError("eventName is required")
        event_data = payload.get("eventData") or {}
        with get_db() as conn:
            save_user(conn, user)
            record_event(conn, telegram_id, event_name, event_data)
        print(f"Recorded event: telegram_id={telegram_id}, event_name={event_name}")
        json_response(self, 200, {"ok": True})

    def handle_state_load(self, payload, user, telegram_id):
        with get_db() as conn:
            save_user(conn, user)
            app_state = get_user_state(conn, telegram_id)
            preview = _load_sent_weekly_preview()
            seeded_state = _build_global_weekly_state(preview) if preview else None
            if seeded_state and _should_apply_weekly_seed(app_state, seeded_state):
                try:
                    save_user_state(conn, telegram_id, seeded_state, force=True)
                    app_state = get_user_state(conn, telegram_id)
                except Exception as err:
                    print(f"Weekly seed state failed for telegram_id={telegram_id}: {err}")
        json_response(self, 200, {"ok": True, "app_state": app_state})

    def handle_state_save(self, payload, user, telegram_id):
        state = payload.get("state")
        if not isinstance(state, dict):
            raise ValueError("state object is required")
        with get_db() as conn:
            save_user(conn, user)
            saved = save_user_state(conn, telegram_id, state)
        json_response(self, 200, {"ok": True, "saved": saved})

    def handle_coach_comment(self, payload):
        comment_payload = payload.get("commentPayload") or {}
        if not isinstance(comment_payload, dict):
            raise ValueError("commentPayload object is required")
        init_data = payload.get("initData", "")
        telegram_id = None
        ai_user_key = normalize_ai_user_key(
            payload.get("userId")
            or payload.get("user_id")
            or comment_payload.get("user_key")
            or "anonymous"
        )
        if init_data:
            _, user = validate_init_data(init_data)
            telegram_id = int(user["id"])
            ai_user_key = str(telegram_id)
            with get_db() as conn:
                save_user(conn, user)
                increment_ai_request_counters(conn, ai_user_key, telegram_id=telegram_id)
        else:
            with get_db() as conn:
                increment_ai_request_counters(conn, ai_user_key, telegram_id=None)
        comment = build_coach_comment(comment_payload)
        json_response(self, 200, {"ok": True, "comment": comment})

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} - {fmt % args}")


def telegram_api(method, payload=None, timeout=TELEGRAM_REQUEST_TIMEOUT):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    data = json.dumps(payload or {}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def send_welcome(chat_id):
    telegram_api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": "♟ Задача дня\nКаждый день — новая шахматная задача в 1 клик.\n\n🔍 Анализ позиции\n🎯 Подсказки на доске\n🧠 AI-комментарий в анализе\n⚙️ Настройки\n\nСделано специально для шахматного сообщества на Красной Поляне:\nhttps://t.me/chesspolyana\n\n⚡️ Готов начать?",
            "reply_markup": {
                "inline_keyboard": [
                    [
                        {
                            "text": "Открыть Mini App",
                            "url": MINI_APP_URL,
                        }
                    ]
                ]
            },
        },
    )


def send_text(chat_id, text):
    telegram_api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
        },
    )


def get_bot_command(text):
    if not text:
        return ""
    first = text.strip().split(maxsplit=1)[0].lower()
    return first.split("@", 1)[0]


def handle_cheat_command(message):
    user = user_from_message(message)
    if not user:
        return False
    telegram_id = int(user["id"])
    with get_db() as conn:
        save_user(conn, user)
        reset_daily_quota(conn, telegram_id)
        record_event(
            conn,
            telegram_id,
            "bot_cheat",
            {"text": message.get("text", ""), "chat_id": message.get("chat", {}).get("id")},
        )
    print(f"Daily quota reset by /cheat: telegram_id={telegram_id}")
    return True


def run_bot_polling():
    if not BOT_TOKEN:
        print("Set TELEGRAM_BOT_TOKEN to start the Telegram bot.")
        return

    offset = None
    print("Telegram bot polling started.")
    while True:
        try:
            payload = {"timeout": TELEGRAM_POLL_TIMEOUT}
            if offset is not None:
                payload["offset"] = offset
            result = telegram_api("getUpdates", payload, timeout=TELEGRAM_REQUEST_TIMEOUT)
            for update in result.get("result", []):
                offset = update["update_id"] + 1
                message = update.get("message") or {}
                chat = message.get("chat") or {}
                text = message.get("text", "")
                command = get_bot_command(text)
                if chat.get("id") and command == "/cheat":
                    handle_cheat_command(message)
                    send_text(chat["id"], "Р“РѕС‚РѕРІРѕ. Р”РЅРµРІРЅРѕР№ СЃС‡РµС‚С‡РёРє Р·Р°РґР°С‡ СЃР±СЂРѕС€РµРЅ, РјРѕР¶РЅРѕ СЂРµС€Р°С‚СЊ РµС‰Рµ.")
                    continue
                if chat.get("id") and (command == "/start" or text):
                    record_bot_message(message)
                    send_welcome(chat["id"])
        except KeyboardInterrupt:
            print("\nStopping Telegram bot polling.")
            break
        except (urllib.error.URLError, TimeoutError, socket.timeout) as err:
            print("Telegram polling network error:", err)
            time.sleep(3)
        except Exception as err:
            print("Telegram polling error:", err)
            time.sleep(3)


def run_http_server():
    print(f"[backend] pid={os.getpid()} file={__file__} host={HOST} port={PORT}")
    server = ThreadingHTTPServer((HOST, PORT), AnalyticsHandler)
    print(f"Analytics backend listening on http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    try:
        init_db()
        threading.Thread(target=run_http_server, daemon=True).start()
        run_bot_polling()
    except KeyboardInterrupt:
        print("\nBackend stopped.")

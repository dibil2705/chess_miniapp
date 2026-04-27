import hashlib
import hmac
import json
import os
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
MINI_APP_URL = "https://t.me/chess_every_day_bot/app?startapp=test&mode=fullscreen"
DATABASE_PATH = os.environ.get("ANALYTICS_DB", "analytics.sqlite3")
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))
INITDATA_MAX_AGE_SECONDS = int(os.environ.get("INITDATA_MAX_AGE_SECONDS", "86400"))
TELEGRAM_POLL_TIMEOUT = int(os.environ.get("TELEGRAM_POLL_TIMEOUT", "25"))
TELEGRAM_REQUEST_TIMEOUT = TELEGRAM_POLL_TIMEOUT + 15


def utc_now():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


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
                last_seen_at TEXT NOT NULL
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
                updated_at TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
            );
            """
        )


def save_user(conn, user):
    now = utc_now()
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
        (telegram_id, utc_now(), platform, user_agent),
    )


def start_session(conn, telegram_id):
    cursor = conn.execute(
        """
        INSERT INTO sessions (telegram_id, started_at)
        VALUES (?, ?)
        """,
        (telegram_id, utc_now()),
    )
    return cursor.lastrowid


def end_session(conn, telegram_id, session_id):
    ended_at = utc_now()
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
            utc_now(),
        ),
    )


def get_user_state(conn, telegram_id):
    row = conn.execute(
        "SELECT state_json, updated_at FROM user_state WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    if not row:
        return None
    try:
        state = json.loads(row["state_json"])
    except json.JSONDecodeError:
        return None
    return {
        "state": state,
        "updated_at": row["updated_at"],
    }


def save_user_state(conn, telegram_id, state):
    conn.execute(
        """
        INSERT INTO user_state (telegram_id, state_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            state_json = excluded.state_json,
            updated_at = excluded.updated_at
        """,
        (telegram_id, json.dumps(state or {}, ensure_ascii=False), utc_now()),
    )


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
        SELECT telegram_id, first_seen_at, last_seen_at
        FROM users
        WHERE telegram_id = ?
        """,
        (telegram_id,),
    ).fetchone()
    opens_count = conn.execute(
        "SELECT COUNT(*) AS count FROM app_opens WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()["count"]
    return {
        "telegram_id": telegram_id,
        "opens_count": opens_count,
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
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type")
    handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    handler.end_headers()
    handler.wfile.write(body)


class AnalyticsHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            json_response(self, 200, {"ok": True})
            return
        json_response(self, 404, {"ok": False, "error": "Not found"})

    def do_POST(self):
        try:
            payload = self.read_json()
            init_data, user = validate_init_data(payload.get("initData", ""))
            telegram_id = int(user["id"])

            if self.path == "/api/app/open":
                self.handle_app_open(payload, user, telegram_id)
            elif self.path == "/api/session/end":
                self.handle_session_end(payload, user, telegram_id)
            elif self.path == "/api/events":
                self.handle_event(payload, user, telegram_id)
            elif self.path == "/api/state/save":
                self.handle_state_save(payload, user, telegram_id)
            else:
                json_response(self, 404, {"ok": False, "error": "Not found"})
        except ValueError as err:
            json_response(self, 400, {"ok": False, "error": str(err)})
        except Exception as err:
            print("Backend error:", err)
            json_response(self, 500, {"ok": False, "error": "Internal server error"})

    def read_json(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        return json.loads(raw or "{}")

    def handle_app_open(self, payload, user, telegram_id):
        platform = normalize_platform(payload.get("platform"))
        user_agent = payload.get("userAgent") or self.headers.get("User-Agent")
        with get_db() as conn:
            save_user(conn, user)
            record_app_open(conn, telegram_id, platform, user_agent)
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

    def handle_state_save(self, payload, user, telegram_id):
        state = payload.get("state")
        if not isinstance(state, dict):
            raise ValueError("state object is required")
        with get_db() as conn:
            save_user(conn, user)
            save_user_state(conn, telegram_id, state)
        json_response(self, 200, {"ok": True})

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
            "text": "Привет 👋\n\n♟ Задача дня\n1 шахматная задача каждый день — в 1 клик.\n\n⚡ Готов начать?",
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
                if chat.get("id") and (text.startswith("/start") or text):
                    record_bot_message(message)
                    send_welcome(chat["id"])
        except (urllib.error.URLError, TimeoutError, socket.timeout) as err:
            print("Telegram polling network error:", err)
            time.sleep(3)
        except Exception as err:
            print("Telegram polling error:", err)
            time.sleep(3)


def run_http_server():
    server = ThreadingHTTPServer((HOST, PORT), AnalyticsHandler)
    print(f"Analytics backend listening on http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    init_db()
    threading.Thread(target=run_http_server, daemon=True).start()
    run_bot_polling()

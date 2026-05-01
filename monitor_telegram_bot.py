import json
import http.client
import hashlib
import mimetypes
import os
import random
import re
import shutil
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from html import escape
from threading import RLock
from uuid import uuid4


def load_env_file(path=".env"):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


load_env_file()

MONITOR_BOT_TOKEN = os.environ.get("TELEGRAM_MONITOR_BOT_TOKEN", "").strip()
MONITOR_CHAT_ID = int(os.environ.get("TELEGRAM_MONITOR_CHAT_ID", "0") or "0")
MONITOR_STATE_PATH = os.environ.get("MONITOR_STATE_PATH", "monitor_state.json")
MONITOR_DB_PATH = os.environ.get("ANALYTICS_DB", "analytics.sqlite3")
MONITOR_HEALTH_URL = os.environ.get("MONITOR_HEALTH_URL", "http://127.0.0.1:12315/health")
MONITOR_INTERVAL_SECONDS = max(10, int(os.environ.get("MONITOR_INTERVAL_SECONDS", "30") or "30"))
MONITOR_ALERT_COOLDOWN_SECONDS = max(30, int(os.environ.get("MONITOR_ALERT_COOLDOWN_SECONDS", "180") or "180"))
MONITOR_MIN_FREE_GB = float(os.environ.get("MONITOR_MIN_FREE_GB", "1.0") or "1.0")
MONITOR_MAX_LOG_SCAN_BYTES = max(32768, int(os.environ.get("MONITOR_MAX_LOG_SCAN_BYTES", "262144") or "262144"))
MONITOR_LOG_FILES = [x.strip() for x in os.environ.get("MONITOR_LOG_FILES", "backend_8000.err.log,backend_8000.log,backend_direct.err.log,backend_direct.out.log,backend_main_combined.log").split(",") if x.strip()]
MAIN_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
MAIN_BOT_API_BASE = f"https://api.telegram.org/bot{MAIN_BOT_TOKEN}" if MAIN_BOT_TOKEN else ""
MAIN_BOT_REQUEST_TIMEOUT = max(8, int(os.environ.get("MAIN_BOT_REQUEST_TIMEOUT", "20") or "20"))
MINI_APP_URL = os.environ.get(
    "MINI_APP_URL",
    "https://t.me/chess_every_day_bot/app?startapp=test&mode=fullscreen",
).strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5.4-nano").strip()
WEEKLY_BROADCAST_ENABLED = os.environ.get("WEEKLY_BROADCAST_ENABLED", "1").strip() == "1"
WEEKLY_BROADCAST_DAYS = (2, 3)  # Wednesday or Thursday
WEEKLY_BROADCAST_START_HOUR = 10
WEEKLY_BROADCAST_END_HOUR = 12
WEEKLY_BROADCAST_SET_SIZE = 3
WEEKLY_BROADCAST_RANDOM_URL = "https://api.chess.com/pub/puzzle/random"
WEEKLY_FETCH_TIMEOUT_SECONDS = max(8, int(os.environ.get("WEEKLY_FETCH_TIMEOUT_SECONDS", "20") or "20"))
WEEKLY_FETCH_RETRIES = max(0, int(os.environ.get("WEEKLY_FETCH_RETRIES", "2") or "2"))
WEEKLY_RESET_UTC_OFFSET_MS = 4 * 60 * 60 * 1000
STOCKFISH_TEST_FEN = os.environ.get(
    "MONITOR_STOCKFISH_TEST_FEN",
    "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
).strip()

ERROR_PATTERNS = [
    re.compile(r"\btraceback\b", re.IGNORECASE),
    re.compile(r"\berror\b", re.IGNORECASE),
    re.compile(r"\bexception\b", re.IGNORECASE),
    re.compile(r"\bcritical\b", re.IGNORECASE),
    re.compile(r"\bbackend error\b", re.IGNORECASE),
]


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def fmt_seconds(seconds):
    if seconds is None:
        return "нет данных"
    value = max(0, int(seconds))
    mins, sec = divmod(value, 60)
    hrs, mins = divmod(mins, 60)
    return f"{hrs:02d}:{mins:02d}:{sec:02d}"


class MonitorBot:
    def __init__(self):
        self.lock = RLock()
        self.state = self._load_state()
        self.api_base = f"https://api.telegram.org/bot{MONITOR_BOT_TOKEN}"

    def _load_state(self):
        default = {
            "last_update_id": 0,
            "monitoring_enabled": True,
            "log_offsets": {},
            "last_alert_at": {},
            "last_analysis_error_event_id": 0,
            "ui_message_id_by_chat": {},
            "chat_flow": {},
            "connections": {},
            "weekly_broadcast": {},
        }
        if not os.path.exists(MONITOR_STATE_PATH):
            return default
        try:
            with open(MONITOR_STATE_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            default.update({
                "last_update_id": int(raw.get("last_update_id", 0) or 0),
                "monitoring_enabled": bool(raw.get("monitoring_enabled", True)),
                "log_offsets": dict(raw.get("log_offsets") or {}),
                "last_alert_at": dict(raw.get("last_alert_at") or {}),
                "last_analysis_error_event_id": int(raw.get("last_analysis_error_event_id", 0) or 0),
                "ui_message_id_by_chat": dict(raw.get("ui_message_id_by_chat") or {}),
                "chat_flow": dict(raw.get("chat_flow") or {}),
                "connections": dict(raw.get("connections") or {}),
                "weekly_broadcast": dict(raw.get("weekly_broadcast") or {}),
            })
        except Exception:
            pass
        return default

    def _save_state(self):
        with self.lock:
            with open(MONITOR_STATE_PATH, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)

    def _telegram_api(self, method, payload=None, timeout=20):
        req = urllib.request.Request(
            f"{self.api_base}/{method}",
            data=json.dumps(payload or {}, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def send_message(self, chat_id, text, reply_markup=None, parse_mode=None):
        payload = {"chat_id": int(chat_id), "text": str(text or ""), "disable_web_page_preview": True}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        if parse_mode:
            payload["parse_mode"] = parse_mode
        return self._telegram_api("sendMessage", payload)

    def edit_message(self, chat_id, message_id, text, reply_markup=None, parse_mode=None):
        payload = {"chat_id": int(chat_id), "message_id": int(message_id), "text": str(text or ""), "disable_web_page_preview": True}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        if parse_mode:
            payload["parse_mode"] = parse_mode
        return self._telegram_api("editMessageText", payload)

    def answer_callback(self, callback_query_id, text="Готово"):
        return self._telegram_api("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text, "show_alert": False})

    def send_document_bytes(self, chat_id, filename, content_bytes, caption=""):
        boundary = f"----WebKitFormBoundary{uuid4().hex}"
        file_mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        body = bytearray()

        def add_field(name, value):
            body.extend(f"--{boundary}\r\n".encode("utf-8"))
            body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"))
            body.extend(str(value).encode("utf-8"))
            body.extend(b"\r\n")

        add_field("chat_id", int(chat_id))
        if caption:
            add_field("caption", caption)
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(f'Content-Disposition: form-data; name="document"; filename="{filename}"\r\n'.encode("utf-8"))
        body.extend(f"Content-Type: {file_mime}\r\n\r\n".encode("utf-8"))
        body.extend(content_bytes)
        body.extend(b"\r\n")
        body.extend(f"--{boundary}--\r\n".encode("utf-8"))

        req = urllib.request.Request(
            f"{self.api_base}/sendDocument",
            data=bytes(body),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=40) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _chat_key(self, chat_id):
        return str(int(chat_id))

    def get_ui_message_id(self, chat_id):
        return int((self.state.get("ui_message_id_by_chat") or {}).get(self._chat_key(chat_id), 0) or 0)

    def save_ui_message_id(self, chat_id, message_id):
        self.state.setdefault("ui_message_id_by_chat", {})[self._chat_key(chat_id)] = int(message_id)
        self._save_state()

    def get_flow(self, chat_id):
        return str((self.state.get("chat_flow") or {}).get(self._chat_key(chat_id), ""))

    def set_flow(self, chat_id, flow):
        self.state.setdefault("chat_flow", {})[self._chat_key(chat_id)] = str(flow or "")
        self._save_state()

    def clear_flow(self, chat_id):
        self.state.setdefault("chat_flow", {}).pop(self._chat_key(chat_id), None)
        self._save_state()

    def keyboard_main(self):
        monitor_label = "🛑 Выключить мониторинг" if self.state.get("monitoring_enabled", True) else "✅ Включить мониторинг"
        return {
            "inline_keyboard": [
                [{"text": "📊 Статус", "callback_data": "screen:status"}, {"text": "⚡ Проверить сейчас", "callback_data": "action:check_now"}],
                [{"text": "♟ Проверка Stockfish", "callback_data": "action:stockfish_check"}],
                [{"text": "📣 Создать рассылку", "callback_data": "action:weekly_prepare"}],
                [{"text": "🧾 Отчеты БД", "callback_data": "screen:reports"}, {"text": "🌐 Вся база (HTML)", "callback_data": "action:db_full"}],
                [{"text": monitor_label, "callback_data": "action:toggle_monitor"}],
            ]
        }

    def keyboard_back(self):
        return {"inline_keyboard": [[{"text": "⬅️ Назад", "callback_data": "screen:main"}]]}

    def render_screen(self, chat_id, screen="main", edit_message_id=0):
        if screen == "status":
            monitoring = "включен ✅" if self.state.get("monitoring_enabled", True) else "выключен ⛔"
            db_exists = "да" if os.path.exists(MONITOR_DB_PATH) else "нет"
            text = (
                "📊 <b>Состояние мониторинга</b>\n\n"
                f"• Мониторинг: {monitoring}\n"
                f"• Health URL: <code>{MONITOR_HEALTH_URL}</code>\n"
                f"• База данных найдена: {db_exists}\n"
                f"• Интервал проверки: {MONITOR_INTERVAL_SECONDS} сек\n"
                f"• Кулдаун алертов: {MONITOR_ALERT_COOLDOWN_SECONDS} сек\n"
                f"• Время: {now_str()}"
            )
            keyboard = self.keyboard_back()
        elif screen == "connect":
            conn = (self.state.get("connections") or {}).get(self._chat_key(chat_id))
            status = "Данные подключения не сохранены."
            if isinstance(conn, dict) and conn:
                status = "Данные подключения сохранены ✅"
            text = (
                "🔌 <b>Подключение серверов</b>\n\n"
                "Нажмите кнопку ниже и отправьте одним сообщением JSON:\n"
                "<code>{\"health_url\":\"https://.../health\",\"api_base\":\"https://...\",\"api_key\":\"...\"}</code>\n\n"
                f"{status}"
            )
            keyboard = {"inline_keyboard": [[{"text": "📝 Ввести данные", "callback_data": "action:connect_input"}], [{"text": "⬅️ Назад", "callback_data": "screen:main"}]]}
        elif screen == "reports":
            text = (
                "🧾 <b>Отчеты по базе данных</b>\n\n"
                "Выберите период для краткого отчета или выгрузите полную HTML-базу."
            )
            keyboard = {
                "inline_keyboard": [
                    [{"text": "📌 Краткий: сегодня/неделя", "callback_data": "action:db_usage_summary"}],
                    [{"text": "📅 За неделю", "callback_data": "action:db_short:week"}, {"text": "🗓 За месяц", "callback_data": "action:db_short:month"}],
                    [{"text": "🕰 За всё время", "callback_data": "action:db_short:all"}],
                    [{"text": "🌐 Вся база (HTML)", "callback_data": "action:db_full"}],
                    [{"text": "⬅️ Назад", "callback_data": "screen:main"}],
                ]
            }
        else:
            text = "🛡️ <b>Мониторинг проекта</b>\n\nВыберите действие ниже. Экран обновляется в этом же сообщении."
            keyboard = self.keyboard_main()

        if edit_message_id:
            try:
                self.edit_message(chat_id, edit_message_id, text, reply_markup=keyboard, parse_mode="HTML")
                self.save_ui_message_id(chat_id, edit_message_id)
                return
            except Exception:
                pass
        sent = self.send_message(chat_id, text, reply_markup=keyboard, parse_mode="HTML")
        msg_id = (((sent or {}).get("result") or {}).get("message_id")) if isinstance(sent, dict) else None
        if msg_id:
            self.save_ui_message_id(chat_id, msg_id)

    def _db_conn(self):
        conn = sqlite3.connect(MONITOR_DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    def _safe_scalar(self, conn, q, params=(), fallback=0):
        try:
            row = conn.execute(q, params).fetchone()
            return row[0] if row and row[0] is not None else fallback
        except Exception:
            return fallback

    def _format_user_name(self, row):
        username = str(row["username"] or "").strip() if "username" in row.keys() else ""
        first_name = str(row["first_name"] or "").strip() if "first_name" in row.keys() else ""
        last_name = str(row["last_name"] or "").strip() if "last_name" in row.keys() else ""
        if username:
            return f"@{username}"
        if first_name or last_name:
            return " ".join(x for x in (first_name, last_name) if x)
        return f"ID {row['telegram_id']}" if "telegram_id" in row.keys() else "Пользователь"

    def _period_filter_sql(self, period):
        p = str(period or "all").lower()
        if p == "week":
            return "datetime('now','-7 day')", "за неделю"
        if p == "month":
            return "datetime('now','-30 day')", "за месяц"
        return None, "за всё время"

    def build_db_report(self, period="all"):
        if not os.path.exists(MONITOR_DB_PATH):
            return "База данных не найдена."
        since_expr, period_label = self._period_filter_sql(period)
        with self._db_conn() as conn:
            users_total = int(self._safe_scalar(conn, "SELECT COUNT(*) FROM users", fallback=0) or 0)
            if since_expr:
                app_opens_total = int(self._safe_scalar(conn, f"SELECT COUNT(*) FROM app_opens WHERE opened_at >= {since_expr}", fallback=0) or 0)
                sessions_total = int(self._safe_scalar(conn, f"SELECT COUNT(*) FROM sessions WHERE started_at >= {since_expr}", fallback=0) or 0)
                events_total = int(self._safe_scalar(conn, f"SELECT COUNT(*) FROM events WHERE created_at >= {since_expr}", fallback=0) or 0)
            else:
                app_opens_total = int(self._safe_scalar(conn, "SELECT COUNT(*) FROM app_opens", fallback=0) or 0)
                sessions_total = int(self._safe_scalar(conn, "SELECT COUNT(*) FROM sessions", fallback=0) or 0)
                events_total = int(self._safe_scalar(conn, "SELECT COUNT(*) FROM events", fallback=0) or 0)
            solved_total = int(self._safe_scalar(conn, "SELECT COALESCE(SUM(solved_tasks_count),0) FROM users", fallback=0) or 0)
            avg_session_seconds = self._safe_scalar(conn, "SELECT AVG(duration_seconds) FROM sessions WHERE duration_seconds IS NOT NULL", fallback=None)
            ai_total = int(self._safe_scalar(conn, "SELECT requests_count FROM ai_request_totals WHERE id=1", fallback=0) or 0)
            ai_users_total = int(self._safe_scalar(conn, "SELECT COUNT(*) FROM ai_request_users", fallback=0) or 0)
            top_rows = conn.execute("SELECT telegram_id, username, first_name, last_name, solved_tasks_count FROM users ORDER BY solved_tasks_count DESC, telegram_id ASC LIMIT 5").fetchall()

        lines = [
            f"Отчёт по базе данных ({period_label}) — {now_str()}",
            "",
            f"- Пользователей: {users_total}",
            f"- Открытий приложения: {app_opens_total}",
            f"- Сессий: {sessions_total}",
            f"- Событий: {events_total}",
            f"- Решённых задач: {solved_total}",
            f"- Средняя длительность сессии: {fmt_seconds(avg_session_seconds) if avg_session_seconds else 'нет данных'}",
            f"- AI-запросов всего: {ai_total}",
            f"- Пользователей с AI-запросами: {ai_users_total}",
            "",
            "Топ-5 по решённым задачам:",
        ]
        if top_rows:
            for i, r in enumerate(top_rows, 1):
                lines.append(f"{i}. {self._format_user_name(r)} — {int(r['solved_tasks_count'] or 0)}")
        else:
            lines.append("нет данных")
        return "\n".join(lines)

    def build_db_usage_summary_report(self, users_limit=12):
        if not os.path.exists(MONITOR_DB_PATH):
            return "База данных не найдена."
        limit = max(5, int(users_limit))
        with self._db_conn() as conn:
            opens_today = int(self._safe_scalar(conn, "SELECT COUNT(*) FROM app_opens WHERE datetime(opened_at) >= datetime('now','start of day')", fallback=0) or 0)
            opens_week = int(self._safe_scalar(conn, "SELECT COUNT(*) FROM app_opens WHERE datetime(opened_at) >= datetime('now','-7 day')", fallback=0) or 0)

            users_today = conn.execute(
                """
                SELECT u.telegram_id, u.username, u.first_name, u.last_name, COUNT(*) AS opens_count
                FROM app_opens ao
                JOIN users u ON u.telegram_id = ao.telegram_id
                WHERE datetime(ao.opened_at) >= datetime('now','start of day')
                GROUP BY u.telegram_id, u.username, u.first_name, u.last_name
                ORDER BY opens_count DESC, u.telegram_id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            users_week = conn.execute(
                """
                SELECT u.telegram_id, u.username, u.first_name, u.last_name, COUNT(*) AS opens_count
                FROM app_opens ao
                JOIN users u ON u.telegram_id = ao.telegram_id
                WHERE datetime(ao.opened_at) >= datetime('now','-7 day')
                GROUP BY u.telegram_id, u.username, u.first_name, u.last_name
                ORDER BY opens_count DESC, u.telegram_id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            users_today_total = int(self._safe_scalar(conn, "SELECT COUNT(DISTINCT telegram_id) FROM app_opens WHERE datetime(opened_at) >= datetime('now','start of day')", fallback=0) or 0)
            users_week_total = int(self._safe_scalar(conn, "SELECT COUNT(DISTINCT telegram_id) FROM app_opens WHERE datetime(opened_at) >= datetime('now','-7 day')", fallback=0) or 0)

        lines = [
            f"📌 Краткий отчет по БД — {now_str()}",
            "",
            "Сегодня:",
            f"- Открытий приложения: {opens_today}",
            f"- Активных пользователей: {users_today_total}",
        ]
        if users_today:
            lines.append(f"- Топ пользователей (до {limit}):")
            for i, row in enumerate(users_today, 1):
                lines.append(f"  {i}. {self._format_user_name(row)} — {int(row['opens_count'] or 0)}")
            if users_today_total > len(users_today):
                lines.append(f"  … и ещё {users_today_total - len(users_today)}")
        else:
            lines.append("- Пользователей сегодня пока нет")

        lines.extend([
            "",
            "За 7 дней:",
            f"- Открытий приложения: {opens_week}",
            f"- Активных пользователей: {users_week_total}",
        ])
        if users_week:
            lines.append(f"- Топ пользователей (до {limit}):")
            for i, row in enumerate(users_week, 1):
                lines.append(f"  {i}. {self._format_user_name(row)} — {int(row['opens_count'] or 0)}")
            if users_week_total > len(users_week):
                lines.append(f"  … и ещё {users_week_total - len(users_week)}")
        else:
            lines.append("- За неделю пользователей нет")

        return "\n".join(lines)

    def _translated_headers(self, table_name, headers):
        users_map = {
            "telegram_id": "Telegram ID",
            "username": "Имя пользователя",
            "first_name": "Имя",
            "last_name": "Фамилия",
            "first_seen_at": "Первый вход",
            "last_seen_at": "Последний вход",
            "solved_tasks_count": "Решено задач",
            "app_opens_count": "Открытий приложения",
            "total_time_seconds": "Общее время (сек)",
            "average_session_seconds": "Средняя сессия (сек)",
        }
        if table_name == "users":
            return [users_map.get(h, h) for h in headers]
        return list(headers)

    def _format_cell(self, table_name, col_name, value):
        if value is None:
            return ""
        text = str(value)
        if table_name == "user_state" and col_name == "json":
            try:
                obj = json.loads(text)
                text = json.dumps(obj, ensure_ascii=False, indent=2)
            except Exception:
                pass
            return f"<pre>{escape(text)}</pre>"
        return escape(text)

    def build_db_full_html_report(self):
        if not os.path.exists(MONITOR_DB_PATH):
            return "<html><body><h1>База данных не найдена</h1></body></html>"
        parts = [
            "<!doctype html><html lang='ru'><head><meta charset='utf-8'>",
            "<meta name='viewport' content='width=device-width,initial-scale=1'>",
            "<title>Полный отчёт БД</title>",
            "<style>"
            "body{font-family:Segoe UI,Arial,sans-serif;margin:18px;background:#CCD0CF;color:#06141B}"
            "h1,h2{margin:.4em 0}"
            ".meta{margin:10px 0 18px;padding:12px;background:#9BA8AB;border:1px solid #4A5C6A;border-radius:10px}"
            "table{border-collapse:separate;border-spacing:0;width:100%;margin:10px 0 22px;background:#11212D;border:1px solid #253745;border-radius:10px;overflow:hidden}"
            "th,td{border-bottom:1px solid #253745;padding:7px 9px;text-align:left;vertical-align:top;font-size:13px;color:#CCD0CF}"
            "th{background:#253745;color:#CCD0CF}"
            "tr:last-child td{border-bottom:none}"
            "code{background:#253745;color:#CCD0CF;padding:1px 4px;border-radius:4px}"
            "pre{margin:0;white-space:pre-wrap;word-break:break-word;font-size:12px}"
            "details{margin:12px 0}"
            "summary{cursor:pointer;font-weight:600;color:#06141B}"
            "</style>",
            "</head><body>",
            f"<h1>Полный отчёт базы данных</h1><div class='meta'><b>Сформирован:</b> {escape(now_str())}<br><b>Путь:</b> <code>{escape(os.path.abspath(MONITOR_DB_PATH))}</code></div>",
        ]
        with self._db_conn() as conn:
            tables = [r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
            ordered = []
            if "users" in tables:
                ordered.append("users")
            ordered.extend([t for t in tables if t not in ("users", "events")])
            if "events" in tables:
                ordered.append("events")

            for name in ordered:
                rows = conn.execute(f"SELECT * FROM {name}").fetchall()
                is_events = name == "events"
                if is_events:
                    parts.append("<details><summary>Таблица: events (развернуть)</summary>")
                parts.append(f"<h2>Таблица: {escape(name)}</h2>")
                if not rows:
                    parts.append("<p>Нет строк.</p>")
                    if is_events:
                        parts.append("</details>")
                    continue
                headers = list(rows[0].keys())
                shown_headers = self._translated_headers(name, headers)
                parts.append("<table><tr>" + "".join(f"<th>{escape(h)}</th>" for h in shown_headers) + "</tr>")
                for row in rows:
                    vals = [f"<td>{self._format_cell(name, h, row[h])}</td>" for h in headers]
                    parts.append("<tr>" + "".join(vals) + "</tr>")
                parts.append("</table>")
                if is_events:
                    parts.append("</details>")
        parts.append("</body></html>")
        return "".join(parts)

    def send_db_full_report_html(self, chat_id):
        filename = f"otchet_bazy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        html = self.build_db_full_html_report().encode("utf-8")
        self.send_document_bytes(chat_id, filename, html, caption="🌐 Полная выгрузка базы данных (HTML)")

    def _current_week_key(self):
        now = datetime.now()
        iso = now.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"

    def _default_weekly_state(self):
        return {
            "week_key": "",
            "scheduled_ts": 0,
            "scheduled_iso": "",
            "scheduled_day": "",
            "status": "",
            "preview": {},
            "last_sent_week_key": "",
        }

    def _weekly_state(self):
        raw = self.state.setdefault("weekly_broadcast", {})
        if not isinstance(raw, dict):
            raw = {}
            self.state["weekly_broadcast"] = raw
        merged = self._default_weekly_state()
        merged.update(raw)
        self.state["weekly_broadcast"] = merged
        return merged

    def _compute_weekly_schedule_for_key(self, week_key):
        seed = int(hashlib.sha256(str(week_key).encode("utf-8")).hexdigest()[:12], 16)
        rnd = random.Random(seed)
        day = int(rnd.choice(WEEKLY_BROADCAST_DAYS))
        hour = int(rnd.randint(WEEKLY_BROADCAST_START_HOUR, max(WEEKLY_BROADCAST_START_HOUR, WEEKLY_BROADCAST_END_HOUR - 1)))
        minute = int(rnd.randint(0, 59))
        base = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        delta = (day - base.weekday()) % 7
        scheduled = base + timedelta(days=delta, hours=hour, minutes=minute)
        return scheduled, day

    def _ensure_weekly_schedule(self):
        if not WEEKLY_BROADCAST_ENABLED:
            return
        weekly = self._weekly_state()
        week_key = self._current_week_key()
        if weekly.get("week_key") == week_key and int(weekly.get("scheduled_ts") or 0) > 0:
            return
        scheduled_dt, day = self._compute_weekly_schedule_for_key(week_key)
        weekly.update(
            {
                "week_key": week_key,
                "scheduled_ts": int(scheduled_dt.timestamp()),
                "scheduled_iso": scheduled_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "scheduled_day": "wednesday" if day == 2 else "thursday",
                "status": "scheduled",
                "preview": {},
            }
        )
        self._save_state()
        print(
            f"[weekly] schedule set week={week_key} day={weekly['scheduled_day']} at={weekly['scheduled_iso']}"
        )

    def _ensure_weekly_tables(self):
        if not os.path.exists(MONITOR_DB_PATH):
            return
        with self._db_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS weekly_broadcast_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    week_key TEXT,
                    scheduled_day TEXT,
                    puzzle_id TEXT,
                    status TEXT NOT NULL,
                    approved INTEGER NOT NULL DEFAULT 0,
                    sent_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    details_json TEXT
                )
                """
            )
            conn.commit()

    def _log_weekly_broadcast(self, week_key, scheduled_day, puzzle_id, status, approved=0, sent_count=0, failed_count=0, details=None):
        self._ensure_weekly_tables()
        payload = json.dumps(details or {}, ensure_ascii=False)
        with self._db_conn() as conn:
            conn.execute(
                """
                INSERT INTO weekly_broadcast_log (
                    created_at, week_key, scheduled_day, puzzle_id, status, approved, sent_count, failed_count, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_str(),
                    str(week_key or ""),
                    str(scheduled_day or ""),
                    str(puzzle_id or ""),
                    str(status or ""),
                    int(1 if approved else 0),
                    int(sent_count or 0),
                    int(failed_count or 0),
                    payload,
                ),
            )
            conn.commit()

    def _fetch_json(self, url, timeout=12, retries=0):
        last_error = None
        for attempt in range(max(0, int(retries)) + 1):
            try:
                req = urllib.request.Request(
                    url,
                    method="GET",
                    headers={
                        "User-Agent": "chess-miniapp-monitor/1.0",
                        "Accept": "application/json",
                    },
                )
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return resp.status, json.loads(resp.read().decode("utf-8") or "{}")
            except Exception as err:
                last_error = err
                if attempt < int(retries):
                    time.sleep(0.6 * (attempt + 1))
                    continue
                break
        raise RuntimeError(f"request failed for {url}: {last_error}")

    def _fetch_chesscom_random_puzzle(self):
        status, data = self._fetch_json(
            WEEKLY_BROADCAST_RANDOM_URL,
            timeout=WEEKLY_FETCH_TIMEOUT_SECONDS,
            retries=WEEKLY_FETCH_RETRIES,
        )
        if status != 200 or not isinstance(data, dict):
            raise RuntimeError(f"Chess.com random puzzle failed: HTTP {status}")
        return data

    def _puzzle_key(self, puzzle):
        if not isinstance(puzzle, dict):
            return ""
        return str(puzzle.get("url") or puzzle.get("id") or puzzle.get("fen") or "").strip()

    def _build_weekly_puzzle_set(self, size=WEEKLY_BROADCAST_SET_SIZE):
        wanted = max(1, int(size))
        puzzles = []
        seen = set()
        attempts = 0
        max_attempts = max(10, wanted * 8)
        while len(puzzles) < wanted and attempts < max_attempts:
            attempts += 1
            data = self._fetch_chesscom_random_puzzle()
            key = self._puzzle_key(data)
            if not key or key in seen:
                continue
            seen.add(key)
            puzzles.append(data)
        if len(puzzles) < wanted:
            fallback = self._load_recent_puzzles_from_state(limit=wanted)
            for item in fallback:
                key = self._puzzle_key(item)
                if key and key not in seen:
                    seen.add(key)
                    puzzles.append(item)
                if len(puzzles) >= wanted:
                    break
        if len(puzzles) < wanted:
            raise RuntimeError("Не удалось собрать набор задач для рассылки (проверьте доступ к Chess.com).")
        return puzzles

    def _load_recent_puzzles_from_state(self, limit=3):
        if not os.path.exists(MONITOR_DB_PATH):
            return []
        found = []
        seen = set()
        try:
            with self._db_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT current_puzzle_json
                    FROM user_state
                    WHERE current_puzzle_json IS NOT NULL
                    ORDER BY updated_at DESC
                    LIMIT 300
                    """
                ).fetchall()
            for row in rows:
                raw = row["current_puzzle_json"] if isinstance(row, sqlite3.Row) else row[0]
                if not raw:
                    continue
                try:
                    state = json.loads(raw)
                except Exception:
                    continue
                if not isinstance(state, dict):
                    continue
                puzzle = state.get("puzzleData") if isinstance(state.get("puzzleData"), dict) else None
                if not puzzle:
                    continue
                key = self._puzzle_key(puzzle)
                if not key or key in seen:
                    continue
                seen.add(key)
                found.append(puzzle)
                if len(found) >= max(1, int(limit)):
                    break
        except Exception as err:
            print(f"[weekly] fallback puzzle load failed: {err}")
            return []
        return found

    def _extract_openai_text(self, data):
        if not isinstance(data, dict):
            return ""
        direct = str(data.get("output_text") or "").strip()
        if direct:
            return direct
        out = data.get("output")
        if isinstance(out, list):
            chunks = []
            for item in out:
                content = item.get("content") if isinstance(item, dict) else None
                if not isinstance(content, list):
                    continue
                for block in content:
                    if isinstance(block, dict) and block.get("type") in {"output_text", "text"}:
                        text = str(block.get("text") or "").strip()
                        if text:
                            chunks.append(text)
            return " ".join(chunks).strip()
        return ""

    def _generate_weekly_short_text(self, previous_text=""):
        fallback = [
            "♟ Попробуй найти лучший ход в сегодняшней задаче.",
            "Разомнись за минуту — новая шахматная задача уже ждёт.",
            "Сможешь найти точный ход без подсказки?",
        ]
        if not OPENAI_API_KEY:
            return random.choice(fallback)
        prompt = (
            "Сгенерируй ОДНО короткое предложение на русском для Telegram-рассылки шахматной задачи. "
            "Без воскресных встреч, без длинной мотивации, без спама, без эмодзи кроме шахматного по желанию. "
            "Максимум 90 символов. Только готовый текст, без кавычек."
        )
        if previous_text:
            prompt += f"\nПредыдущий вариант (не повторяй): {previous_text}"
        body = {
            "model": OPENAI_MODEL,
            "input": prompt,
            "max_output_tokens": 60,
        }
        req = urllib.request.Request(
            "https://api.openai.com/v1/responses",
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read().decode("utf-8") or "{}")
            text = self._extract_openai_text(data).strip()
            text = re.sub(r"\s+", " ", text).strip().strip("\"'“”")
            if text:
                return text[:220]
        except Exception as err:
            print(f"[weekly] openai weekly text failed: {err}")
        return random.choice(fallback)

    def _weekly_preview_keyboard(self):
        weekly = self._weekly_state()
        preview = weekly.get("preview") if isinstance(weekly.get("preview"), dict) else {}
        selected_day = str(preview.get("scheduled_day") or weekly.get("scheduled_day") or "wednesday")
        wed_label = "📅 Ср ✅" if selected_day == "wednesday" else "📅 Ср"
        thu_label = "📅 Чт ✅" if selected_day == "thursday" else "📅 Чт"
        return {
            "inline_keyboard": [
                [
                    {"text": wed_label, "callback_data": "action:weekly_day:wednesday"},
                    {"text": thu_label, "callback_data": "action:weekly_day:thursday"},
                ],
                [{"text": "✅ Подтвердить", "callback_data": "action:weekly_confirm"}],
                [{"text": "🔁 Изменить текст", "callback_data": "action:weekly_regen"}],
                [{"text": "❌ Отмена", "callback_data": "action:weekly_cancel"}],
            ]
        }

    def _format_weekly_preview_text(self, preview):
        first = (preview.get("puzzles") or [{}])[0]
        day = str(preview.get("scheduled_day") or "wednesday")
        day_ru = "среда" if day == "wednesday" else "четверг" if day == "thursday" else day
        lines = [
            "📣 <b>Предпросмотр недельной рассылки</b>",
            f"Неделя: <code>{escape(str(preview.get('week_key') or ''))}</code>",
            f"Выбран день: <code>{escape(day_ru)}</code>",
            "",
            "<b>Авто-логика:</b> 1 раз в неделю, случайно ср/чт, окно 10:00-12:00, отправка только после подтверждения.",
            "",
            f"<b>Текст:</b> {escape(str(preview.get('text') or ''))}",
            "",
            "<b>Основная задача (для всех):</b>",
            f"• id/url: <code>{escape(str(first.get('url') or first.get('id') or 'n/a'))}</code>",
            f"• fen: <code>{escape(str(first.get('fen') or 'n/a'))}</code>",
            f"• title: {escape(str(first.get('title') or 'n/a'))}",
        ]
        if first.get("url"):
            lines.append(f"• ссылка: {escape(str(first.get('url')))}")
        lines.extend(
            [
                "",
                "Нажмите <b>Подтвердить</b>, чтобы отправить всем пользователям.",
                "Или <b>Изменить текст</b> для перегенерации.",
                "Можно отправить свой текст обычным сообщением в этот чат.",
            ]
        )
        return "\n".join(lines)

    def _send_weekly_preview(self, preview):
        first = (preview.get("puzzles") or [{}])[0]
        text = self._format_weekly_preview_text(preview)
        image_url = self._extract_puzzle_image_url(first)
        if image_url:
            self._send_monitor_photo(
                MONITOR_CHAT_ID,
                image_url,
                caption=text,
                reply_markup=self._weekly_preview_keyboard(),
                parse_mode="HTML",
            )
            return
        self.send_message(
            MONITOR_CHAT_ID,
            text,
            reply_markup=self._weekly_preview_keyboard(),
            parse_mode="HTML",
        )

    def _prepare_weekly_preview(self, force_regen=False, custom_text=None):
        weekly = self._weekly_state()
        week_key = weekly.get("week_key") or self._current_week_key()
        scheduled_day = weekly.get("scheduled_day") or "wednesday"
        preview = weekly.get("preview") if isinstance(weekly.get("preview"), dict) else {}

        puzzles = preview.get("puzzles") if isinstance(preview.get("puzzles"), list) else []
        if not puzzles:
            puzzles = self._build_weekly_puzzle_set(WEEKLY_BROADCAST_SET_SIZE)

        previous_text = str(preview.get("text") or "")
        if custom_text:
            text = str(custom_text).strip()
        elif force_regen or not previous_text:
            text = self._generate_weekly_short_text(previous_text=previous_text)
        else:
            text = previous_text

        preview = {
            "week_key": week_key,
            "scheduled_day": scheduled_day,
            "text": text,
            "puzzles": puzzles,
            "created_at": now_str(),
            "approved": False,
        }
        weekly["preview"] = preview
        weekly["status"] = "preview_pending"
        self._save_state()
        self._log_weekly_broadcast(
            week_key=week_key,
            scheduled_day=scheduled_day,
            puzzle_id=self._puzzle_key(puzzles[0] if puzzles else {}),
            status="preview_created",
            approved=0,
            details={"text": text},
        )
        self._send_weekly_preview(preview)
        return preview

    def _main_bot_api(self, method, payload=None, timeout=MAIN_BOT_REQUEST_TIMEOUT):
        if not MAIN_BOT_API_BASE:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
        req = urllib.request.Request(
            f"{MAIN_BOT_API_BASE}/{method}",
            data=json.dumps(payload or {}, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8") or "{}")

    def _extract_puzzle_image_url(self, puzzle):
        if not isinstance(puzzle, dict):
            return ""
        for key in ("image", "image_url", "imageUrl"):
            val = str(puzzle.get(key) or "").strip()
            if val.startswith("http://") or val.startswith("https://"):
                return val
        return ""

    def _send_monitor_photo(self, chat_id, photo_url, caption="", reply_markup=None, parse_mode=None):
        payload = {
            "chat_id": int(chat_id),
            "photo": str(photo_url),
            "caption": str(caption or "")[:1024],
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        if parse_mode:
            payload["parse_mode"] = parse_mode
        return self._telegram_api("sendPhoto", payload, timeout=MAIN_BOT_REQUEST_TIMEOUT)

    def _send_main_bot_photo(self, chat_id, photo_url, caption="", reply_markup=None, parse_mode=None):
        payload = {
            "chat_id": int(chat_id),
            "photo": str(photo_url),
            "caption": str(caption or "")[:1024],
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        if parse_mode:
            payload["parse_mode"] = parse_mode
        return self._main_bot_api("sendPhoto", payload, timeout=MAIN_BOT_REQUEST_TIMEOUT)

    def _send_main_bot_message(self, chat_id, text, puzzle=None):
        image_url = self._extract_puzzle_image_url(puzzle or {})
        if image_url:
            return self._send_main_bot_photo(
                chat_id,
                image_url,
                caption=text,
                reply_markup={
                    "inline_keyboard": [
                        [
                            {
                                "text": "Открыть Mini App",
                                "url": MINI_APP_URL,
                            }
                        ]
                    ]
                },
            )
        payload = {
            "chat_id": int(chat_id),
            "text": str(text or ""),
            "disable_web_page_preview": True,
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
        }
        return self._main_bot_api("sendMessage", payload, timeout=MAIN_BOT_REQUEST_TIMEOUT)

    def _build_puzzle_state_payload(self, puzzle, puzzle_set):
        now_ms = int(time.time() * 1000)
        fen = str(puzzle.get("fen") or "")
        active = "b" if (len(fen.split(" ")) > 1 and fen.split(" ")[1] == "b") else "w"
        puzzle_state = {
            "puzzleData": puzzle,
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
        }
        now_utc_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        window_start = ((now_utc_ms - WEEKLY_RESET_UTC_OFFSET_MS) // (24 * 60 * 60 * 1000)) * (24 * 60 * 60 * 1000) + WEEKLY_RESET_UTC_OFFSET_MS
        quota_state = {
            "windowStart": int(window_start),
            "started": 0,
            "solved": 0,
            "bonusActivated": False,
            "dayDone": False,
            "bonusUnlocked": False,
        }
        return puzzle_state, quota_state, now_ms

    def _upsert_user_state(self, conn, telegram_id, state_json, current_puzzle_json):
        conn.execute(
            """
            INSERT INTO user_state (
                telegram_id, state_json, current_puzzle_json, settings_json, quota_json, history_json, updated_at
            ) VALUES (?, ?, ?, NULL, NULL, NULL, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                state_json = excluded.state_json,
                current_puzzle_json = excluded.current_puzzle_json,
                updated_at = excluded.updated_at
            """,
            (
                int(telegram_id),
                json.dumps(state_json, ensure_ascii=False),
                json.dumps(current_puzzle_json, ensure_ascii=False),
                now_str(),
            ),
        )

    def _execute_weekly_broadcast(self):
        weekly = self._weekly_state()
        preview = weekly.get("preview") if isinstance(weekly.get("preview"), dict) else {}
        if not preview:
            raise RuntimeError("Нет предпросмотра для рассылки.")
        week_key = str(weekly.get("week_key") or self._current_week_key())
        if str(weekly.get("last_sent_week_key") or "") == week_key:
            raise RuntimeError(f"Рассылка за неделю {week_key} уже была отправлена.")
        if not MAIN_BOT_TOKEN:
            raise RuntimeError("TELEGRAM_BOT_TOKEN не задан.")
        puzzles = preview.get("puzzles") or []
        if not puzzles:
            raise RuntimeError("Набор задач отсутствует.")
        message_text = str(preview.get("text") or "").strip()
        if not message_text:
            raise RuntimeError("Текст рассылки пустой.")

        primary_puzzle = puzzles[0]
        puzzle_set = {
            "weekKey": week_key,
            "createdAt": int(time.time() * 1000),
            "puzzles": puzzles[:WEEKLY_BROADCAST_SET_SIZE],
        }
        puzzle_state, quota_state, now_ms = self._build_puzzle_state_payload(primary_puzzle, puzzle_set)

        with self._db_conn() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT telegram_id
                FROM users
                WHERE telegram_id IS NOT NULL
                ORDER BY telegram_id ASC
                """
            ).fetchall()
            sent = 0
            failed = 0
            for row in rows:
                tg_id = int(row["telegram_id"])
                try:
                    self._send_main_bot_message(tg_id, message_text, puzzle=primary_puzzle)
                    sent += 1
                except Exception as err:
                    failed += 1
                    print(f"[weekly] send failed telegram_id={tg_id}: {err}")
                try:
                    existing = conn.execute(
                        "SELECT state_json FROM user_state WHERE telegram_id = ?",
                        (tg_id,),
                    ).fetchone()
                    state = {}
                    if existing and existing["state_json"]:
                        try:
                            state = json.loads(existing["state_json"]) or {}
                        except Exception:
                            state = {}
                    if not isinstance(state, dict):
                        state = {}
                    state["version"] = 1
                    state["savedAt"] = now_ms
                    state["quotaResetAt"] = now_ms
                    state["quota"] = quota_state
                    state["puzzle"] = puzzle_state
                    self._upsert_user_state(conn, tg_id, state, puzzle_state)
                except Exception as err:
                    print(f"[weekly] state upsert failed telegram_id={tg_id}: {err}")
                time.sleep(0.03)
            conn.commit()

        weekly["status"] = "sent"
        weekly["last_sent_week_key"] = week_key
        preview["approved"] = True
        preview["sent_at"] = now_str()
        weekly["preview"] = preview
        self._save_state()
        self._log_weekly_broadcast(
            week_key=weekly.get("week_key"),
            scheduled_day=weekly.get("scheduled_day"),
            puzzle_id=self._puzzle_key(primary_puzzle),
            status="sent",
            approved=1,
            sent_count=sent,
            failed_count=failed,
            details={"text": message_text},
        )
        return sent, failed

    def _is_owner(self, chat_id):
        return int(chat_id or 0) == int(MONITOR_CHAT_ID or 0)

    def _has_pending_weekly_preview(self):
        weekly = self._weekly_state()
        preview = weekly.get("preview") if isinstance(weekly.get("preview"), dict) else {}
        return bool(weekly.get("status") == "preview_pending" and preview.get("text"))

    def _try_weekly_trigger(self):
        if not WEEKLY_BROADCAST_ENABLED:
            return
        self._ensure_weekly_schedule()
        weekly = self._weekly_state()
        if weekly.get("status") != "scheduled":
            return
        scheduled_ts = int(weekly.get("scheduled_ts") or 0)
        if scheduled_ts <= 0:
            return
        if int(time.time()) < scheduled_ts:
            return
        try:
            self._prepare_weekly_preview(force_regen=False)
            if MONITOR_CHAT_ID:
                try:
                    self.send_message(
                        MONITOR_CHAT_ID,
                        "⏰ Наступило время недельной рассылки. Проверьте предпросмотр и подтвердите отправку.",
                    )
                except Exception:
                    pass
        except Exception as err:
            print(f"[weekly] preview preparation failed: {err}")
            self._log_weekly_broadcast(
                week_key=weekly.get("week_key"),
                scheduled_day=weekly.get("scheduled_day"),
                puzzle_id="",
                status="preview_failed",
                approved=0,
                details={"error": str(err)},
            )

    def _handle_message(self, message):
        chat_id = int((message.get("chat") or {}).get("id") or 0)
        text = str(message.get("text") or "").strip()
        if not text:
            return
        if text.startswith("/start"):
            if self._is_owner(chat_id):
                self.clear_flow(chat_id)
                self.render_screen(chat_id, "main")
            else:
                self.send_message(chat_id, "Доступ ограничен.")
            return
        if not self._is_owner(chat_id):
            self.send_message(chat_id, "Доступ ограничен.")
            return
        if self.get_flow(chat_id) == "connect_input":
            try:
                cfg = json.loads(text)
                if not isinstance(cfg, dict):
                    raise ValueError("JSON должен быть объектом")
                self.state.setdefault("connections", {})[self._chat_key(chat_id)] = cfg
                self._save_state()
                self.clear_flow(chat_id)
                self.send_message(chat_id, "✅ Данные подключения сохранены.")
                self.render_screen(chat_id, "main", edit_message_id=self.get_ui_message_id(chat_id))
            except Exception as err:
                self.send_message(chat_id, f"⚠️ Ошибка JSON: {err}")
            return
        if self.get_flow(chat_id) == "weekly_custom_text":
            try:
                self.clear_flow(chat_id)
                self._prepare_weekly_preview(custom_text=text)
            except Exception as err:
                self.send_message(chat_id, f"⚠️ Не удалось применить текст: {err}")
            return
        if self._has_pending_weekly_preview() and not text.startswith("/"):
            try:
                self._prepare_weekly_preview(custom_text=text)
            except Exception as err:
                self.send_message(chat_id, f"⚠️ Не удалось обновить предпросмотр: {err}")
            return
        if text.startswith("/status"):
            self.render_screen(chat_id, "status", edit_message_id=self.get_ui_message_id(chat_id))
        elif text.startswith("/weekly"):
            try:
                self._ensure_weekly_schedule()
                self._prepare_weekly_preview(force_regen=False)
            except Exception as err:
                self.send_message(chat_id, f"⚠️ Ошибка подготовки рассылки: {err}")
        elif text.startswith("/dbquick"):
            self.send_message(chat_id, self.build_db_usage_summary_report())
        elif text.startswith("/dbfull"):
            self.send_db_full_report_html(chat_id)
        elif text.startswith("/db"):
            self.send_message(chat_id, self.build_db_report())
        elif text.startswith("/check"):
            self.run_checks_and_alerts(force=True, chat_id=chat_id)
        elif text.startswith("/stockfish"):
            self.send_message(chat_id, self.build_stockfish_check_report(), parse_mode="HTML")
        else:
            self.send_message(chat_id, "Команды: /status, /weekly, /dbquick, /db, /dbfull, /check, /stockfish")

    def _handle_callback(self, callback):
        callback_id = callback.get("id")
        data = str(callback.get("data") or "")
        message = callback.get("message") or {}
        chat_id = int((message.get("chat") or {}).get("id") or 0)
        message_id = int(message.get("message_id") or 0)
        if not self._is_owner(chat_id):
            self.answer_callback(callback_id, "Доступ ограничен")
            return
        if data.startswith("screen:"):
            self.answer_callback(callback_id, "Открываю")
            self.clear_flow(chat_id)
            self.render_screen(chat_id, data.split(":", 1)[1], edit_message_id=message_id)
            return
        if data == "action:check_now":
            self.answer_callback(callback_id, "Проверяю")
            self.run_checks_and_alerts(force=True, chat_id=chat_id)
        elif data == "action:stockfish_check":
            self.answer_callback(callback_id, "Проверяю Stockfish")
            self.send_message(chat_id, self.build_stockfish_check_report(), parse_mode="HTML")
        elif data == "action:weekly_prepare":
            self.answer_callback(callback_id, "Готовлю рассылку")
            try:
                self._ensure_weekly_schedule()
                self._prepare_weekly_preview(force_regen=False)
            except Exception as err:
                self.send_message(chat_id, f"⚠️ Ошибка подготовки рассылки: {err}")
        elif data == "action:weekly_regen":
            self.answer_callback(callback_id, "Генерирую новый текст")
            try:
                self._prepare_weekly_preview(force_regen=True)
            except Exception as err:
                self.send_message(chat_id, f"⚠️ Ошибка генерации текста: {err}")
        elif data.startswith("action:weekly_day:"):
            day = data.split(":", 2)[2].strip().lower()
            if day not in {"wednesday", "thursday"}:
                self.answer_callback(callback_id, "Некорректный день")
                return
            weekly = self._weekly_state()
            weekly["scheduled_day"] = day
            preview = weekly.get("preview") if isinstance(weekly.get("preview"), dict) else {}
            if isinstance(preview, dict) and preview:
                preview["scheduled_day"] = day
                weekly["preview"] = preview
            self._save_state()
            self.answer_callback(callback_id, f"День: {'среда' if day == 'wednesday' else 'четверг'}")
            try:
                self._prepare_weekly_preview(force_regen=False)
            except Exception as err:
                self.send_message(chat_id, f"⚠️ Не удалось обновить день: {err}")
        elif data == "action:weekly_cancel":
            self.answer_callback(callback_id, "Рассылка отменена")
            weekly = self._weekly_state()
            preview = weekly.get("preview") if isinstance(weekly.get("preview"), dict) else {}
            weekly["status"] = "cancelled"
            self._save_state()
            self._log_weekly_broadcast(
                week_key=weekly.get("week_key"),
                scheduled_day=weekly.get("scheduled_day"),
                puzzle_id=self._puzzle_key((preview.get("puzzles") or [{}])[0]),
                status="cancelled",
                approved=0,
                details={"text": str(preview.get("text") or "")},
            )
            self.send_message(chat_id, "❌ Недельная рассылка отменена. Автоотправка не выполнялась.")
        elif data == "action:weekly_confirm":
            self.answer_callback(callback_id, "Запускаю рассылку")
            try:
                sent, failed = self._execute_weekly_broadcast()
                self.send_message(
                    chat_id,
                    f"✅ Недельная рассылка отправлена.\nУспешно: {sent}\nОшибки: {failed}",
                )
            except Exception as err:
                self.send_message(chat_id, f"⚠️ Ошибка отправки рассылки: {err}")
        elif data == "action:db_short":
            self.answer_callback(callback_id, "Готовлю")
            self.send_message(chat_id, self.build_db_report())
        elif data.startswith("action:db_short:"):
            period = data.split(":", 2)[2]
            self.answer_callback(callback_id, "Готовлю")
            self.send_message(chat_id, self.build_db_report(period=period))
        elif data == "action:db_usage_summary":
            self.answer_callback(callback_id, "Готовлю")
            self.send_message(chat_id, self.build_db_usage_summary_report())
        elif data == "action:db_full":
            self.answer_callback(callback_id, "Формирую HTML")
            self.send_db_full_report_html(chat_id)
        elif data == "action:toggle_monitor":
            self.state["monitoring_enabled"] = not bool(self.state.get("monitoring_enabled", True))
            self._save_state()
            self.answer_callback(callback_id, "Состояние обновлено")
            self.render_screen(chat_id, "main", edit_message_id=message_id)
        elif data == "action:connect_input":
            self.answer_callback(callback_id, "Жду данные")
            self.set_flow(chat_id, "connect_input")
            self.send_message(chat_id, "📝 Отправьте JSON с данными подключения одним сообщением.")
        else:
            self.answer_callback(callback_id, "Неизвестная команда")

    def poll_updates(self, timeout=5):
        payload = {"timeout": max(1, int(timeout)), "allowed_updates": ["message", "callback_query"]}
        last_update_id = int(self.state.get("last_update_id", 0) or 0)
        if last_update_id > 0:
            payload["offset"] = last_update_id + 1
        response = self._telegram_api("getUpdates", payload, timeout=max(10, timeout + 5))
        for update in response.get("result", []):
            update_id = int(update.get("update_id") or 0)
            if update_id:
                self.state["last_update_id"] = max(last_update_id, update_id)
                last_update_id = self.state["last_update_id"]
            if "message" in update:
                self._handle_message(update["message"])
            elif "callback_query" in update:
                self._handle_callback(update["callback_query"])
        self._save_state()

    def _should_send_alert(self, key):
        now = time.time()
        last = float((self.state.get("last_alert_at") or {}).get(key, 0) or 0)
        if now - last < MONITOR_ALERT_COOLDOWN_SECONDS:
            return False
        self.state.setdefault("last_alert_at", {})[key] = now
        return True

    def _send_alert(self, key, text, force=False):
        if not MONITOR_CHAT_ID:
            return
        if force or self._should_send_alert(key):
            try:
                self.send_message(MONITOR_CHAT_ID, text)
                self._save_state()
            except Exception:
                # ?? ?????? ??????????, ???? Telegram ???????? ??????????.
                pass

    def check_backend_health(self):
        started = time.time()
        try:
            req = urllib.request.Request(MONITOR_HEALTH_URL, method="GET")
            with urllib.request.urlopen(req, timeout=6) as resp:
                body = resp.read().decode("utf-8")
                elapsed = int((time.time() - started) * 1000)
                if resp.status != 200:
                    return [("health_status", f"Health-check HTTP {resp.status}", "critical")]
                data = json.loads(body or "{}")
                if not data.get("ok", False):
                    return [("health_ok", "Health-check вернул ok=false", "critical")]
                return [("health_slow", f"Медленный health-check: {elapsed} мс", "warning")] if elapsed > 2000 else []
        except Exception as err:
            return [("health_unreachable", f"Сервис недоступен: {err}", "critical")]

    def _guess_api_base(self):
        parsed = urllib.parse.urlparse(MONITOR_HEALTH_URL)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return "http://127.0.0.1:8080"

    def _post_json(self, url, payload, timeout=8):
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw or "{}")

    def build_stockfish_check_report(self):
        started = time.time()
        lines = ["♟ <b>Проверка Stockfish</b>", f"Время: {now_str()}"]

        api_base = self._guess_api_base()
        health_url = f"{api_base}/health"
        analyze_url = f"{api_base}/analyze"
        evaluate_url = f"{api_base}/evaluate_move"
        lines.append(f"API: <code>{escape(api_base)}</code>")

        health_ok = False
        try:
            req = urllib.request.Request(health_url, method="GET")
            with urllib.request.urlopen(req, timeout=6) as resp:
                body = resp.read().decode("utf-8")
                data = json.loads(body or "{}")
                health_ok = bool(resp.status == 200 and data.get("ok"))
                status_icon = "✅" if health_ok else "⚠️"
                lines.append(f"{status_icon} /health: HTTP {resp.status}, ok={data.get('ok')}")
                lines.append(f"Engine: <code>{escape(str(data.get('engine', 'n/a')))}</code>")
                lines.append(
                    "Pool: size={0}, available={1}".format(
                        data.get("pool_size", "n/a"),
                        data.get("available", "n/a"),
                    )
                )
        except Exception as err:
            lines.append(f"❌ /health ошибка: <code>{escape(str(err))}</code>")

        analyze_ok = False
        best_move = ""
        best_cp = None
        best_mate = None
        try:
            analyze_payload = {
                "fen": STOCKFISH_TEST_FEN,
                "movetime_ms": 400,
                "multipv": 1,
                "side": "turn",
            }
            t0 = time.time()
            status, data = self._post_json(analyze_url, analyze_payload, timeout=10)
            elapsed_ms = int((time.time() - t0) * 1000)
            lines_data = data.get("lines") or []
            if status == 200 and isinstance(lines_data, list) and lines_data:
                best_line = lines_data[0] if isinstance(lines_data[0], dict) else {}
                best_move = str(best_line.get("best_move_uci") or "")
                best_cp = best_line.get("score_cp")
                best_mate = best_line.get("mate")
                has_eval = best_cp is not None or best_mate is not None
                analyze_ok = bool(best_move and has_eval)
                lines.append(
                    f"{'✅' if analyze_ok else '⚠️'} /analyze: HTTP {status}, {elapsed_ms} мс, "
                    f"best={best_move or 'n/a'}, cp={best_cp if best_cp is not None else 'null'}, "
                    f"mate={best_mate if best_mate is not None else 'null'}"
                )
            else:
                lines.append(f"❌ /analyze: HTTP {status}, пустой ответ")
        except Exception as err:
            lines.append(f"❌ /analyze ошибка: <code>{escape(str(err))}</code>")

        evaluate_ok = False
        try:
            move_for_check = best_move or "e2e4"
            evaluate_payload = {
                "fen": STOCKFISH_TEST_FEN,
                "move_uci": move_for_check,
                "movetime_ms": 400,
                "side": "turn",
            }
            t0 = time.time()
            status, data = self._post_json(evaluate_url, evaluate_payload, timeout=10)
            elapsed_ms = int((time.time() - t0) * 1000)
            legal = bool(data.get("legal"))
            label = str(data.get("label") or "n/a")
            best_score = data.get("best_score") if isinstance(data.get("best_score"), dict) else {}
            after_score = data.get("after_score") if isinstance(data.get("after_score"), dict) else {}
            best_score_cp = best_score.get("score_cp")
            best_score_mate = best_score.get("mate")
            after_score_cp = after_score.get("score_cp")
            after_score_mate = after_score.get("mate")
            has_eval = (
                best_score_cp is not None
                or best_score_mate is not None
                or after_score_cp is not None
                or after_score_mate is not None
            )
            evaluate_ok = bool(status == 200 and legal and has_eval)
            lines.append(
                f"{'✅' if evaluate_ok else '⚠️'} /evaluate_move: HTTP {status}, {elapsed_ms} мс, legal={legal}, "
                f"label={escape(label)}, best_cp={best_score_cp if best_score_cp is not None else 'null'}, "
                f"after_cp={after_score_cp if after_score_cp is not None else 'null'}, "
                f"best_mate={best_score_mate if best_score_mate is not None else 'null'}, "
                f"after_mate={after_score_mate if after_score_mate is not None else 'null'}"
            )
        except Exception as err:
            lines.append(f"❌ /evaluate_move ошибка: <code>{escape(str(err))}</code>")

        total_ms = int((time.time() - started) * 1000)
        overall_ok = health_ok and analyze_ok and evaluate_ok
        lines.append("")
        if overall_ok:
            lines.append("✅ <b>Итог: Stockfish работает корректно</b>")
        else:
            lines.append("⚠️ <b>Итог: есть проблемы, см. детали выше</b>")
        lines.append(f"Общее время проверки: {total_ms} мс")
        return "\n".join(lines)

    def check_database(self):
        if not os.path.exists(MONITOR_DB_PATH):
            return [("db_missing", f"База данных не найдена: {os.path.abspath(MONITOR_DB_PATH)}", "critical")]
        alerts = []
        try:
            with sqlite3.connect(MONITOR_DB_PATH) as conn:
                row = conn.execute("PRAGMA quick_check").fetchone()
                if str(row[0] if row else "").lower() != "ok":
                    alerts.append(("db_quick_check", "PRAGMA quick_check вернул ошибку", "critical"))
        except Exception as err:
            alerts.append(("db_open_error", f"Ошибка базы данных: {err}", "critical"))
        return alerts

    def check_disk(self):
        try:
            usage = shutil.disk_usage(os.path.dirname(os.path.abspath(MONITOR_DB_PATH)) or os.getcwd())
            free_gb = usage.free / (1024 ** 3)
            if free_gb < MONITOR_MIN_FREE_GB:
                return [("disk_low", f"Мало места на диске: {free_gb:.2f} ГБ", "critical")]
            return []
        except Exception as err:
            return [("disk_error", f"Ошибка проверки диска: {err}", "warning")]

    def check_logs(self):
        offsets = self.state.setdefault("log_offsets", {})
        alerts = []
        for path in MONITOR_LOG_FILES:
            if not path or not os.path.exists(path):
                continue
            try:
                size = os.path.getsize(path)
                prev = int(offsets.get(path, 0) or 0)
                if size < prev:
                    prev = 0
                start = max(0, size - MONITOR_MAX_LOG_SCAN_BYTES, prev)
                with open(path, "rb") as f:
                    f.seek(start)
                    data = f.read()
                offsets[path] = size
                text = data.decode("utf-8", errors="ignore")
                hit = []
                for line in text.splitlines():
                    line = line.strip()
                    if line and any(p.search(line) for p in ERROR_PATTERNS):
                        hit.append(line)
                    if len(hit) >= 3:
                        break
                if hit:
                    alerts.append((f"log_{os.path.basename(path)}", f"Ошибки в логе {path}:\n" + "\n".join(f"- {x[:200]}" for x in hit), "warning"))
            except Exception as err:
                alerts.append((f"log_read_{path}", f"Ошибка чтения лога {path}: {err}", "warning"))
        return alerts

    def check_analysis_error_events(self):
        if not os.path.exists(MONITOR_DB_PATH):
            return []
        alerts = []
        last_seen_id = int(self.state.get("last_analysis_error_event_id", 0) or 0)
        max_seen_id = last_seen_id
        try:
            with sqlite3.connect(MONITOR_DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    """
                    SELECT id, event_data, created_at
                    FROM events
                    WHERE event_name = 'analysis_error' AND id > ?
                    ORDER BY id ASC
                    LIMIT 20
                    """,
                    (last_seen_id,),
                ).fetchall()
            if not rows:
                return []

            details = []
            for row in rows[:3]:
                max_seen_id = max(max_seen_id, int(row["id"] or 0))
                payload = {}
                try:
                    payload = json.loads(row["event_data"] or "{}")
                except Exception:
                    payload = {}
                msg = str(payload.get("message") or "unknown error")
                fen = str(payload.get("fen") or "")
                details.append(f"#{row['id']} {row['created_at']}: {msg[:140]} | fen={fen[:40]}")

            max_seen_id = max(max_seen_id, max(int(r["id"] or 0) for r in rows))
            self.state["last_analysis_error_event_id"] = max_seen_id
            self._save_state()

            alerts.append(
                (
                    f"analysis_error_event_{max_seen_id}",
                    "Новые ошибки анализа от клиентов:\n" + "\n".join(f"- {x}" for x in details),
                    "warning",
                )
            )
        except Exception as err:
            alerts.append(("analysis_error_event_check", f"Ошибка чтения analysis_error событий: {err}", "warning"))
        return alerts

    def run_checks_and_alerts(self, force=False, chat_id=None):
        alerts = []
        alerts.extend(self.check_backend_health())
        alerts.extend(self.check_database())
        alerts.extend(self.check_disk())
        alerts.extend(self.check_analysis_error_events())
        alerts.extend(self.check_logs())
        if force and chat_id:
            self.send_message(chat_id, "✅ Проблем не обнаружено." if not alerts else "⚠️ Обнаружены проблемы, отправляю детали.")
        for key, message, severity in alerts:
            prefix = "🚨 Критично" if severity == "critical" else "⚠️ Предупреждение"
            self._send_alert(key, f"{prefix} ({now_str()}):\n{message}", force=force)

    def run(self):
        if not MONITOR_BOT_TOKEN:
            raise RuntimeError("Не задан TELEGRAM_MONITOR_BOT_TOKEN в .env")
        if not MONITOR_CHAT_ID:
            raise RuntimeError("Не задан TELEGRAM_MONITOR_CHAT_ID в .env")
        self._ensure_weekly_tables()
        self._ensure_weekly_schedule()
        self._send_alert("monitor_started", f"🟢 Мониторинг запущен ({now_str()}).", force=True)
        next_check_at = 0.0
        while True:
            try:
                self._try_weekly_trigger()
                now_ts = time.time()
                if self.state.get("monitoring_enabled", True) and now_ts >= next_check_at:
                    self.run_checks_and_alerts(force=False)
                    next_check_at = now_ts + MONITOR_INTERVAL_SECONDS
                self.poll_updates(timeout=4)
            except KeyboardInterrupt:
                self._send_alert("monitor_stopped", f"🔴 Мониторинг остановлен ({now_str()}).", force=True)
                break
            except (urllib.error.URLError, TimeoutError, http.client.RemoteDisconnected, ConnectionError, OSError) as err:
                self._send_alert("monitor_network_error", f"Сетевая ошибка мониторинга: {err}", force=False)
                time.sleep(2)
            except Exception as err:
                self._send_alert("monitor_runtime_error", f"Ошибка мониторинга: {err}", force=False)
                time.sleep(2)


if __name__ == "__main__":
    MonitorBot().run()




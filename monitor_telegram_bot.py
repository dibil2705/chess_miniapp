import json
import http.client
import mimetypes
import os
import re
import shutil
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime
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
MONITOR_HEALTH_URL = os.environ.get("MONITOR_HEALTH_URL", "http://127.0.0.1:8080/health")
MONITOR_INTERVAL_SECONDS = max(10, int(os.environ.get("MONITOR_INTERVAL_SECONDS", "30") or "30"))
MONITOR_ALERT_COOLDOWN_SECONDS = max(30, int(os.environ.get("MONITOR_ALERT_COOLDOWN_SECONDS", "180") or "180"))
MONITOR_MIN_FREE_GB = float(os.environ.get("MONITOR_MIN_FREE_GB", "1.0") or "1.0")
MONITOR_MAX_LOG_SCAN_BYTES = max(32768, int(os.environ.get("MONITOR_MAX_LOG_SCAN_BYTES", "262144") or "262144"))
MONITOR_LOG_FILES = [x.strip() for x in os.environ.get("MONITOR_LOG_FILES", "backend_8000.err.log,backend_8000.log,backend_direct.err.log,backend_direct.out.log,backend_main_combined.log").split(",") if x.strip()]

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
            "ui_message_id_by_chat": {},
            "chat_flow": {},
            "connections": {},
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
                "ui_message_id_by_chat": dict(raw.get("ui_message_id_by_chat") or {}),
                "chat_flow": dict(raw.get("chat_flow") or {}),
                "connections": dict(raw.get("connections") or {}),
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

    def _is_owner(self, chat_id):
        return int(chat_id or 0) == int(MONITOR_CHAT_ID or 0)

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
        if text.startswith("/status"):
            self.render_screen(chat_id, "status", edit_message_id=self.get_ui_message_id(chat_id))
        elif text.startswith("/dbquick"):
            self.send_message(chat_id, self.build_db_usage_summary_report())
        elif text.startswith("/dbfull"):
            self.send_db_full_report_html(chat_id)
        elif text.startswith("/db"):
            self.send_message(chat_id, self.build_db_report())
        elif text.startswith("/check"):
            self.run_checks_and_alerts(force=True, chat_id=chat_id)
        else:
            self.send_message(chat_id, "Команды: /status, /dbquick, /db, /dbfull, /check")

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

    def run_checks_and_alerts(self, force=False, chat_id=None):
        alerts = []
        alerts.extend(self.check_backend_health())
        alerts.extend(self.check_database())
        alerts.extend(self.check_disk())
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
        self._send_alert("monitor_started", f"🟢 Мониторинг запущен ({now_str()}).", force=True)
        next_check_at = 0.0
        while True:
            try:
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




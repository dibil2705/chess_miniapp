import os
import asyncio
import shutil
import time
from typing import List, Optional, Dict, Any, Tuple
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import chess
import chess.engine

# --- Важно для Windows + Python 3.12 ---
# Если event loop без поддержки subprocess, запуск Stockfish упадёт.
if os.name == "nt":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass


# --- Настройки ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BUNDLED_STOCKFISH_PATH = os.path.join(BASE_DIR, "engines", "stockfish", "stockfish.exe")
STOCKFISH_PATH = os.getenv("STOCKFISH_PATH", BUNDLED_STOCKFISH_PATH)
ENGINE_POOL_SIZE = int(os.getenv("ENGINE_POOL_SIZE", "1"))
DEFAULT_DEPTH = int(os.getenv("DEFAULT_DEPTH", "14"))
DEFAULT_MULTIPV = int(os.getenv("DEFAULT_MULTIPV", "3"))
DEFAULT_MOVETIME_MS = int(os.getenv("DEFAULT_MOVETIME_MS", "1500"))  # по умолчанию лучше time-limit (мс) вместо depth

# Жёсткие лимиты, чтобы сервер не убивали большими depth/time
MAX_DEPTH = int(os.getenv("MAX_DEPTH", "20"))
MAX_MOVETIME_MS = int(os.getenv("MAX_MOVETIME_MS", "2500"))
MAX_MULTIPV = int(os.getenv("MAX_MULTIPV", "5"))

ENGINE_OPTIONS = {
    "Threads": int(os.getenv("SF_THREADS", "2")),
    "Hash": int(os.getenv("SF_HASH_MB", "128")),
}

# Rate limit (очень базово, в памяти)
# Разные лимиты для разных эндпоинтов, чтобы /evaluate_move не спамили "на перетаскивание".
RATE_LIMIT_ANALYZE_RPM = int(os.getenv("RATE_LIMIT_ANALYZE_RPM", "60"))
RATE_LIMIT_EVAL_RPM = int(os.getenv("RATE_LIMIT_EVAL_RPM", "120"))
RATE_LIMIT_OTHER_RPM = int(os.getenv("RATE_LIMIT_OTHER_RPM", "240"))
RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "1") == "1"

# Cache (в памяти)
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "600"))  # 10 минут
CACHE_MAX_ITEMS = int(os.getenv("CACHE_MAX_ITEMS", "5000"))
CACHE_ENABLED = os.getenv("CACHE_ENABLED", "1") == "1"


# --- Логирование ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stockfish_api")


# --- API модели ---
class AnalyzeRequest(BaseModel):
    fen: str = Field(..., description="FEN позиции")
    depth: Optional[int] = Field(None, ge=1, le=MAX_DEPTH)
    movetime_ms: Optional[int] = Field(None, ge=1, le=MAX_MOVETIME_MS)
    multipv: Optional[int] = Field(None, ge=1, le=MAX_MULTIPV)
    side: str = Field("turn", description="turn/white/black")


class PVLine(BaseModel):
    rank: int
    best_move_uci: str
    pv_uci: List[str]
    pv_san: List[str]
    score_cp: Optional[int] = None
    mate: Optional[int] = None


class AnalyzeResponse(BaseModel):
    fen: str
    lines: List[PVLine]


class EvaluateMoveRequest(BaseModel):
    fen: str = Field(..., description="FEN позиции")
    move_uci: str = Field(..., description="Ход в UCI, например e2e4 или e7e8q")
    depth: Optional[int] = Field(None, ge=1, le=MAX_DEPTH)
    movetime_ms: Optional[int] = Field(None, ge=1, le=MAX_MOVETIME_MS)
    side: str = Field("turn", description="turn/white/black (оценка для кого считаем)")


class EvalScore(BaseModel):
    score_cp: Optional[int] = None
    mate: Optional[int] = None


class EvaluateMoveResponse(BaseModel):
    fen: str
    move_uci: str
    legal: bool
    best_move_uci: str
    label: str
    delta_cp: Optional[int] = None
    best_score: EvalScore
    after_score: EvalScore
    best_line: PVLine
    after_line: PVLine


# --- Вспомогательное ---
def _resolve_engine_path(path: str) -> str:
    # 1) Прямой путь
    if os.path.isfile(path):
        return path

    # 2) Поиск в PATH
    found = shutil.which(path)
    if found and os.path.isfile(found):
        return found

    # 3) Под Windows часто забывают .exe
    if os.name == "nt" and not path.lower().endswith(".exe"):
        maybe = path + ".exe"
        if os.path.isfile(maybe):
            return maybe

    raise RuntimeError(
        "Stockfish не найден. Задай STOCKFISH_PATH полным путём до stockfish.exe. "
        f"Сейчас STOCKFISH_PATH='{path}'"
    )


def _pov_color(side: str, board: chess.Board) -> chess.Color:
    s = side.lower().strip()
    if s == "white":
        return chess.WHITE
    if s == "black":
        return chess.BLACK
    return board.turn  # 'turn'


def _build_san_line(board: chess.Board, pv_uci: List[str]) -> List[str]:
    tmp = board.copy()
    san_moves: List[str] = []
    for u in pv_uci:
        try:
            move = chess.Move.from_uci(u)
        except Exception:
            break
        if move not in tmp.legal_moves:
            break
        san_moves.append(tmp.san(move))
        tmp.push(move)
    return san_moves


def _make_limit(depth: int, movetime_ms: int) -> chess.engine.Limit:
    if movetime_ms and movetime_ms > 0:
        return chess.engine.Limit(time=movetime_ms / 1000.0)
    return chess.engine.Limit(depth=depth)


def _sanitize_params(
    depth: Optional[int],
    movetime_ms: Optional[int],
    multipv: Optional[int],
) -> Tuple[int, int, int]:
    d = DEFAULT_DEPTH if depth is None else int(depth)
    t = DEFAULT_MOVETIME_MS if movetime_ms is None else int(movetime_ms)
    m = DEFAULT_MULTIPV if multipv is None else int(multipv)

    if d < 1:
        d = 1
    if t < 0:
        t = 0
    if m < 1:
        m = 1

    if d > MAX_DEPTH:
        d = MAX_DEPTH
    if t > MAX_MOVETIME_MS:
        t = MAX_MOVETIME_MS
    if m > MAX_MULTIPV:
        m = MAX_MULTIPV

    return d, t, m


def _extract_pv_and_score(
    board: chess.Board,
    inf: Dict[str, Any],
    pov: chess.Color,
) -> Tuple[List[str], Optional[int], Optional[int]]:
    # PV
    pv = inf.get("pv")
    if pv is None:
        pv_moves = []
    elif isinstance(pv, (list, tuple)):
        pv_moves = list(pv)
    elif isinstance(pv, chess.Move):
        pv_moves = [pv]
    else:
        pv_moves = []

    pv_uci = [m.uci() for m in pv_moves if isinstance(m, chess.Move)]

    # Score
    score_cp = None
    mate = None
    score = inf.get("score")
    if score is not None:
        try:
            pscore = score.pov(pov)
            mate_v = pscore.mate()
            if mate_v is not None:
                mate = int(mate_v)
            else:
                cp_v = pscore.score()
                score_cp = int(cp_v) if cp_v is not None else None
        except Exception:
            score_cp = None
            mate = None

    return pv_uci, score_cp, mate


def _label_from_delta(delta_cp: Optional[int], is_best: bool) -> str:
    if is_best:
        return "best"
    if delta_cp is None:
        return "ok"  # когда не удалось получить cp
    d = abs(int(delta_cp))
    # простые пороги (можно подкрутить)
    if d <= 20:
        return "excellent"
    if d <= 50:
        return "good"
    if d <= 150:
        return "inaccuracy"
    if d <= 300:
        return "mistake"
    return "blunder"


# --- Cache (простая TTL в памяти) ---
_cache: Dict[str, Tuple[float, Any]] = {}


def _cache_get(key: str):
    if not CACHE_ENABLED:
        return None
    item = _cache.get(key)
    if not item:
        return None
    ts, val = item
    if (time.time() - ts) > CACHE_TTL_SECONDS:
        _cache.pop(key, None)
        return None
    return val


def _cache_set(key: str, val: Any):
    if not CACHE_ENABLED:
        return
    # простая защита от разрастания
    if len(_cache) >= CACHE_MAX_ITEMS:
        # удаляем ~10% самых старых
        oldest = sorted(_cache.items(), key=lambda kv: kv[1][0])[: max(1, CACHE_MAX_ITEMS // 10)]
        for k, _ in oldest:
            _cache.pop(k, None)
    _cache[key] = (time.time(), val)


def _cache_key(prefix: str, **kwargs) -> str:
    parts = [prefix]
    for k in sorted(kwargs.keys()):
        parts.append(f"{k}={kwargs[k]}")
    return "|".join(parts)


# --- Rate limit (очень базовый) ---
_rate: Dict[str, Tuple[float, int]] = {}  # key(ip:bucket) -> (window_start, count)


def _rate_check(ip: str, limit_rpm: int, bucket: str):
    if not RATE_LIMIT_ENABLED:
        return
    now = time.time()
    key = f"{ip}:{bucket}"
    win, cnt = _rate.get(key, (now, 0))
    if (now - win) >= 60:
        win, cnt = now, 0
    cnt += 1
    _rate[key] = (win, cnt)
    if cnt > int(limit_rpm):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")


# --- Пул движков ---
class EnginePool:
    def __init__(self, path: str, size: int):
        self.path = _resolve_engine_path(path)
        self.size = size
        self.queue: asyncio.Queue[chess.engine.SimpleEngine] = asyncio.Queue()
        self._started = False

    async def start(self):
        if self._started:
            return
        for _ in range(self.size):
            engine = chess.engine.SimpleEngine.popen_uci(self.path)
            try:
                engine.configure(ENGINE_OPTIONS)
            except Exception:
                pass
            await self.queue.put(engine)
        self._started = True

    async def stop(self):
        if not self._started:
            return
        while not self.queue.empty():
            engine = await self.queue.get()
            try:
                engine.quit()
            except Exception:
                pass
        self._started = False

    async def acquire(self) -> chess.engine.SimpleEngine:
        return await self.queue.get()

    async def release(self, engine: chess.engine.SimpleEngine):
        await self.queue.put(engine)


pool = EnginePool(STOCKFISH_PATH, ENGINE_POOL_SIZE)


async def _analyze_position(
    board: chess.Board,
    depth: int,
    movetime_ms: int,
    multipv: int,
    side: str,
) -> List[PVLine]:
    key = _cache_key(
        "analyze",
        fen=board.fen(),
        depth=depth,
        movetime_ms=movetime_ms,
        multipv=multipv,
        side=side,
    )
    cached = _cache_get(key)
    if cached is not None:
        return cached

    limit = _make_limit(depth=depth, movetime_ms=movetime_ms)
    pov = _pov_color(side, board)

    engine = await pool.acquire()
    try:
        try:
            engine.configure({"MultiPV": multipv})
        except Exception:
            pass

        info = engine.analyse(board, limit, multipv=multipv)
        infos: List[Dict[str, Any]] = [info] if isinstance(info, dict) else list(info)
        infos = [x for x in infos if isinstance(x, dict)]

        lines: List[PVLine] = []
        for i, inf in enumerate(infos, start=1):
            pv_uci, score_cp, mate = _extract_pv_and_score(board, inf, pov)
            best_move = pv_uci[0] if pv_uci else ""
            lines.append(
                PVLine(
                    rank=i,
                    best_move_uci=best_move,
                    pv_uci=pv_uci,
                    pv_san=_build_san_line(board, pv_uci),
                    score_cp=score_cp,
                    mate=mate,
                )
            )

        _cache_set(key, lines)
        return lines

    finally:
        await pool.release(engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await pool.start()
    yield
    await pool.stop()


app = FastAPI(title="Stockfish Analyze API", lifespan=lifespan)

# --- CORS ---
# Браузер шлёт OPTIONS (preflight) перед POST, если страница не с того же origin.
# Для локальной разработки проще разрешить всё. Для продакшена — ограничь домены.
CORS_ALLOW_ALL = os.getenv("CORS_ALLOW_ALL", "1") == "1"
if CORS_ALLOW_ALL:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    ip = request.client.host if request.client else "unknown"
    path = request.url.path

    if path == "/analyze":
        _rate_check(ip, RATE_LIMIT_ANALYZE_RPM, "analyze")
    elif path == "/evaluate_move":
        _rate_check(ip, RATE_LIMIT_EVAL_RPM, "evaluate_move")
    else:
        _rate_check(ip, RATE_LIMIT_OTHER_RPM, "other")

    return await call_next(request)


@app.exception_handler(Exception)
async def exception_handler(request: Request, exc: Exception):
    # Не превращаем HTTPException в 500
    if isinstance(exc, HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    logger.exception("Unhandled error: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": f"Unhandled error: {type(exc).__name__}: {str(exc)}"},
    )


@app.get("/health")
async def health():
    return {
        "ok": True,
        "pool_size": ENGINE_POOL_SIZE,
        "available": pool.queue.qsize(),
        "engine": pool.path,
        "cache_enabled": CACHE_ENABLED,
        "cache_items": len(_cache),
        "rate_limit_enabled": RATE_LIMIT_ENABLED,
        "rate_limit_analyze_rpm": RATE_LIMIT_ANALYZE_RPM,
        "rate_limit_eval_rpm": RATE_LIMIT_EVAL_RPM,
        "rate_limit_other_rpm": RATE_LIMIT_OTHER_RPM,
    }


@app.post("/analyze", response_model=AnalyzeResponse)
async def analyze(req: AnalyzeRequest):
    try:
        board = chess.Board(req.fen)
    except Exception:
        raise HTTPException(status_code=400, detail="Некорректный FEN")

    depth, movetime_ms, multipv = _sanitize_params(req.depth, req.movetime_ms, req.multipv)

    lines = await _analyze_position(
        board,
        depth=depth,
        movetime_ms=movetime_ms,
        multipv=multipv,
        side=req.side,
    )

    return AnalyzeResponse(fen=req.fen, lines=lines)


@app.post("/evaluate_move", response_model=EvaluateMoveResponse)
async def evaluate_move(req: EvaluateMoveRequest):
    try:
        board = chess.Board(req.fen)
    except Exception:
        raise HTTPException(status_code=400, detail="Некорректный FEN")

    pov = _pov_color(req.side, board)

    try:
        move = chess.Move.from_uci(req.move_uci)
    except Exception:
        raise HTTPException(status_code=400, detail="Некорректный move_uci")

    if move not in board.legal_moves:
        empty = PVLine(rank=1, best_move_uci="", pv_uci=[], pv_san=[], score_cp=None, mate=None)
        return EvaluateMoveResponse(
            fen=req.fen,
            move_uci=req.move_uci,
            legal=False,
            best_move_uci="",
            label="illegal",
            delta_cp=None,
            best_score=EvalScore(score_cp=None, mate=None),
            after_score=EvalScore(score_cp=None, mate=None),
            best_line=empty,
            after_line=empty,
        )

    depth, movetime_ms, _ = _sanitize_params(req.depth, req.movetime_ms, None)

    root_lines = await _analyze_position(
        board, depth=depth, movetime_ms=movetime_ms, multipv=1, side=req.side
    )
    best_line = root_lines[0] if root_lines else PVLine(rank=1, best_move_uci="", pv_uci=[], pv_san=[], score_cp=None, mate=None)
    best_move_uci = best_line.best_move_uci

    side_fixed = "white" if pov == chess.WHITE else "black"

    best_after_score = EvalScore(score_cp=None, mate=None)
    if best_move_uci:
        try:
            bm = chess.Move.from_uci(best_move_uci)
            bb = board.copy()
            if bm in bb.legal_moves:
                bb.push(bm)
                bl = await _analyze_position(bb, depth=depth, movetime_ms=movetime_ms, multipv=1, side=side_fixed)
                if bl:
                    best_after_score = EvalScore(score_cp=bl[0].score_cp, mate=bl[0].mate)
        except Exception:
            pass

    ba = board.copy()
    ba.push(move)
    al = await _analyze_position(ba, depth=depth, movetime_ms=movetime_ms, multipv=1, side=side_fixed)
    after_line = al[0] if al else PVLine(rank=1, best_move_uci="", pv_uci=[], pv_san=[], score_cp=None, mate=None)
    after_score = EvalScore(score_cp=after_line.score_cp, mate=after_line.mate)

    is_best = req.move_uci == best_move_uci
    delta_cp = 0 if is_best else None
    if (
        not is_best
        and best_after_score.mate is None
        and after_score.mate is None
        and best_after_score.score_cp is not None
        and after_score.score_cp is not None
    ):
        delta_cp = int(best_after_score.score_cp - after_score.score_cp)

    label = _label_from_delta(delta_cp, is_best=is_best)

    return EvaluateMoveResponse(
        fen=req.fen,
        move_uci=req.move_uci,
        legal=True,
        best_move_uci=best_move_uci,
        label=label,
        delta_cp=delta_cp,
        best_score=best_after_score,
        after_score=after_score,
        best_line=best_line,
        after_line=after_line,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)

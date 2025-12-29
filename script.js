// --- White pieces: local SVGs (relative to this HTML file) ---
// IMPORTANT: in URLs use forward slashes: icone/white/...
const WHITE_SVG = {
  P: 'icone/white/Chess_plt45.svg',
  N: 'icone/white/Chess_nlt45.svg',
  B: 'icone/white/Chess_blt45.svg',
  R: 'icone/white/Chess_rlt45.svg',
  Q: 'icone/white/Chess_qlt45.svg',
  K: 'icone/white/Chess_klt45.svg'
};

// --- Black pieces: local SVGs (relative to this HTML file) ---
// IMPORTANT: in URLs use forward slashes: icone/black/...
const BLACK_SVG = {
  p: 'icone/black/Chess_pdt45.svg',
  n: 'icone/black/Chess_ndt45.svg',
  b: 'icone/black/Chess_bdt45.svg',
  r: 'icone/black/Chess_rdt45.svg',
  q: 'icone/black/Chess_qdt45.svg',
  k: 'icone/black/Chess_kdt45.svg'
};

// Default: empty board while ждем задачу
const START_FEN = '8/8/8/8/8/8/8/8 w - - 0 1';

const tg = window.Telegram?.WebApp;

const moveSound = new Audio('audio/chess-move.ogg');
const checkSound = new Audio('audio/chess-check.ogg');
const HISTORY_STORAGE_KEY = 'chess-miniapp-history';
const PUZZLE_STORAGE_KEY = 'chess-miniapp-current-puzzle';
const SOUND_STORAGE_KEY = 'chess-miniapp-sound-enabled';
const PALETTE_STORAGE_KEY = 'chess-miniapp-board-palette';

let flipped = false;
let boardState = fenToBoard(START_FEN);
let activeColor = 'w';
let castlingRights = { w: { K: true, Q: true }, b: { K: true, Q: true } };

let selectedSquare = null;
let highlightedMoves = [];
let promotionState = null;
let puzzleData = null;
let puzzleMode = false;
let puzzleSolutionMoves = [];
let puzzleMoveIndex = 0;
let puzzleSolved = false;
let puzzleStartFen = null;
let puzzlePlayerColor = null;
let puzzleSolutionTargetFen = null;
let puzzleLoading = false;
let moveHistory = [];
let historyStartFen = START_FEN;
let soundEnabled = loadSoundPreference();
let puzzleLockedAfterError = false;

function getExpectedMoveColor(moveIndex){
  const opponentColor = puzzlePlayerColor === 'w' ? 'b' : 'w';
  return moveIndex % 2 === 0 ? puzzlePlayerColor : opponentColor;
}

const files = ['a','b','c','d','e','f','g','h'];
const ranks = ['8','7','6','5','4','3','2','1'];

const boardEl = document.getElementById('board');
const fenOutEl = document.getElementById('fenOut');
const statusEl = document.getElementById('status');
const promotionOverlay = document.getElementById('promotionOverlay');
const promotionButtons = Array.from(promotionOverlay?.querySelectorAll('.promotion-btn') || []);
const filesBottomEl = document.getElementById('filesBottom');
const ranksLeftEl = document.getElementById('ranksLeft');
const puzzleStatusEl = document.getElementById('puzzleStatus');
const analyzeBtn = document.getElementById('analyzeBtn');
const puzzleTitleEl = document.getElementById('puzzleTitle');
const puzzleUrlEl = document.getElementById('puzzleUrl');
const puzzlePublishEl = document.getElementById('puzzlePublish');
const puzzleFenEl = document.getElementById('puzzleFen');
const puzzlePgnEl = document.getElementById('puzzlePgn');
const puzzleImageEl = document.getElementById('puzzleImage');
const puzzleFeedbackEl = document.getElementById('puzzleFeedback');
const puzzleOverlayEl = document.getElementById('puzzleOverlay');
const puzzleOverlayTitleEl = document.getElementById('puzzleOverlayTitle');
const puzzleOverlayActionsEl = document.getElementById('puzzleOverlayActions');
const analysisOverlayEl = document.getElementById('analysisOverlay');
const analysisFrameEl = document.getElementById('analysisFrame');
const closeAnalysisBtn = document.getElementById('closeAnalysisBtn');
const settingsDetailsEl = document.querySelector('.floating-settings details');
const openSettingsPageBtn = document.getElementById('openSettingsPage');
const boardTitleEl = document.getElementById('boardTitle');

const defaultTheme = {
  bg: '#111',
  panel: 'rgba(255,255,255,.06)',
  border: 'rgba(255,255,255,.12)',
  text: '#fff',
  dark: '#769656',
  light: '#eeeed2'
};
const boardPalettes = {
  default: { dark: '#769656', light: '#eeeed2' },
  classic: { dark: '#8cb369', light: '#f2ead3' },
  coffee: { dark: '#8b5e3c', light: '#d7c9aa' },
  sea: { dark: '#466b8a', light: '#e9ddc5' },
  warm: { dark: '#b83c3c', light: '#f0e7d8' }
};

function getBoardPalette(name){
  return boardPalettes[name] || boardPalettes.default;
}

function preventZoom(){
  // Avoid pinch and double-tap zooming inside the Telegram webview.
  document.addEventListener('touchstart', (event) => {
    if (event.touches.length > 1) {
      event.preventDefault();
    }
  }, { passive: false });

  let lastTouchEnd = 0;
  document.addEventListener('touchend', (event) => {
    const now = Date.now();
    if (now - lastTouchEnd <= 350) {
      event.preventDefault();
    }
    lastTouchEnd = now;
  }, { passive: false });

  ['gesturestart', 'gesturechange', 'gestureend'].forEach((type) => {
    document.addEventListener(type, (event) => event.preventDefault());
  });
}

function loadSoundPreference(){
  const stored = localStorage.getItem(SOUND_STORAGE_KEY);
  if (stored === null) return true;
  return stored !== 'false' && stored !== '0';
}

function loadPalettePreference(){
  const stored = localStorage.getItem(PALETTE_STORAGE_KEY);
  return boardPalettes[stored] ? stored : 'default';
}

function applyBoardPalette(name){
  const palette = getBoardPalette(name);
  const root = document.documentElement;
  root.style.setProperty('--dark', palette.dark);
  root.style.setProperty('--light', palette.light);
}

function applyTelegramTheme(){
  if (!tg) return;
  const theme = tg.themeParams || {};
  const root = document.documentElement;
  root.style.setProperty('--bg', theme.bg_color || defaultTheme.bg);
  root.style.setProperty('--panel', theme.secondary_bg_color || defaultTheme.panel);
  root.style.setProperty('--border', theme.section_separator_color || defaultTheme.border);
  root.style.setProperty('--text', theme.text_color || defaultTheme.text);
  // Keep board colors stable across platforms.
  applyBoardPalette(loadPalettePreference());
}

function initTelegram(){
  if (!tg) return;
  tg.ready();
  tg.expand();
  applyTelegramTheme();
  tg.onEvent('themeChanged', applyTelegramTheme);
}

function setPromotionIcons(color){
  const iconMap = color === 'w' ? WHITE_SVG : BLACK_SVG;
  promotionButtons.forEach(btn => {
    const pieceCode = btn.dataset.piece;
    const key = color === 'w' ? pieceCode.toUpperCase() : pieceCode;
    const img = btn.querySelector('img');
    if (img) img.src = iconMap[key] || '';
  });
}

function openPromotionDialog(color){
  if (!promotionOverlay) return;
  setPromotionIcons(color);
  promotionOverlay.classList.add('active');
}

function closePromotionDialog(){
  if (!promotionOverlay) return;
  promotionOverlay.classList.remove('active');
}

function openPuzzleOverlay({ title = '', actions = [], variant = '' } = {}){
  if (!puzzleOverlayEl || !puzzleOverlayTitleEl || !puzzleOverlayActionsEl) return;
  puzzleOverlayTitleEl.textContent = title;
  puzzleOverlayActionsEl.innerHTML = '';
  actions.forEach(action => puzzleOverlayActionsEl.appendChild(action));
  puzzleOverlayEl.classList.toggle('solved', variant === 'solved');
  puzzleOverlayEl.classList.add('active');
}

function closePuzzleOverlay(){
  if (!puzzleOverlayEl || !puzzleOverlayTitleEl || !puzzleOverlayActionsEl) return;
  puzzleOverlayEl.classList.remove('active');
  puzzleOverlayEl.classList.remove('solved');
  puzzleOverlayTitleEl.textContent = '';
  puzzleOverlayActionsEl.innerHTML = '';
}

function fenToBoard(fen){
  // returns 8x8 array; each cell is piece char or ''
  const placement = fen.split(' ')[0];
  const rows = placement.split('/');
  if (rows.length !== 8) throw new Error('Bad FEN');
  const b = [];
  for (const r of rows){
    const row = [];
    for (const ch of r){
      if (/[1-8]/.test(ch)){
        for (let i=0;i<Number(ch);i++) row.push('');
      } else {
        row.push(ch);
      }
    }
    if (row.length !== 8) throw new Error('Bad FEN row');
    b.push(row);
  }
  return b;
}

function parseFenState(fen){
  const parts = fen.split(' ');
  const board = fenToBoard(fen);
  const active = parts[1] === 'b' ? 'b' : 'w';
  const castlingPart = parts[2] || '-';
  const castling = { w: { K: false, Q: false }, b: { K: false, Q: false } };
  if (castlingPart && castlingPart !== '-'){
    for (const ch of castlingPart){
      if (ch === 'K') castling.w.K = true;
      if (ch === 'Q') castling.w.Q = true;
      if (ch === 'k') castling.b.K = true;
      if (ch === 'q') castling.b.Q = true;
    }
  }
  return { board, active, castling };
}

function loadPositionFromFen(fen, options = {}){
  const { preservePuzzleProgress = false } = options;
  const parsed = parseFenState(fen);
  boardState = parsed.board;
  activeColor = parsed.active;
  castlingRights = parsed.castling;
  resetMoveHistory(fen);
  const hasSolution = puzzleSolutionMoves.length > 0;
  if (!preservePuzzleProgress){
    puzzlePlayerColor = parsed.active;
    puzzleMode = hasSolution;
    puzzleSolved = false;
    puzzleMoveIndex = 0;
    puzzleLockedAfterError = false;
  } else {
    puzzleMode = puzzleMode && hasSolution;
    puzzlePlayerColor = puzzlePlayerColor || parsed.active;
  }
  promotionState = null;
  resetSelection();
  closePromotionDialog();
  closePuzzleOverlay();
  render();
  updatePuzzleStatus();
  persistPuzzleState();
}

function boardToFen(b){
  // only placement part (enough for visual)
  const rows = b.map(row => {
    let out = '';
    let empties = 0;
    for (const cell of row){
      if (!cell){
        empties++;
      } else {
        if (empties){ out += String(empties); empties = 0; }
        out += cell;
      }
    }
    if (empties) out += String(empties);
    return out;
  });
  const castle = getCastlingFen();
  return rows.join('/') + ` ${activeColor} ${castle} - 0 1`;
}

function getCastlingFen(){
  let out = '';
  if (castlingRights.w.K) out += 'K';
  if (castlingRights.w.Q) out += 'Q';
  if (castlingRights.b.K) out += 'k';
  if (castlingRights.b.Q) out += 'q';
  return out || '-';
}

function persistPuzzleState(){
  try{
    const payload = {
      puzzleData,
      puzzleMode,
      puzzleSolutionMoves,
      puzzleMoveIndex,
      puzzleSolved,
      puzzleStartFen,
      puzzlePlayerColor,
      puzzleSolutionTargetFen,
      boardFen: boardToFen(boardState),
      castlingRights,
      activeColor,
      moveHistory,
      historyStartFen,
      puzzleLockedAfterError
    };
    localStorage.setItem(PUZZLE_STORAGE_KEY, JSON.stringify(payload));
  } catch (err){
    console.warn('Не удалось сохранить состояние задачи', err);
  }
}

function persistMoveHistory(){
  try{
    const payload = { startFen: historyStartFen, moves: moveHistory };
    localStorage.setItem(HISTORY_STORAGE_KEY, JSON.stringify(payload));
  } catch (err){
    console.warn('Не удалось сохранить историю ходов', err);
  }
}

function resetMoveHistory(startFen){
  historyStartFen = startFen || boardToFen(boardState);
  moveHistory = [];
  persistMoveHistory();
}

function recordMoveToHistory({ fromR, fromC, toR, toC, promotionPiece = null, fenBefore, fenAfter }){
  if (!fenAfter) return;
  const fromSq = coordToNotation(fromR, fromC);
  const toSq = coordToNotation(toR, toC);
  const promo = promotionPiece ? promotionPiece.toLowerCase() : '';
  const uci = `${fromSq}${toSq}${promo}`;
  let san = null;
  if (typeof Chess === 'function'){
    try{
      const chess = new Chess(fenBefore || historyStartFen);
      const move = chess.move({ from: fromSq, to: toSq, promotion: promo || undefined });
      san = move?.san || null;
    } catch (err){
      console.warn('Не удалось сконвертировать ход в SAN', err);
    }
  }
  moveHistory.push({
    san: san || uci,
    uci,
    fen: fenAfter,
    active: fenAfter.split(' ')[1] === 'b' ? 'b' : 'w'
  });
  persistMoveHistory();
}

function coordToDisplay(r,c){
  // apply flip mapping
  if (!flipped) return { r, c };
  return { r: 7 - r, c: 7 - c };
}

function displayToCoord(dr,dc){
  // reverse mapping
  if (!flipped) return { r: dr, c: dc };
  return { r: 7 - dr, c: 7 - dc };
}

function getDisplayFiles(){
  const files = ['a','b','c','d','e','f','g','h'];
  return flipped ? [...files].reverse() : files;
}

function getDisplayRanks(){
  const ranks = ['8','7','6','5','4','3','2','1'];
  return flipped ? [...ranks].reverse() : ranks;
}

function isWhite(piece){ return piece === piece.toUpperCase(); }
function isBlack(piece){ return piece === piece.toLowerCase(); }

function isPathClear(fromR, fromC, toR, toC, board = boardState){
  const stepR = Math.sign(toR - fromR);
  const stepC = Math.sign(toC - fromC);
  let r = fromR + stepR;
  let c = fromC + stepC;
  while (r !== toR || c !== toC){
    if (board[r][c]) return false;
    r += stepR;
    c += stepC;
  }
  return true;
}

function isLegalPawnMove(piece, fromR, fromC, toR, toC, board = boardState){
  const dir = piece === 'P' ? -1 : 1;
  const startRow = piece === 'P' ? 6 : 1;
  const target = board[toR][toC];
  const forward = fromC === toC && target === '';
  const doubleForward = fromC === toC && target === '' && fromR === startRow && toR === fromR + 2*dir && board[fromR + dir][fromC] === '';
  const capture = Math.abs(toC - fromC) === 1 && toR === fromR + dir && target && ((isWhite(piece) && isBlack(target)) || (isBlack(piece) && isWhite(target)));
  return (forward && toR === fromR + dir) || doubleForward || capture;
}

function isLegalKnightMove(fromR, fromC, toR, toC){
  const dr = Math.abs(toR - fromR);
  const dc = Math.abs(toC - fromC);
  return dr * dc === 2;
}

function isLegalBishopMove(fromR, fromC, toR, toC, board = boardState){
  if (Math.abs(toR - fromR) !== Math.abs(toC - fromC)) return false;
  return isPathClear(fromR, fromC, toR, toC, board);
}

function isLegalRookMove(fromR, fromC, toR, toC, board = boardState){
  if (fromR !== toR && fromC !== toC) return false;
  return isPathClear(fromR, fromC, toR, toC, board);
}

function isLegalQueenMove(fromR, fromC, toR, toC, board = boardState){
  return isLegalRookMove(fromR, fromC, toR, toC, board) || isLegalBishopMove(fromR, fromC, toR, toC, board);
}

function isLegalKingMove(fromR, fromC, toR, toC, board = boardState, rights = castlingRights, { allowCastling = true } = {}){
  const dr = Math.abs(toR - fromR);
  const dc = Math.abs(toC - fromC);
  if (dr <= 1 && dc <= 1) return true;
  if (!allowCastling) return false;
  const piece = board[fromR][fromC];
  const color = isWhite(piece) ? 'w' : 'b';
  if (dr === 0 && dc === 2){
    const side = toC > fromC ? 'K' : 'Q';
    return canCastle(color, side, board, rights);
  }
  return false;
}

function isLegalMove(fromR, fromC, toR, toC, board = boardState, rights = castlingRights, opts = {}){
  const { allowCastling = true } = opts;
  if (fromR === toR && fromC === toC) return false;
  if (toR < 0 || toR > 7 || toC < 0 || toC > 7) return false;

  const piece = board[fromR][fromC];
  if (!piece) return false;

  const target = board[toR][toC];
  if (target){
    if (isWhite(piece) === isWhite(target)) return false;
  }

  switch (piece.toLowerCase()){
    case 'p':
      return isLegalPawnMove(piece, fromR, fromC, toR, toC, board);
    case 'n':
      return isLegalKnightMove(fromR, fromC, toR, toC);
    case 'b':
      return isLegalBishopMove(fromR, fromC, toR, toC, board);
    case 'r':
      return isLegalRookMove(fromR, fromC, toR, toC, board);
    case 'q':
      return isLegalQueenMove(fromR, fromC, toR, toC, board);
    case 'k':
      return isLegalKingMove(fromR, fromC, toR, toC, board, rights, { allowCastling });
    default:
      return false;
  }
}

function cloneBoard(board){
  return board.map(row => [...row]);
}

function isSquareAttacked(board, targetR, targetC, attackerColor, rights = castlingRights){
  for (let r=0; r<8; r++){
    for (let c=0; c<8; c++){
      const piece = board[r][c];
      if (!piece) continue;
      if (attackerColor === 'w' && !isWhite(piece)) continue;
      if (attackerColor === 'b' && !isBlack(piece)) continue;
      if (isLegalMove(r, c, targetR, targetC, board, rights, { allowCastling: false })) return true;
    }
  }
  return false;
}

function getKingPosition(board, color){
  const kingChar = color === 'w' ? 'K' : 'k';
  for (let r=0; r<8; r++){
    for (let c=0; c<8; c++){
      if (board[r][c] === kingChar){
        return { r, c };
      }
    }
  }
  return null;
}

function isKingInCheck(board, color, rights = castlingRights){
  const kingPos = getKingPosition(board, color);
  if (!kingPos) return false;
  const attacker = color === 'w' ? 'b' : 'w';
  return isSquareAttacked(board, kingPos.r, kingPos.c, attacker, rights);
}

function canCastle(color, side, board = boardState, rights = castlingRights){
  const rightsForColor = rights[color];
  if (!rightsForColor) return false;
  if (side === 'K' && !rightsForColor.K) return false;
  if (side === 'Q' && !rightsForColor.Q) return false;

  const row = color === 'w' ? 7 : 0;
  const kingCol = 4;
  const rookCol = side === 'K' ? 7 : 0;
  const king = color === 'w' ? 'K' : 'k';
  const rook = color === 'w' ? 'R' : 'r';
  if (board[row][kingCol] !== king || board[row][rookCol] !== rook) return false;

  const throughCols = side === 'K' ? [5,6] : [3,2];
  if (!isPathClear(row, kingCol, row, rookCol, board)) return false;
  if (isKingInCheck(board, color, rights)) return false;
  const opponent = color === 'w' ? 'b' : 'w';
  for (const col of throughCols){
    if (isSquareAttacked(board, row, col, opponent, rights)) return false;
  }
  return true;
}

function isCastlingMove(piece, fromR, fromC, toR, toC){
  if (piece.toLowerCase() !== 'k') return false;
  if (fromR !== toR) return false;
  return Math.abs(toC - fromC) === 2;
}

function moveLeavesKingInCheck(fromR, fromC, toR, toC, board = boardState, rights = castlingRights){
  const piece = board[fromR][fromC];
  const movingColor = isWhite(piece) ? 'w' : 'b';
  const next = cloneBoard(board);
  const nextRights = JSON.parse(JSON.stringify(rights));
  if (isCastlingMove(piece, fromR, fromC, toR, toC)){
    const isKingSide = toC > fromC;
    const rookFromC = isKingSide ? 7 : 0;
    const rookToC = isKingSide ? 5 : 3;
    next[toR][toC] = piece;
    next[fromR][fromC] = '';
    next[fromR][rookFromC] = '';
    next[fromR][rookToC] = isWhite(piece) ? 'R' : 'r';
  } else {
    next[toR][toC] = piece;
    next[fromR][fromC] = '';
  }
  return isKingInCheck(next, movingColor, nextRights);
}

function isMoveAllowed(fromR, fromC, toR, toC){
  if (!isLegalMove(fromR, fromC, toR, toC, boardState, castlingRights)) return false;
  return !moveLeavesKingInCheck(fromR, fromC, toR, toC, boardState, castlingRights);
}

function getLegalMovesForPiece(fromR, fromC, color = activeColor){
  const piece = boardState[fromR][fromC];
  if (!piece) return [];
  if (color === 'w' && isBlack(piece)) return [];
  if (color === 'b' && isWhite(piece)) return [];

  const moves = [];
  for (let r=0; r<8; r++){
    for (let c=0; c<8; c++){
      if (isMoveAllowed(fromR, fromC, r, c)){
        moves.push({ r, c });
      }
    }
  }
  return moves;
}

function playerHasLegalMoves(color){
  for (let r=0; r<8; r++){
    for (let c=0; c<8; c++){
      const piece = boardState[r][c];
      if (!piece) continue;
      if (color === 'w' && !isWhite(piece)) continue;
      if (color === 'b' && !isBlack(piece)) continue;
      if (getLegalMovesForPiece(r, c, color).length) return true;
    }
  }
  return false;
}

function playSound(sound){
  if (!soundEnabled || !sound) return;
  try {
    sound.currentTime = 0;
    sound.play().catch(() => {});
  } catch (_) {}
}

function playMoveAudio(){
  const inCheck = isKingInCheck(boardState, activeColor);
  const hasMoves = playerHasLegalMoves(activeColor);
  const isMate = inCheck && !hasMoves;
  if (isMate || inCheck){
    playSound(checkSound);
  } else {
    playSound(moveSound);
  }
}

function resetSelection(){
  selectedSquare = null;
  highlightedMoves = [];
}

function selectSquare(r, c){
  selectedSquare = { r, c };
  const fromPiece = boardState[r][c];
  highlightedMoves = getLegalMovesForPiece(r, c).map(move => {
    const targetPiece = boardState[move.r][move.c];
    const isCapture = Boolean(targetPiece) && ((isWhite(fromPiece) && isBlack(targetPiece)) || (isBlack(fromPiece) && isWhite(targetPiece)));
    return { ...move, capture: isCapture };
  });
}

function handleSquareTap(r, c){
  if (puzzleMode && puzzleLockedAfterError){
    updatePuzzleFeedback('wrong', 'Нажмите «Решить заново» или «Новая задача».', { withActions: true });
    return;
  }
  if (puzzleMode && !puzzleSolved && activeColor !== puzzlePlayerColor){
    return;
  }

  if (selectedSquare && highlightedMoves.some(m => m.r === r && m.c === c)){
    performMove(selectedSquare.r, selectedSquare.c, r, c);
    return;
  }

  const piece = boardState[r][c];
  if (piece && ((activeColor === 'w' && isWhite(piece)) || (activeColor === 'b' && isBlack(piece)))){
    selectSquare(r, c);
  } else {
    resetSelection();
  }
  render();
}

function updateStatus(){
  if (!statusEl) return;
  const hasPieces = boardState.some(row => row.some(Boolean));
  if (!hasPieces){
    statusEl.textContent = 'Загружаем задачу...';
    statusEl.classList.remove('mate');
    return;
  }
  const inCheck = isKingInCheck(boardState, activeColor);
  const hasMoves = playerHasLegalMoves(activeColor);
  if (inCheck && !hasMoves){
    const loser = activeColor === 'w' ? 'белым' : 'черным';
    const winner = activeColor === 'w' ? 'Черные' : 'Белые';
    statusEl.textContent = `Мат ${loser}. ${winner} победили.`;
    statusEl.classList.add('mate');
    return;
  }
  statusEl.classList.remove('mate');
  if (!inCheck && !hasMoves){
    statusEl.textContent = 'Пат. Ничья.';
    return;
  }
  if (inCheck){
    statusEl.textContent = `Шах ${activeColor === 'w' ? 'белым' : 'черным'}.`;
    return;
  }
  statusEl.textContent = `Ход ${activeColor === 'w' ? 'белых' : 'черных'}.`;
}

function updatePuzzleStatus(){
  if (!puzzleStatusEl) return;
  if (puzzleLoading){
    puzzleStatusEl.textContent = 'Загрузка задачи...';
    return;
  }
  if (puzzleSolved){
    puzzleStatusEl.textContent = 'Задача решена.';
    return;
  }
  if (puzzleMode && puzzleData){
    puzzleStatusEl.textContent = `Режим задачи: ход ${activeColor === 'w' ? 'белых' : 'черных'}.`;
    return;
  }
  if (puzzleData){
    puzzleStatusEl.textContent = 'Задача загружена.';
    return;
  }
  puzzleStatusEl.textContent = '';
}

function formatPublishTime(ts){
  if (!ts) return '';
  const date = new Date(Number(ts) * 1000);
  if (Number.isNaN(date.getTime())) return String(ts);
  return date.toLocaleString('ru-RU');
}

function updatePuzzleInfoDisplay(data){
  puzzleData = data;
  puzzleStartFen = data?.fen || null;
  if (puzzleTitleEl) puzzleTitleEl.textContent = data?.title || '';
  if (puzzleUrlEl){
    puzzleUrlEl.textContent = data?.url || '';
    puzzleUrlEl.href = data?.url || '#';
  }
  if (puzzlePublishEl) puzzlePublishEl.textContent = data?.publish_time ? formatPublishTime(data.publish_time) : '';
  if (puzzleFenEl) puzzleFenEl.textContent = data?.fen || '';
  if (puzzlePgnEl) puzzlePgnEl.textContent = data?.pgn || '';
  if (puzzleImageEl){
    puzzleImageEl.textContent = data?.image || '';
    puzzleImageEl.href = data?.image || '#';
  }
  const solution = parseSolutionMovesFromPgn(data?.pgn || '', puzzleStartFen);
  puzzleSolutionMoves = solution.moves;
  puzzleSolutionTargetFen = solution.finalFen;
  puzzleMoveIndex = 0;
  puzzleSolved = false;
  if (solution.error){
    updatePuzzleFeedback('error', solution.error);
  } else if (puzzleSolutionMoves.length){
    const startColor = (puzzleStartFen?.split(' ')[1] === 'b') ? 'b' : 'w';
    const playerColorLabel = startColor === 'b' ? 'черными' : 'белыми';
    updatePuzzleFeedback('idle');
  } else {
    updatePuzzleFeedback('idle');
  }
  updatePuzzleStatus();
}

function stateFromFen(fen){
  const parsed = parseFenState(fen || START_FEN);
  return {
    board: parsed.board,
    active: parsed.active,
    castling: parsed.castling
  };
}

function cloneState(state){
  return {
    board: cloneBoard(state.board),
    active: state.active,
    castling: JSON.parse(JSON.stringify(state.castling || {}))
  };
}

function castlingToFen(rights){
  let out = '';
  if (rights?.w?.K) out += 'K';
  if (rights?.w?.Q) out += 'Q';
  if (rights?.b?.K) out += 'k';
  if (rights?.b?.Q) out += 'q';
  return out || '-';
}

function stateToFen(state){
  const rows = state.board.map(row => {
    let out = '';
    let empties = 0;
    for (const cell of row){
      if (!cell){
        empties++;
      } else {
        if (empties){ out += String(empties); empties = 0; }
        out += cell;
      }
    }
    if (empties) out += String(empties);
    return out;
  });
  return `${rows.join('/')}` + ` ${state.active} ${castlingToFen(state.castling)} - 0 1`;
}

function isMoveAllowedInState(state, fromR, fromC, toR, toC){
  if (!isLegalMove(fromR, fromC, toR, toC, state.board, state.castling)) return false;
  return !moveLeavesKingInCheck(fromR, fromC, toR, toC, state.board, state.castling);
}

function getLegalMovesForPieceInState(state, fromR, fromC){
  const piece = state.board[fromR][fromC];
  if (!piece) return [];
  if (state.active === 'w' && isBlack(piece)) return [];
  if (state.active === 'b' && isWhite(piece)) return [];

  const moves = [];
  for (let r=0; r<8; r++){
    for (let c=0; c<8; c++){
      if (isMoveAllowedInState(state, fromR, fromC, r, c)){
        moves.push({ r, c });
      }
    }
  }
  return moves;
}

function applyMoveToState(state, { fromR, fromC, toR, toC, promotionPiece = null }){
  const piece = state.board[fromR][fromC];
  updateCastlingRightsForState(state, fromR, fromC, toR, toC, piece);

  const normalizedPromotion = promotionPiece
    ? (isWhite(piece) ? promotionPiece.toUpperCase() : promotionPiece.toLowerCase())
    : null;
  const pieceToPlace = normalizedPromotion || piece;
  if (isCastlingMove(piece, fromR, fromC, toR, toC)){
    const isKingSide = toC > fromC;
    const rookFromC = isKingSide ? 7 : 0;
    const rookToC = isKingSide ? 5 : 3;
    state.board[fromR][fromC] = '';
    state.board[toR][toC] = piece;
    state.board[fromR][rookFromC] = '';
    state.board[fromR][rookToC] = isWhite(piece) ? 'R' : 'r';
  } else {
    state.board[fromR][fromC] = '';
    state.board[toR][toC] = pieceToPlace;
  }

  state.active = state.active === 'w' ? 'b' : 'w';
}

function updateCastlingRightsForState(state, fromR, fromC, toR, toC, piece){
  const rights = state.castling;
  const pieceColor = isWhite(piece) ? 'w' : 'b';
  if (piece.toLowerCase() === 'k'){
    rights[pieceColor].K = false;
    rights[pieceColor].Q = false;
  }
  if (piece.toLowerCase() === 'r'){
    if (pieceColor === 'w'){
      if (fromR === 7 && fromC === 0) rights.w.Q = false;
      if (fromR === 7 && fromC === 7) rights.w.K = false;
    } else {
      if (fromR === 0 && fromC === 0) rights.b.Q = false;
      if (fromR === 0 && fromC === 7) rights.b.K = false;
    }
  }

  const target = state.board[toR][toC];
  if (target && target.toLowerCase() === 'r'){
    const targetColor = isWhite(target) ? 'w' : 'b';
    if (targetColor === 'w'){
      if (toR === 7 && toC === 0) rights.w.Q = false;
      if (toR === 7 && toC === 7) rights.w.K = false;
    } else {
      if (toR === 0 && toC === 0) rights.b.Q = false;
      if (toR === 0 && toC === 7) rights.b.K = false;
    }
  }
}

function tokenizePgnMoves(pgn){
  const withoutHeaders = pgn.replace(/^\s*\[[^\]]+\]\s*$/gm, '');
  const withoutComments = withoutHeaders
    .replace(/\{[^}]*\}/g, ' ')
    .replace(/;[^\n]*/g, ' ')
    .replace(/\([^)]*\)/g, ' ');
  const tokens = withoutComments
    .replace(/\d+\.\.\./g, ' ')
    .replace(/\d+\./g, ' ')
    .split(/\s+/)
    .map(t => t.trim())
    .filter(Boolean);
  return tokens;
}

function sanToMoveKey(state, san){
  const fail = (msg) => ({ ok: false, error: msg, moveKey: null });
  if (!san) return fail('Пустой ход в PGN.');

  const cleaned = san.replace(/[+#]+/g, '').replace(/[!?]+/g, '');
  if (/^(1-0|0-1|1\/2-1\/2|\*)$/.test(cleaned)){
    return fail('Достигнут конец партии до окончания решения.');
  }

  if (/^O-O(-O)?$/i.test(cleaned)){
    const isQueenSide = /O-O-O/i.test(cleaned);
    const row = state.active === 'w' ? 7 : 0;
    const kingFromC = 4;
    const kingToC = isQueenSide ? 2 : 6;
    const fromR = row;
    const fromC = kingFromC;
    const toR = row;
    const toC = kingToC;
    const moveKey = buildMoveKey({ fromR, fromC, toR, toC });
    const moveAllowed = isMoveAllowedInState(state, fromR, fromC, toR, toC);
    return moveAllowed ? { ok: true, moveKey } : fail(`Рокировка из PGN невозможна: ${san}`);
  }

  const promotionMatch = cleaned.match(/=([NBRQ])/i);
  const promotionPiece = promotionMatch ? promotionMatch[1].toLowerCase() : null;
  const base = cleaned.replace(/=([NBRQ])/i, '');
  const capture = base.includes('x');

  const pieceLetter = /^[KQRBN]/.test(base) ? base[0] : 'P';
  const rest = pieceLetter === 'P' ? base : base.slice(1);
  const noCaptureRest = rest.replace('x', '');
  const target = noCaptureRest.slice(-2);
  const disambig = noCaptureRest.slice(0, -2);

  const targetC = files.indexOf(target[0]);
  const targetR = ranks.indexOf(target[1]);
  if (targetC === -1 || targetR === -1) return fail(`Не удалось понять целевое поле в ходе ${san}`);

  const legalMoves = [];
  for (let r=0; r<8; r++){
    for (let c=0; c<8; c++){
      const piece = state.board[r][c];
      if (!piece) continue;
      const pieceColor = isWhite(piece) ? 'w' : 'b';
      if (pieceColor !== state.active) continue;
      const normalized = piece.toUpperCase();
      if (pieceLetter === 'P' && normalized !== 'P') continue;
      if (pieceLetter !== 'P' && normalized !== pieceLetter) continue;
      if (!isMoveAllowedInState(state, r, c, targetR, targetC)) continue;
      const promotionNeeded = needsPromotion(piece, targetR);
      const moveKey = buildMoveKey({ fromR: r, fromC: c, toR: targetR, toC: targetC, promotionPiece: promotionPiece || undefined });
      const moveCaptures = Boolean(state.board[targetR][targetC]);
      if (capture && !moveCaptures) continue;
      if (!capture && moveCaptures && pieceLetter === 'P') continue;
      if (disambig){
        if (disambig.length === 2){
          if (files.indexOf(disambig[0]) !== c) continue;
          if (ranks.indexOf(disambig[1]) !== r) continue;
        } else if (/[a-h]/.test(disambig)){
          if (files.indexOf(disambig) !== c) continue;
        } else if (/[1-8]/.test(disambig)){
          if (ranks.indexOf(disambig) !== r) continue;
        }
      }
      if (promotionNeeded && !promotionPiece) continue;
      legalMoves.push({ fromR: r, fromC: c, toR: targetR, toC: targetC, promotionPiece: promotionPiece || null, moveKey });
    }
  }

  if (!legalMoves.length) return fail(`Не найден допустимый ход для SAN: ${san}`);
  if (legalMoves.length > 1) return fail(`Ход неоднозначен в SAN: ${san}`);
  const move = legalMoves[0];
  return { ok: true, moveKey: move.moveKey, move };
}

function parseSolutionMovesFromPgn(pgn, startFen = null){
  const fail = (msg) => ({ moves: [], error: msg, finalFen: null });
  if (!pgn) return fail('В ответе задачи нет PGN с решением.');

  const fenFromTag = (pgn.match(/\[FEN\s+"([^"]+)"\]/i) || [])[1];
  const initialFen = fenFromTag || startFen || START_FEN;
  let state = stateFromFen(initialFen);

  const tokens = tokenizePgnMoves(pgn);
  const moves = [];

  for (const token of tokens){
    if (/^(1-0|0-1|1\/2-1\/2|\*)$/.test(token)) break;
    const parsed = sanToMoveKey(state, token);
    if (!parsed.ok){
      return fail(parsed.error);
    }
    moves.push(parsed.moveKey);
    applyMoveToState(state, parsed.move);
  }

  if (!moves.length) return fail('PGN не содержит ходов решения.');

  return { moves, error: null, finalFen: stateToFen(state) };
}

function coordToNotation(r, c){
  return `${files[c]}${ranks[r]}`;
}

function buildMoveKey({ fromR, fromC, toR, toC, promotionPiece = null }){
  const promo = promotionPiece ? promotionPiece.toLowerCase() : '';
  return `${coordToNotation(fromR, fromC)}${coordToNotation(toR, toC)}${promo}`;
}

function parseMoveKey(moveKey){
  const match = moveKey.match(/^([a-h])([1-8])([a-h])([1-8])([nbrqNBRQ])?$/);
  if (!match) return null;
  const [, fromFile, fromRank, toFile, toRank, promo = ''] = match;
  const fromC = files.indexOf(fromFile);
  const toC = files.indexOf(toFile);
  const fromR = ranks.indexOf(fromRank);
  const toR = ranks.indexOf(toRank);
  if ([fromC, toC, fromR, toR].some(v => v === -1)) return null;
  return { fromR, fromC, toR, toC, promotionPiece: promo || null };
}

function resetPuzzleProgress(){
  puzzleMoveIndex = 0;
  puzzleSolved = false;
  updatePuzzleFeedback('idle');
}

function restartCurrentPuzzle(){
  puzzleSolved = false;
  puzzleMoveIndex = 0;
  puzzleLockedAfterError = false;
  closePuzzleOverlay();
  if (puzzleStartFen){
    loadPositionFromFen(puzzleStartFen);
  } else {
    resetSelection();
    render();
  }
  updatePuzzleFeedback('info', 'Задача перезапущена.');
}

function updatePuzzleFeedback(state, message = '', options = {}){
  const showActions = options.withActions ?? (state === 'wrong' || state === 'solved');
  if (!puzzleFeedbackEl) return;
  closePuzzleOverlay();
  puzzleFeedbackEl.className = 'puzzle-feedback';
  puzzleFeedbackEl.innerHTML = '';

  const wrapper = document.createElement('div');
  wrapper.className = 'puzzle-feedback-row';
  const shouldRenderRow = state !== 'solved';

  const icon = document.createElement('span');
  icon.className = 'puzzle-indicator';

  const text = document.createElement('span');
  text.className = 'puzzle-feedback-text';

  let overlayTitle = '';
  let overlayActions = [];

  if (state === 'correct'){
    icon.textContent = '✓';
    wrapper.classList.add('success');
    text.textContent = message || 'Ход верный.';
  } else if (state === 'wrong'){
    icon.textContent = '✕';
    wrapper.classList.add('error');
    text.textContent = message || 'Неправильный ход. Попробуйте решить задачу заново.';
    overlayTitle = text.textContent;
  } else if (state === 'error'){
    icon.textContent = '✕';
    wrapper.classList.add('error');
    text.textContent = message || 'Не удалось проверить задачу.';
  } else if (state === 'info'){
    icon.textContent = '•';
    wrapper.classList.add('info');
    text.textContent = message || '';
  } else if (state === 'solved'){
    icon.textContent = '✓';
    wrapper.classList.add('success', 'solved');
    text.textContent = message || '';
    overlayTitle = options.overlayTitle || message || 'Задача решена';
  } else {
    return;
  }

  if (shouldRenderRow){
    wrapper.prepend(icon);
    wrapper.append(text);
  }

  if (showActions){
    const retryBtn = document.createElement('button');
    retryBtn.type = 'button';
    retryBtn.className = 'promotion-btn';
    retryBtn.textContent = 'Решить заново';
    retryBtn.addEventListener('click', () => {
      closePuzzleOverlay();
      restartCurrentPuzzle();
    });

    const newBtn = document.createElement('button');
    newBtn.type = 'button';
    newBtn.className = 'promotion-btn';
    newBtn.textContent = 'Новая задача';
    newBtn.addEventListener('click', () => {
      closePuzzleOverlay();
      fetchRandomPuzzle();
    });

    overlayActions = [retryBtn, newBtn];
  }

  if (shouldRenderRow){
    puzzleFeedbackEl.appendChild(wrapper);
  }

  if (overlayTitle || overlayActions.length){
    openPuzzleOverlay({
      title: overlayTitle || text.textContent,
      actions: overlayActions,
      variant: state === 'solved' ? 'solved' : ''
    });
  }
}

function resetPuzzleBoardToStart(){
  if (!puzzleStartFen) return;
  loadPositionFromFen(puzzleStartFen);
}

function isAtSolutionPosition(){
  if (!puzzleSolutionTargetFen) return false;
  const expectedPlacement = puzzleSolutionTargetFen.split(' ')[0];
  const currentPlacement = boardToFen(boardState).split(' ')[0];
  return expectedPlacement === currentPlacement;
}

function finalizePuzzleSolved(message = ''){
  puzzleLockedAfterError = false;
  puzzleSolved = true;
  puzzleMoveIndex = puzzleSolutionMoves.length;
  updatePuzzleFeedback('solved', message, { withActions: true });
  updatePuzzleStatus();
}

function ensureSolvedFeedbackVisible(message = 'Задача решена.'){
  if (!puzzleSolved || !puzzleFeedbackEl) return;
  const alreadyShown = puzzleFeedbackEl.querySelector('.puzzle-feedback-row.solved');
  if (alreadyShown) return;
  updatePuzzleFeedback('solved', message, { withActions: true });
}

function verifyPuzzleMove(moveKey){
  if (!puzzleMode || !puzzleSolutionMoves.length || puzzleSolved) return true;

  const expectedColor = getExpectedMoveColor(puzzleMoveIndex);
  if (activeColor !== expectedColor) return false;

  const expectedMove = puzzleSolutionMoves[puzzleMoveIndex];
  const isPlayerMove = activeColor === puzzlePlayerColor;
  if (moveKey === expectedMove){
    puzzleLockedAfterError = false;
    puzzleMoveIndex += 1;
    if (puzzleMoveIndex >= puzzleSolutionMoves.length){
      const parsedMove = parseMoveKey(moveKey);
      const stateSnapshot = cloneState({
        board: cloneBoard(boardState),
        active: activeColor,
        castling: JSON.parse(JSON.stringify(castlingRights))
      });
      if (parsedMove){
        applyMoveToState(stateSnapshot, parsedMove);
      }
      const expectedPlacement = (puzzleSolutionTargetFen || '').split(' ')[0];
      const finalPlacement = stateToFen(stateSnapshot).split(' ')[0];
      if (expectedPlacement && expectedPlacement !== finalPlacement){
        updatePuzzleFeedback('error', 'Финальная позиция не совпадает с тем, что записано в PGN.');
        puzzleMoveIndex -= 1;
        return false;
      }
      puzzleSolved = true;
      updatePuzzleFeedback('solved', '', { withActions: true });
      updatePuzzleStatus();
    } else {
      const who = isPlayerMove ? 'Ваш ход принят' : 'Соперник ответил';
      updatePuzzleFeedback('correct', `${who}: ${moveKey}. Ждем следующий ход.`);
    }
    return true;
  }

  puzzleSolved = false;
  puzzleMoveIndex = 0;
  puzzleLockedAfterError = true;
  updatePuzzleFeedback('wrong', `Ожидался ход ${expectedMove}.`, { withActions: true });
  return false;
}

function updateCastlingRights(fromR, fromC, toR, toC, piece){
  const pieceColor = isWhite(piece) ? 'w' : 'b';
  if (piece.toLowerCase() === 'k'){
    castlingRights[pieceColor].K = false;
    castlingRights[pieceColor].Q = false;
  }
  if (piece.toLowerCase() === 'r'){
    if (pieceColor === 'w'){
      if (fromR === 7 && fromC === 0) castlingRights.w.Q = false;
      if (fromR === 7 && fromC === 7) castlingRights.w.K = false;
    } else {
      if (fromR === 0 && fromC === 0) castlingRights.b.Q = false;
      if (fromR === 0 && fromC === 7) castlingRights.b.K = false;
    }
  }

  const target = boardState[toR][toC];
  if (target && target.toLowerCase() === 'r'){
    const targetColor = isWhite(target) ? 'w' : 'b';
    if (targetColor === 'w'){
      if (toR === 7 && toC === 0) castlingRights.w.Q = false;
      if (toR === 7 && toC === 7) castlingRights.w.K = false;
    } else {
      if (toR === 0 && toC === 0) castlingRights.b.Q = false;
      if (toR === 0 && toC === 7) castlingRights.b.K = false;
    }
  }
}

function needsPromotion(piece, toR){
  if (piece === 'P' && toR === 0) return true;
  if (piece === 'p' && toR === 7) return true;
  return false;
}

function applyMove({ fromR, fromC, toR, toC, piece, promotionPiece = null }){
  const moveKey = buildMoveKey({ fromR, fromC, toR, toC, promotionPiece });
  if (!verifyPuzzleMove(moveKey)){
    return;
  }

  const fenBefore = boardToFen(boardState);
  const pieceToPlace = promotionPiece || piece;
  updateCastlingRights(fromR, fromC, toR, toC, piece);

  if (isCastlingMove(piece, fromR, fromC, toR, toC)){
    const isKingSide = toC > fromC;
    const rookFromC = isKingSide ? 7 : 0;
    const rookToC = isKingSide ? 5 : 3;
    boardState[fromR][fromC] = '';
    boardState[toR][toC] = piece;
    boardState[fromR][rookFromC] = '';
    boardState[fromR][rookToC] = isWhite(piece) ? 'R' : 'r';
  } else {
    boardState[fromR][fromC] = '';
    boardState[toR][toC] = pieceToPlace;
  }

  activeColor = activeColor === 'w' ? 'b' : 'w';
  const fenAfter = boardToFen(boardState);
  recordMoveToHistory({ fromR, fromC, toR, toC, promotionPiece, fenBefore, fenAfter });
  playMoveAudio();
  resetSelection();
  render();
  if (!puzzleSolved && isAtSolutionPosition()){
    finalizePuzzleSolved('Позиция решения достигнута.');
  }
  attemptAutoOpponentMove();
  persistPuzzleState();
}

function handlePromotionChoice(pieceCode){
  if (!promotionState) return;
  const { fromR, fromC, toR, toC, piece } = promotionState;
  const color = isWhite(piece) ? 'w' : 'b';
  const promotionPiece = color === 'w' ? pieceCode.toUpperCase() : pieceCode;
  closePromotionDialog();
  promotionState = null;
  applyMove({ fromR, fromC, toR, toC, piece, promotionPiece });
}

function performMove(fromR, fromC, toR, toC){
  if (!isMoveAllowed(fromR, fromC, toR, toC)) return;
  const piece = boardState[fromR][fromC];
  if (needsPromotion(piece, toR)){
    promotionState = { fromR, fromC, toR, toC, piece };
    openPromotionDialog(isWhite(piece) ? 'w' : 'b');
    return;
  }
  applyMove({ fromR, fromC, toR, toC, piece });
}

function attemptAutoOpponentMove(){
  if (puzzleLockedAfterError) return;
  if (!puzzleMode || puzzleSolved) return;
  if (!puzzleSolutionMoves.length) return;
  if (puzzleMoveIndex >= puzzleSolutionMoves.length) return;
  if (activeColor === puzzlePlayerColor) return;

  const moveKey = puzzleSolutionMoves[puzzleMoveIndex];
  const parsed = parseMoveKey(moveKey);
  if (!parsed) return;

  updatePuzzleFeedback('info', `Соперник готовит ответ: ${moveKey}`);

  const { fromR, fromC, toR, toC, promotionPiece } = parsed;
  const piece = boardState[fromR]?.[fromC];
  if (!piece) return;
  if ((activeColor === 'w' && isBlack(piece)) || (activeColor === 'b' && isWhite(piece))) return;

  const promoPiece = promotionPiece
    ? (activeColor === 'w' ? promotionPiece.toUpperCase() : promotionPiece.toLowerCase())
    : null;

  setTimeout(() => {
    applyMove({ fromR, fromC, toR, toC, piece, promotionPiece: promoPiece });
  }, 200);
}

function updateCoordinates(){
  const files = getDisplayFiles();
  const ranks = getDisplayRanks();
  if (filesBottomEl) filesBottomEl.innerHTML = files.map(f => `<span>${f}</span>`).join('');
  if (ranksLeftEl) ranksLeftEl.innerHTML = ranks.map(r => `<span>${r}</span>`).join('');
}

function render(){
  boardEl.innerHTML = '';
  updateCoordinates();
  const whiteKingPos = getKingPosition(boardState, 'w');
  const blackKingPos = getKingPosition(boardState, 'b');
  const whiteInCheck = whiteKingPos && isKingInCheck(boardState, 'w');
  const blackInCheck = blackKingPos && isKingInCheck(boardState, 'b');
  for (let dr=0; dr<8; dr++){
    for (let dc=0; dc<8; dc++){
      const { r, c } = displayToCoord(dr, dc);
      const piece = boardState[r][c];

      const sq = document.createElement('div');
      sq.className = 'sq ' + (((dr+dc)%2===0) ? 'light' : 'dark');
      sq.dataset.dr = String(dr);
      sq.dataset.dc = String(dc);

      if (selectedSquare && selectedSquare.r === r && selectedSquare.c === c){
        sq.classList.add('selected');
      }

      if ((whiteInCheck && whiteKingPos && whiteKingPos.r === r && whiteKingPos.c === c) || (blackInCheck && blackKingPos && blackKingPos.r === r && blackKingPos.c === c)){
        sq.classList.add('king-check');
      }

      if (piece){
        const p = document.createElement('div');
        const isPieceBlack = isBlack(piece);
        p.className = 'piece ' + (isPieceBlack ? 'black' : 'white');

        const img = document.createElement('img');
        img.alt = '';
        img.src = isPieceBlack ? (BLACK_SVG[piece] || '') : (WHITE_SVG[piece] || '');
        img.draggable = false; // keep drag handling on container
        p.appendChild(img);

        p.draggable = true;
        p.dataset.fromR = String(r);
        p.dataset.fromC = String(c);
        p.addEventListener('dragstart', onDragStart);
        p.addEventListener('dragend', onDragEnd);
        p.addEventListener('click', onPieceClick);
        p.addEventListener('pointerdown', onPointerDownManual);
        sq.appendChild(p);
      }

      const moveInfo = highlightedMoves.find(m => m.r === r && m.c === c);
      if (moveInfo){
        if (moveInfo.capture){
          const ring = document.createElement('div');
          ring.className = 'capture-ring';
          sq.appendChild(ring);
        } else {
          const dot = document.createElement('div');
          dot.className = 'move-dot';
          sq.appendChild(dot);
        }
      }

      sq.addEventListener('dragover', onDragOver);
      sq.addEventListener('dragleave', onDragLeave);
      sq.addEventListener('drop', onDrop);
      sq.addEventListener('click', onSquareClick);

      boardEl.appendChild(sq);
    }
  }

  if (fenOutEl){
    fenOutEl.textContent = boardToFen(boardState);
  }
  updateStatus();
  updatePuzzleStatus();
}

let dragFrom = null; // {r,c}
let manualDrag = null; // { fromR, fromC, pointerId, ghost, originEl }
let manualDragActive = false;

function getSquareFromPoint(clientX, clientY){
  const rect = boardEl.getBoundingClientRect();
  if (clientX < rect.left || clientX > rect.right || clientY < rect.top || clientY > rect.bottom){
    return null;
  }
  const cellSize = rect.width / 8;
  const dr = Math.floor((clientY - rect.top) / cellSize);
  const dc = Math.floor((clientX - rect.left) / cellSize);
  const { r, c } = displayToCoord(dr, dc);
  return { r, c, dr, dc };
}

function stopManualDrag(){
  manualDragActive = false;
  if (manualDrag){
    manualDrag.originEl?.classList?.remove('dragging');
    if (manualDrag.ghost?.remove) manualDrag.ghost.remove();
    manualDrag = null;
  }
  window.removeEventListener('pointermove', onPointerMoveManual);
  window.removeEventListener('pointerup', onPointerUpManual);
  window.removeEventListener('pointercancel', onPointerCancelManual);
}

function onPointerDownManual(e){
  const pointerType = e.pointerType || 'mouse';
  if (pointerType !== 'touch' && pointerType !== 'pen') return;

  if (puzzleMode && puzzleLockedAfterError){
    updatePuzzleFeedback('wrong', 'Нажмите «Решить заново» или «Новая задача».', { withActions: true });
    return;
  }

  if (puzzleMode && !puzzleSolved && activeColor !== puzzlePlayerColor){
    return;
  }

  const fromR = Number(e.currentTarget.dataset.fromR);
  const fromC = Number(e.currentTarget.dataset.fromC);
  const piece = boardState[fromR][fromC];
  if ((activeColor === 'w' && isBlack(piece)) || (activeColor === 'b' && isWhite(piece))){
    return;
  }

  manualDragActive = true;
  e.currentTarget.classList.add('dragging');
  const rect = e.currentTarget.getBoundingClientRect();
  const ghost = e.currentTarget.cloneNode(true);
  ghost.classList.add('drag-ghost');
  ghost.style.width = `${rect.width}px`;
  ghost.style.height = `${rect.height}px`;
  document.body.appendChild(ghost);

  manualDrag = {
    fromR,
    fromC,
    pointerId: e.pointerId,
    ghost,
    originEl: e.currentTarget,
    startX: e.clientX,
    startY: e.clientY,
    moved: false
  };

  const moveGhost = () => {
    ghost.style.left = `${e.clientX}px`;
    ghost.style.top = `${e.clientY}px`;
  };
  moveGhost();

  window.addEventListener('pointermove', onPointerMoveManual);
  window.addEventListener('pointerup', onPointerUpManual);
  window.addEventListener('pointercancel', onPointerCancelManual);
  e.preventDefault();
}

function onPointerMoveManual(e){
  if (!manualDrag || e.pointerId !== manualDrag.pointerId) return;
  const { ghost, startX, startY } = manualDrag;
  ghost.style.left = `${e.clientX}px`;
  ghost.style.top = `${e.clientY}px`;

  if (!manualDrag.moved){
    const dx = Math.abs(e.clientX - startX);
    const dy = Math.abs(e.clientY - startY);
    manualDrag.moved = dx + dy > 6;
  }

  const sq = getSquareFromPoint(e.clientX, e.clientY);
  document.querySelectorAll('.sq.drop').forEach(el => el.classList.remove('drop'));
  if (sq){
    const selector = `.sq[data-dr="${sq.dr}"][data-dc="${sq.dc}"]`;
    document.querySelector(selector)?.classList.add('drop');
  }
}

function onPointerUpManual(e){
  if (!manualDrag || e.pointerId !== manualDrag.pointerId) return;
  const { fromR, fromC, originEl, moved } = manualDrag;
  const targetSq = getSquareFromPoint(e.clientX, e.clientY);
  stopManualDrag();
  document.querySelectorAll('.sq.drop').forEach(el => el.classList.remove('drop'));

  if (originEl){
    originEl.classList.remove('dragging');
  }

  if (!targetSq){
    if (!moved){
      handleSquareTap(fromR, fromC);
    }
    return;
  }

  if (!moved){
    handleSquareTap(targetSq.r, targetSq.c);
    return;
  }

  const piece = boardState[fromR][fromC];
  if (!piece) return;
  if ((activeColor === 'w' && isBlack(piece)) || (activeColor === 'b' && isWhite(piece))){
    return;
  }

  performMove(fromR, fromC, targetSq.r, targetSq.c);
}

function onPointerCancelManual(e){
  if (!manualDrag || e.pointerId !== manualDrag.pointerId) return;
  stopManualDrag();
  document.querySelectorAll('.sq.drop').forEach(el => el.classList.remove('drop'));
}

function onDragStart(e){
  const fromR = Number(e.target.dataset.fromR);
  const fromC = Number(e.target.dataset.fromC);
  const piece = boardState[fromR][fromC];
  if (puzzleMode && puzzleLockedAfterError){
    updatePuzzleFeedback('wrong', 'Нажмите «Решить заново» или «Новая задача».', { withActions: true });
    e.preventDefault();
    return;
  }
  if (puzzleMode && !puzzleSolved && activeColor !== puzzlePlayerColor){
    e.preventDefault();
    return;
  }
  if ((activeColor === 'w' && isBlack(piece)) || (activeColor === 'b' && isWhite(piece))){
    e.preventDefault();
    return;
  }

  dragFrom = { r: fromR, c: fromC };
  e.dataTransfer.setData('text/plain', JSON.stringify(dragFrom));
  e.dataTransfer.effectAllowed = 'move';

  // Drag image: only the piece (not a full-square ghost).
  const ghost = e.target.cloneNode(true);
  ghost.style.position = 'absolute';
  ghost.style.top = '-9999px';
  ghost.style.left = '-9999px';
  ghost.style.width = 'auto';
  ghost.style.height = 'auto';
  ghost.style.background = 'transparent';
  ghost.style.padding = '0';
  ghost.style.margin = '0';
  document.body.appendChild(ghost);

  const x = ghost.offsetWidth / 2;
  const y = ghost.offsetHeight / 2;
  if (e.dataTransfer.setDragImage) e.dataTransfer.setDragImage(ghost, x, y);

  setTimeout(() => ghost.remove(), 0);
}

function onDragEnd(){
  dragFrom = null;
  document.querySelectorAll('.sq.drop').forEach(el => el.classList.remove('drop'));
}

function onPieceClick(e){
  e.stopPropagation();
  if (manualDragActive) return;
  const fromR = Number(e.currentTarget.dataset.fromR);
  const fromC = Number(e.currentTarget.dataset.fromC);
  handleSquareTap(fromR, fromC);
}

function onSquareClick(e){
  const dr = Number(e.currentTarget.dataset.dr);
  const dc = Number(e.currentTarget.dataset.dc);
  const { r, c } = displayToCoord(dr, dc);
  handleSquareTap(r, c);
}

function onDragOver(e){
  e.preventDefault();
  e.currentTarget.classList.add('drop');
  e.dataTransfer.dropEffect = 'move';
}

function onDragLeave(e){
  e.currentTarget.classList.remove('drop');
}

function onDrop(e){
  e.preventDefault();
  e.currentTarget.classList.remove('drop');

  if (puzzleMode && !puzzleSolved && activeColor !== puzzlePlayerColor){
    return;
  }

  let from;
  try{
    from = JSON.parse(e.dataTransfer.getData('text/plain'));
  } catch {
    from = dragFrom;
  }
  if (!from) return;

  const dr = Number(e.currentTarget.dataset.dr);
  const dc = Number(e.currentTarget.dataset.dc);
  const { r: toR, c: toC } = displayToCoord(dr, dc);

  const piece = boardState[from.r][from.c];
  if (!piece) return;

  if ((activeColor === 'w' && isBlack(piece)) || (activeColor === 'b' && isWhite(piece))){
    return;
  }

  performMove(from.r, from.c, toR, toC);
}

async function fetchRandomPuzzle(){
  closePuzzleOverlay();
  puzzleMode = false;
  puzzleSolutionTargetFen = null;
  puzzleLoading = true;
  puzzleSolutionMoves = [];
  puzzleMoveIndex = 0;
  puzzleSolved = false;
  puzzleStartFen = null;
  puzzlePlayerColor = null;
  puzzleData = null;
  puzzleLockedAfterError = false;
  if (puzzleStatusEl) puzzleStatusEl.textContent = 'Загрузка задачи...';
  updatePuzzleFeedback('info', 'Загружаем новую задачу...');
  resetSelection();
  render();
  try {
    const res = await fetch('https://api.chess.com/pub/puzzle/random');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    puzzleLoading = false;
    updatePuzzleInfoDisplay(data);
    if (data?.fen) {
      loadPositionFromFen(data.fen);
    } else {
      puzzleMode = false;
      updatePuzzleStatus();
    }
  } catch (err) {
    console.error('Puzzle load error', err);
    if (puzzleStatusEl) puzzleStatusEl.textContent = 'Не удалось загрузить задачу.';
    updatePuzzleFeedback('error', 'Не удалось загрузить задачу. Попробуйте снова.');
    puzzleLoading = false;
    updatePuzzleStatus();
  }
}

function openAnalysisPage(){
  const fen = boardToFen(boardState);
  const url = `analysis.html?fen=${encodeURIComponent(fen)}`;
  persistPuzzleState();
  window.location.href = url;
}

function closeAnalysisOverlay(){
  if (analysisOverlayEl){
    analysisOverlayEl.classList.remove('active');
  }
  document.body.classList.remove('no-scroll');
}

function hydratePuzzleState(){
  try{
    const raw = localStorage.getItem(PUZZLE_STORAGE_KEY);
    if (!raw) return false;
    const saved = JSON.parse(raw);
    if (!saved?.puzzleData || !saved?.boardFen) return false;

    updatePuzzleInfoDisplay(saved.puzzleData);
    const parsed = parseFenState(saved.boardFen);
    boardState = parsed.board;
    activeColor = parsed.active;
    castlingRights = parsed.castling;
    historyStartFen = saved.historyStartFen || saved.boardFen;
    moveHistory = Array.isArray(saved.moveHistory) ? saved.moveHistory : [];
    persistMoveHistory();
    puzzleSolutionMoves = Array.isArray(saved.puzzleSolutionMoves) && saved.puzzleSolutionMoves.length
      ? saved.puzzleSolutionMoves
      : puzzleSolutionMoves;
    puzzleMoveIndex = Number.isInteger(saved.puzzleMoveIndex) ? saved.puzzleMoveIndex : 0;
    puzzleSolved = !!saved.puzzleSolved;
    puzzleMode = !!saved.puzzleMode && puzzleSolutionMoves.length > 0;
    puzzleStartFen = saved.puzzleStartFen || puzzleStartFen;
    puzzlePlayerColor = saved.puzzlePlayerColor || parsed.active;
    puzzleSolutionTargetFen = saved.puzzleSolutionTargetFen || puzzleSolutionTargetFen;
    puzzleLoading = false;
    puzzleLockedAfterError = !!saved.puzzleLockedAfterError;
    promotionState = null;
    resetSelection();
    closePromotionDialog();
    closePuzzleOverlay();
    render();
    updatePuzzleStatus();
    ensureSolvedFeedbackVisible();
    persistPuzzleState();
    return true;
  } catch (err){
    console.warn('Не удалось восстановить задачу', err);
    return false;
  }
}

document.getElementById('puzzleBtn').addEventListener('click', () => {
  fetchRandomPuzzle();
});

if (analyzeBtn){
  analyzeBtn.addEventListener('click', openAnalysisPage);
}

if (openSettingsPageBtn){
  openSettingsPageBtn.addEventListener('click', () => {
    persistPuzzleState();
    window.location.href = 'settings.html';
  });
}

if (closeAnalysisBtn){
  closeAnalysisBtn.addEventListener('click', closeAnalysisOverlay);
}

if (analysisOverlayEl){
  analysisOverlayEl.addEventListener('click', (event) => {
    if (event.target === analysisOverlayEl){
      closeAnalysisOverlay();
    }
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && analysisOverlayEl.classList.contains('active')){
      closeAnalysisOverlay();
    }
  });
}

if (settingsDetailsEl){
  const handleOutsideSettingsClick = (event) => {
    if (!settingsDetailsEl.open) return;
    if (settingsDetailsEl.contains(event.target)) return;
    settingsDetailsEl.removeAttribute('open');
  };

  // Use capture phase to react even if inner elements stop propagation (e.g., chess pieces).
  document.addEventListener('pointerdown', handleOutsideSettingsClick, true);

  if (boardTitleEl){
    boardTitleEl.addEventListener('click', () => {
      if (!settingsDetailsEl.open) return;
      settingsDetailsEl.removeAttribute('open');
    });
  }
}

window.addEventListener('storage', (event) => {
  if (event.key === SOUND_STORAGE_KEY){
    soundEnabled = loadSoundPreference();
  }
  if (event.key === PALETTE_STORAGE_KEY){
    applyBoardPalette(loadPalettePreference());
  }
});

promotionButtons.forEach(btn => {
  btn.addEventListener('click', () => handlePromotionChoice(btn.dataset.piece));
});

applyBoardPalette(loadPalettePreference());
preventZoom();
initTelegram();
const restoredPuzzle = hydratePuzzleState();
if (!restoredPuzzle){
  resetMoveHistory(boardToFen(boardState));
  render();
  fetchRandomPuzzle();
}

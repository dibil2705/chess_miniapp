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

// Default: start position
const START_FEN = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1';

let flipped = false;
let boardState = fenToBoard(START_FEN);
let activeColor = 'w';
let castlingRights = { w: { K: true, Q: true }, b: { K: true, Q: true } };

let selectedSquare = null;
let highlightedMoves = [];

const boardEl = document.getElementById('board');
const fenOutEl = document.getElementById('fenOut');
const statusEl = document.getElementById('status');

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

function isLegalKingMove(fromR, fromC, toR, toC, board = boardState, { allowCastling = true } = {}){
  const dr = Math.abs(toR - fromR);
  const dc = Math.abs(toC - fromC);
  if (dr <= 1 && dc <= 1) return true;
  if (!allowCastling) return false;
  const piece = board[fromR][fromC];
  const color = isWhite(piece) ? 'w' : 'b';
  if (dr === 0 && dc === 2){
    const side = toC > fromC ? 'K' : 'Q';
    return canCastle(color, side, board);
  }
  return false;
}

function isLegalMove(fromR, fromC, toR, toC, board = boardState, opts = {}){
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
      return isLegalKingMove(fromR, fromC, toR, toC, board, { allowCastling });
    default:
      return false;
  }
}

function cloneBoard(board){
  return board.map(row => [...row]);
}

function isSquareAttacked(board, targetR, targetC, attackerColor){
  for (let r=0; r<8; r++){
    for (let c=0; c<8; c++){
      const piece = board[r][c];
      if (!piece) continue;
      if (attackerColor === 'w' && !isWhite(piece)) continue;
      if (attackerColor === 'b' && !isBlack(piece)) continue;
      if (isLegalMove(r, c, targetR, targetC, board, { allowCastling: false })) return true;
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

function isKingInCheck(board, color){
  const kingPos = getKingPosition(board, color);
  if (!kingPos) return false;
  const attacker = color === 'w' ? 'b' : 'w';
  return isSquareAttacked(board, kingPos.r, kingPos.c, attacker);
}

function canCastle(color, side, board = boardState){
  const rights = castlingRights[color];
  if (!rights) return false;
  if (side === 'K' && !rights.K) return false;
  if (side === 'Q' && !rights.Q) return false;

  const row = color === 'w' ? 7 : 0;
  const kingCol = 4;
  const rookCol = side === 'K' ? 7 : 0;
  const king = color === 'w' ? 'K' : 'k';
  const rook = color === 'w' ? 'R' : 'r';
  if (board[row][kingCol] !== king || board[row][rookCol] !== rook) return false;

  const throughCols = side === 'K' ? [5,6] : [3,2];
  if (!isPathClear(row, kingCol, row, rookCol, board)) return false;
  if (isKingInCheck(board, color)) return false;
  const opponent = color === 'w' ? 'b' : 'w';
  for (const col of throughCols){
    if (isSquareAttacked(board, row, col, opponent)) return false;
  }
  return true;
}

function isCastlingMove(piece, fromR, fromC, toR, toC){
  if (piece.toLowerCase() !== 'k') return false;
  if (fromR !== toR) return false;
  return Math.abs(toC - fromC) === 2;
}

function moveLeavesKingInCheck(fromR, fromC, toR, toC, board = boardState){
  const piece = board[fromR][fromC];
  const movingColor = isWhite(piece) ? 'w' : 'b';
  const next = cloneBoard(board);
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
  return isKingInCheck(next, movingColor);
}

function isMoveAllowed(fromR, fromC, toR, toC){
  if (!isLegalMove(fromR, fromC, toR, toC, boardState)) return false;
  return !moveLeavesKingInCheck(fromR, fromC, toR, toC, boardState);
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

function resetSelection(){
  selectedSquare = null;
  highlightedMoves = [];
}

function selectSquare(r, c){
  selectedSquare = { r, c };
  highlightedMoves = getLegalMovesForPiece(r, c);
}

function updateStatus(){
  if (!statusEl) return;
  const inCheck = isKingInCheck(boardState, activeColor);
  const hasMoves = playerHasLegalMoves(activeColor);
  if (inCheck && !hasMoves){
    const loser = activeColor === 'w' ? 'белым' : 'черным';
    const winner = activeColor === 'w' ? 'Черные' : 'Белые';
    statusEl.textContent = `Мат ${loser}. ${winner} победили.`;
    return;
  }
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

function performMove(fromR, fromC, toR, toC){
  if (!isMoveAllowed(fromR, fromC, toR, toC)) return;
  const piece = boardState[fromR][fromC];
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
    boardState[toR][toC] = piece;
  }
  activeColor = activeColor === 'w' ? 'b' : 'w';
  resetSelection();
  render();
}

function render(){
  boardEl.innerHTML = '';
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
        sq.appendChild(p);
      }

      if (highlightedMoves.some(m => m.r === r && m.c === c)){
        const dot = document.createElement('div');
        dot.className = 'move-dot';
        sq.appendChild(dot);
      }

      sq.addEventListener('dragover', onDragOver);
      sq.addEventListener('dragleave', onDragLeave);
      sq.addEventListener('drop', onDrop);
      sq.addEventListener('click', onSquareClick);

      boardEl.appendChild(sq);
    }
  }

  fenOutEl.textContent = boardToFen(boardState);
  updateStatus();
}

let dragFrom = null; // {r,c}

function onDragStart(e){
  const fromR = Number(e.target.dataset.fromR);
  const fromC = Number(e.target.dataset.fromC);
  const piece = boardState[fromR][fromC];
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
  const fromR = Number(e.currentTarget.dataset.fromR);
  const fromC = Number(e.currentTarget.dataset.fromC);
  const piece = boardState[fromR][fromC];
  if ((activeColor === 'w' && isBlack(piece)) || (activeColor === 'b' && isWhite(piece))){
    return;
  }
  selectSquare(fromR, fromC);
  render();
}

function onSquareClick(e){
  const dr = Number(e.currentTarget.dataset.dr);
  const dc = Number(e.currentTarget.dataset.dc);
  const { r, c } = displayToCoord(dr, dc);

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

document.getElementById('resetBtn').addEventListener('click', () => {
  boardState = fenToBoard(START_FEN);
  activeColor = 'w';
  castlingRights = { w: { K: true, Q: true }, b: { K: true, Q: true } };
  resetSelection();
  render();
});

document.getElementById('flipBtn').addEventListener('click', () => {
  flipped = !flipped;
  render();
});

document.getElementById('loadStartBtn').addEventListener('click', () => {
  // start position (same as START_FEN but explicit)
  boardState = fenToBoard('rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w - - 0 1');
  activeColor = 'w';
  castlingRights = { w: { K: true, Q: true }, b: { K: true, Q: true } };
  resetSelection();
  render();
});

// initial render
render();

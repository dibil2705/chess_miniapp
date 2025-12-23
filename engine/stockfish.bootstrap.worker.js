// Thin bootstrap worker around Stockfish builds.
// Tries local wasm build first; falls back to a pure JS build when SharedArrayBuffer or COOP/COEP are unavailable.

let engine = null;

function wireEngine(){
  if (!engine) return;
  engine.onmessage = (event) => {
    // Emscripten workers sometimes wrap messages in an object; normalize to raw strings.
    postMessage(event?.data ?? event);
  };
  onmessage = (event) => {
    engine.postMessage(event.data ?? event);
  };
}

function tryLoad(url){
  return new Promise((resolve, reject) => {
    try {
      importScripts(url);
      if (typeof Stockfish !== 'function') {
        reject(new Error('Stockfish factory not found after loading ' + url));
        return;
      }
      const inst = Stockfish();
      resolve(inst);
    } catch (err) {
      reject(err);
    }
  });
}

(async () => {
  try {
    // Local wasm build. Requires cross-origin isolation for SharedArrayBuffer.
    engine = await tryLoad('stockfish.js');
  } catch (wasmErr) {
    // Fallback to asm.js version that does not depend on SharedArrayBuffer.
    postMessage('WASM engine unavailable, falling back to asm.js: ' + wasmErr.message);
    engine = await tryLoad('https://cdn.jsdelivr.net/npm/stockfish@16/stockfish.js');
  }

  wireEngine();
  engine.postMessage('uci');
})();

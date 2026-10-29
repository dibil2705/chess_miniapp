(function(root){
  'use strict';

  // FIDE C.07 (March 2026), articles 7–9, 14 and 16:
  // https://handbook.fide.com/chapter/TieBreakRegulations032026
  function calculateStandings(rounds){
    const players = new Map();
    const ensure = (name) => {
      if (!players.has(name)) players.set(name, { name, games:Array(rounds.length).fill(null) });
      return players.get(name);
    };
    rounds.forEach((games, round) => {
      const seen = new Set();
      const record = (name, opponent, score, black) => {
        if (seen.has(name)) throw new Error(`Duplicate player in round ${round + 1}: ${name}`);
        seen.add(name);
        ensure(name).games[round] = { opponent, score, black };
      };
      games.forEach(([white, black, result]) => {
        if (!black){
          if (result !== 'Bye') throw new Error(`Unknown unplayed result: ${result}`);
          record(white, null, 1, false);
          return;
        }
        const scores = { '1:0':[1, 0], '0:1':[0, 1], '½:½':[0.5, 0.5] }[result];
        if (!scores) throw new Error(`Unknown game result: ${result}`);
        record(white, black, scores[0], false);
        record(black, white, scores[1], true);
      });
    });

    players.forEach((player) => {
      player.points = player.games.reduce((sum, game) => sum + (game?.score || 0), 0);
      // Missing rounds are zero-point byes. Trailing withdrawals count as draws
      // only when calculating the tie-breaks of this player's opponents (16.3).
      let trailing = 0;
      for (let i = rounds.length - 1; i >= 0 && !player.games[i]; i--) trailing++;
      player.adjustedPoints = player.points + trailing * 0.5;
    });

    players.forEach((player) => {
      const dummyScore = Math.min(player.points, rounds.length * 0.5);
      const contributions = player.games.map((game) => game?.opponent
        ? players.get(game.opponent).adjustedPoints : dummyScore);
      const voluntary = contributions.filter((value, index) => !player.games[index]);
      const bh = contributions.reduce((sum, value) => sum + value, 0);
      const cut = Math.min(...(voluntary.length ? voluntary : contributions));
      let running = 0;
      let progressive = 0;
      player.games.forEach((game) => { running += game?.score || 0; progressive += running; });
      player.values = [
        bh - cut,
        bh,
        contributions.reduce((sum, value, index) => sum + value * (player.games[index]?.score || 0), 0),
        progressive,
        0,
        player.games.filter((game) => game?.score === 1).length,
        player.games.filter((game) => game?.black && game.score === 1).length
      ];
    });

    const compare = (a, b, count = 7) => {
      if (a.points !== b.points) return b.points - a.points;
      for (let i = 0; i < count; i++){
        if (a.values[i] !== b.values[i]) return b.values[i] - a.values[i];
      }
      return 0;
    };
    const ordered = [...players.values()].sort(compare);
    // DE is applied only within groups still tied after BHC1, BH, SB and PS.
    for (let start = 0; start < ordered.length;){
      let end = start + 1;
      while (end < ordered.length && compare(ordered[start], ordered[end], 4) === 0) end++;
      const group = ordered.slice(start, end);
      const mutual = (a, b) => a.games.filter((game) => game?.opponent === b.name);
      if (group.length > 1 && group.every((a) => group.every((b) => a === b || mutual(a, b).length))){
        group.forEach((a) => {
          a.values[4] = group.reduce((sum, b) => {
            const games = mutual(a, b);
            return sum + (games.length ? games.reduce((s, game) => s + game.score, 0) / games.length : 0);
          }, 0);
        });
      }
      start = end;
    }
    ordered.sort(compare);
    return ordered.map((player, index) => {
      let first = index;
      let last = index;
      while (first > 0 && compare(ordered[first - 1], player) === 0) first--;
      while (last + 1 < ordered.length && compare(ordered[last + 1], player) === 0) last++;
      const place = first === last ? String(first + 1) : `${first + 1}–${last + 1}`;
      return [place, player.name, String(player.points), ...player.values.map(String)];
    });
  }

  root.ChessSwiss = { calculateStandings };
  if (typeof module !== 'undefined' && module.exports) module.exports = root.ChessSwiss;
})(typeof window !== 'undefined' ? window : globalThis);

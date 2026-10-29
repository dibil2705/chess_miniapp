const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {calculateStandings} = require('../assets/tournament-swiss.js');

const html = fs.readFileSync(path.join(__dirname, '../tournament-2026-10-25.html'), 'utf8');
function loadPage(){
  const nodes = new Map();
  const element = () => ({innerHTML:'', textContent:'', classList:{add(){},remove(){}}, addEventListener(){}});
  const document = {
    getElementById(id){ if (!nodes.has(id)) nodes.set(id, element()); return nodes.get(id); },
    querySelectorAll(){ return []; }, addEventListener(){}, body:element()
  };
  const context = vm.createContext({document, window:{}, ChessSwiss:{calculateStandings}});
  const code = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(match => match[1]).join('\n');
  vm.runInContext(code, context);
  const data = JSON.parse(vm.runInContext('JSON.stringify(tournamentData)', context));
  return {context, data, nodes};
}

test('all adult places and seven tie-breaks match the supplied official table', () => {
  const {data} = loadPage();
  assert.equal(data.adults.rounds.length, 7);
  assert.equal(data.adults.standings.length, 21);
  assert.deepEqual(calculateStandings(data.adults.rounds), data.adults.standings);
  assert.equal(data.adults.rounds.flat().filter(([,black]) => black).length, 70);
  assert.equal(data.adults.rounds.flat().filter(([,black]) => !black).length, 4);
});

test('children match every pre-round total shown in the six screenshots', () => {
  const {data} = loadPage();
  const before = [
    [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
    [1,1,1,1,1,1,1,1,0,0,0,0,0,0,0],
    [2,2,2,1.5,1.5,1,1,1,1,1,1,1,0,0,0],
    [2.5,3,2,2,2,2,2,1.5,1.5,1.5,1,1,1,1,0],
    [3,3.5,3,3,3,2.5,2,2,2,2,1.5,1.5,1,1],
    [3.5,4,3.5,3.5,3.5,3,3,3,2,2,2,2,1,2]
  ];
  const points = new Map(data.kids.standings.map(([,name]) => [name, 0]));
  data.kids.rounds.forEach((games, round) => {
    const names = games.flatMap(([white,black]) => black ? [white,black] : [white]);
    assert.equal(new Set(names).size, names.length);
    assert.deepEqual(names.map(name => points.get(name)), before[round]);
    games.forEach(([white,black,result]) => {
      if (!black) points.set(white, points.get(white) + 1);
      else {
        const scores = result === '½:½' ? [0.5,0.5] : result.split(':').map(Number);
        points.set(white, points.get(white) + scores[0]);
        points.set(black, points.get(black) + scores[1]);
      }
    });
  });
  assert.equal([...points.values()].reduce((a,b) => a+b), 46);
  for (const [,name,total] of data.kids.standings) assert.equal(points.get(name), Number(total));
});

test('child medals use BH first; withdrawals adjust opponents only', () => {
  const {data} = loadPage();
  assert.deepEqual(data.kids.standings.slice(0,3), [
    ['1','Михаил Акопян','4.5','19.5','17.5','14.75','14.5','0','4','2'],
    ['2','Михалова Мария','4.5','19.5','17.5','13.25','15','0','4','2'],
    ['3','Ольшевский Данил','4.5','19','18','12.75','14','0','4','2']
  ]);
  const alice = data.kids.standings.find(row => row[1] === 'Алиса Петрова');
  assert.equal(alice[2], '1');
  assert.equal(alice[3], '13');
  assert.equal(alice[4], '12');
  const maxim = data.kids.standings.find(row => row[1] === 'Максим Козлов');
  assert.equal(maxim[3], '20');
  const tigran = data.kids.standings.find(row => row[1] === 'Тигран Гезолян');
  assert.equal(tigran[8], '1'); // WIN includes his allocated bye.
});

test('byes award a point without changing Elo; profiles show every scheduled round', () => {
  const {context,data,nodes} = loadPage();
  for (const category of ['kids','adults']){
    vm.runInContext(`activeCategory = '${category}'; renderCategory();`, context);
    assert.equal((nodes.get('categoryContent').innerHTML.match(/data-player=/g) || []).length, data[category].standings.length);
    const profiles = vm.runInContext(`buildProfiles(tournamentData.${category})`, context);
    for (const [,name] of data[category].standings){
      const profile = profiles.get(name);
      assert.equal(profile.games.length, data[category].rounds.length);
      for (const game of profile.games) if (game.result.startsWith('Bye') || game.result === '0 очков') assert.equal(game.elo, null);
      context.playerName = name;
      vm.runInContext('openProfile(playerName)', context);
      assert.equal(nodes.get('profileName').textContent, name);
      assert.ok(!nodes.get('profileGames').innerHTML.includes('undefined'));
      assert.ok(nodes.get('profileStats').innerHTML.includes('BWG'));
      vm.runInContext('closeProfile()', context);
    }
  }
  const byeOnly = {standings:[['1','A','1']], rounds:[[['A',null,'Bye']]]};
  context.category = byeOnly;
  const profile = vm.runInContext('calculateElo(category).get("A")', context);
  assert.equal(profile.before, 1500);
  assert.equal(profile.after, 1500);
  assert.equal(profile.delta, 0);
});

test('invalid games cannot silently produce standings', () => {
  assert.throws(() => calculateStandings([[['A','B','?']]]), /Unknown game/);
  assert.throws(() => calculateStandings([[['A','B','1:0'],['A','C','0:1']]]), /Duplicate player/);
});

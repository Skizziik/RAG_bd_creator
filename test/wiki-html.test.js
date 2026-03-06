const assert = require('node:assert/strict');
const { extractFromHTML } = require('../lib/wiki');

const html = `
<table class="infobox">
  <tr><th>Internal name</th><td>AlchemicGun</td></tr>
  <tr><th>Type</th><td>Ranged Weapon</td></tr>
  <tr>
    <th>Base DPS</th><td>9</td>
    <th>Base hit</th><td>10</td>
    <th>Base DoT DPS</th><td>13 (poison effect)</td>
  </tr>
  <tr>
    <th>Location</th><td>Secret area in the Ancient Sewers</td>
    <th>Unlock cost</th><td>50</td>
  </tr>
</table>
<div class="mw-parser-output"><p>Body text.</p></div>`;

const result = extractFromHTML(html, 'Alchemic Carbine');

assert.equal(result.pageTitle, 'Alchemic Carbine');
assert.equal(result.infobox['Internal name'], 'AlchemicGun');
assert.equal(result.infobox['Type'], 'Ranged Weapon');
assert.equal(result.infobox['Base DPS'], '9');
assert.equal(result.infobox['Base hit'], '10');
assert.equal(result.infobox['Base DoT DPS'], '13 (poison effect)');
assert.equal(result.infobox['Location'], 'Secret area in the Ancient Sewers');
assert.equal(result.infobox['Unlock cost'], '50');
assert.ok(!('LocationUnlock cost' in result.infobox));
assert.ok(!('Base DPSBase hitBase DoT DPS' in result.infobox));



const atlyssHtml = 
`<div class="lg-container lg-infobox noexcerpt">
  <div class="lg-title">Carbuncle Hat</div>
  <div class="lg-section">General</div>
  <div class="lg-row"><div class="lg-label">Level</div><div class="lg-data">16</div></div>
  <div class="lg-row"><div class="lg-label">Rarity</div><div class="lg-data"><span class="rarity-rare">Rare</span></div></div>
  <div class="lg-row"><div class="lg-label">Buy Price</div><div class="lg-data">890</div></div>
  <div class="lg-row"><div class="lg-label">Sell Price</div><div class="lg-data">338</div></div>
  <div class="lg-section">Stats</div>
  <div class="lg-row"><div class="lg-label">Magic Defense</div><div class="lg-data">+8</div></div>
  <div class="lg-row"><div class="lg-label">Shadow Resist</div><div class="lg-data">+3</div></div>
</div>
<div class="mw-parser-output"><p>The Carbuncle Hat is a Helm item.</p></div>`;

const atlyssResult = extractFromHTML(atlyssHtml, 'Carbuncle Hat');
assert.equal(atlyssResult.infobox['Level'], '16');
assert.equal(atlyssResult.infobox['Rarity'], 'Rare');
assert.equal(atlyssResult.infobox['Buy Price'], '890');
assert.equal(atlyssResult.infobox['Sell Price'], '338');
assert.equal(atlyssResult.infobox['Magic Defense'], '+8');
assert.equal(atlyssResult.infobox['Shadow Resist'], '+3');

console.log('wiki-html smoke test passed');
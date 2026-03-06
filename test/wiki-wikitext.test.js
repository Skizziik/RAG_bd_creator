const assert = require('node:assert/strict');
const { buildChunksFromExtractedPage } = require('../lib/wiki-wikitext');

const page = {
  title: 'Copper Pickaxe',
  wikitext: `{{Infobox item
| damage = 4
| rarity = Common
}}

'''Copper Pickaxe''' is a basic mining tool.

== Obtaining ==
* Crafted at a Work Bench

{| class="wikitable"
|+ Crafting
! Material !! Amount
|-
| Wood || 10
|-
| Copper Bar || 3
|}

== References ==
* Ignore me
`,
};

const detailsPage = {
  title: 'Shovel',
  wikitext: `{{ItemInfobox
| quote = Knocks back enemies and bombs.
| internal_name = Shovel
| type = Melee Weapon
}}

The '''Shovel''' is a [[Melee weapons|melee]] weapon.

== Details ==
{{ItemDetails
| special = * Bombs hit by Shovel strikes are sent back as if they were parried.
* The final hit sends enemies flying away.
| breach_dps = 363
| forced_affix = Better Secrets
| affix_quote = Secrets found in the walls and ground are of increased quality.
}}

== Notes ==
* Shovel works with {{Mutation|Porcupack}} while equipped in the {{Color|Backpack|backpack}}.

== Trivia ==
* Ignore me
`,
};

const tableNoisePage = {
  title: 'Difficulty Table',
  wikitext: `A test page.

{| class="wikitable"
|-
! Biome !! Enemy
|-
| style="width: 17%" |Nightmare/Hell || Kamikaze
|-
| style="width: 17%" |Nightmare/Hell || colspan="5" style="background-color:#0c0c0b; text-align: center;" |Frantic Sword, Kamikaze Outfit
|}
`,
};

const movelistPage = {
  title: 'Stone Warden',
  wikitext: `Stone Wardens are enemies.

== Moveset ==
{{MovelistTable/Start}}
{{MovelistTable
| name = Axe swing
| description = Swings its stone axe in a wide arc.
| notes = 
* Can be blocked, parried, and dodge rolled.
}}
{{MovelistTable/End}}

== Footnotes ==
* Ignore me
`,
};

const genericTemplatePage = {
  title: 'Forest Bee',
  wikitext: `[[Category:Bees]]
{{
Bee|
species=Forest|
latin=Apis Silva|
tier=1|
found=found naturally in {{IconLink|img=Beehive_Item|link=Beehives}} in the [[Biomes#Forests|Forests]].|
description=Originally a marshland species, the 'Forest' Bee has since migrated to lusher forests.|
hint=Although the Forest Bee is found naturally.|
special=Sticky Pearl|
recipe1_bee=Common|
recipe1_conditions=During [[Time|Daytime]]|
recipe1_outcome=Verdant|
recipe1_chance=40|
lifespan=Normal|
productivity=Slow|
fertility=Fertile|
stability=Normal|
behaviour=Diurnal|
climate=Temperate|
trivia=
}}`,
};

const positionalInfoboxPage = {
  title: 'Carbuncle Hat',
  wikitext: `{{EquipmentInfobox|Multi}}

'''Carbuncle Hat''' is a helm item.`,
};

const result = buildChunksFromExtractedPage(page, { limit: 120 });
assert.equal(result.pageTitle, 'Copper Pickaxe');
assert.equal(result.infobox.damage, '4');
assert.match(result.text, /Copper Pickaxe is a basic mining tool/i);
assert.match(result.text, /Crafting/i);
assert.match(result.text, /Wood/i);
assert.ok(result.qualityScore >= 0.4);
assert.ok(result.chunks.length >= 1);

const detailsResult = buildChunksFromExtractedPage(detailsPage, { limit: 400 });
assert.match(detailsResult.text, /The Shovel is a melee weapon/i);
assert.match(detailsResult.text, /## Details/i);
assert.match(detailsResult.text, /Bombs hit by Shovel strikes are sent back/i);
assert.match(detailsResult.text, /Breach Dps: 363/i);
assert.match(detailsResult.text, /Legendary affix: Better Secrets/i);
assert.match(detailsResult.text, /## Notes/i);
assert.match(detailsResult.text, /Porcupack/i);
assert.doesNotMatch(detailsResult.text, /## Trivia/i);
assert.doesNotMatch(detailsResult.text, /## Overview/i);

const tableResult = buildChunksFromExtractedPage(tableNoisePage, { limit: 400 });
assert.match(tableResult.text, /Nightmare\/Hell: Kamikaze/i);
assert.match(tableResult.text, /Nightmare\/Hell: Frantic Sword, Kamikaze Outfit/i);
assert.doesNotMatch(tableResult.text, /style=/i);
assert.doesNotMatch(tableResult.text, /colspan=/i);

const movelistResult = buildChunksFromExtractedPage(movelistPage, { limit: 400 });
assert.match(movelistResult.text, /## Moveset/i);
assert.match(movelistResult.text, /Axe swing: Swings its stone axe in a wide arc/i);
assert.match(movelistResult.text, /Axe swing note: Can be blocked, parried, and dodge rolled/i);
assert.doesNotMatch(movelistResult.text, /## Footnotes/i);

const genericResult = buildChunksFromExtractedPage(genericTemplatePage, { limit: 400 });
assert.match(genericResult.text, /## Overview/i);
assert.match(genericResult.text, /Forest' Bee has since migrated/i);
assert.match(genericResult.text, /## Location/i);
assert.match(genericResult.text, /Beehives/i);
assert.match(genericResult.text, /## Recipes/i);
assert.match(genericResult.text, /Common During Daytime -> Verdant \(40%\)/i);
assert.match(genericResult.text, /## Attributes/i);
assert.match(genericResult.text, /Species: Forest/i);
assert.ok(genericResult.chunks.length >= 1);

const positionalInfoboxResult = buildChunksFromExtractedPage(positionalInfoboxPage, { limit: 400 });
assert.ok(!('1' in positionalInfoboxResult.infobox));

console.log('wiki-wikitext smoke test passed');
const cheerio = require('cheerio');
const { slugifyTitle, splitTextIntoChunks } = require('./wiki-chunker');

const STOP_SECTION_HEADINGS = new Set([
  'achievements',
  'bugs',
  'changelog',
  'external links',
  'footnotes',
  'gallery',
  'history',
  'references',
  'see also',
  'trivia',
]);

const GENERIC_TEMPLATE_SKIP = [
  /^#/, /^card\b/i, /^thumbnail\b/i, /^stub\b/i, /^history\b/i,
  /^nav\b/i, /navbox/i, /^quote\b/i, /^gallery\b/i, /^main\b/i,
  /^displaytitle\b/i, /infobox\b/i, /portable infobox/i, /sidebar\b/i,
  /^itemdetails\b/i, /^movelisttable(?:\/start|\/end)?\b/i,
];

const GENERIC_TEMPLATE_PROSE_FIELDS = [
  ['Overview', ['description', 'summary', 'overview', 'compendium', 'intro', 'about', 'quote', 'quote_flv']],
  ['Location', ['found', 'location', 'locations', 'where_found', 'habitat']],
  ['Behavior', ['behavior', 'behaviour']],
  ['Uses', ['use', 'uses', 'effect', 'effects']],
  ['Mechanics', ['mechanics', 'strategy']],
  ['Hint', ['hint', 'hints']],
  ['Special', ['special', 'ability', 'abilities']],
  ['Notes', ['notes', 'note', 'tips']],
  ['Trivia', ['trivia']],
];

const GENERIC_TEMPLATE_SKIP_FIELDS = new Set([
  'image', 'img', 'caption', 'class', 'style', 'showcase', 'external', 'icon',
  'collapsed', 'identifier', 'spoiler', 'dlc', 'category', 'categories', 'id',
]);

function decodeEntities(value) {
  if (!value) return '';
  return cheerio.load(`<div>${value}</div>`).text();
}

function normalizeWhitespace(value) {
  return decodeEntities(String(value || ''))
    .replace(/\u00a0/g, ' ')
    .replace(/\r/g, '')
    .replace(/[\t ]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function replaceKnownInlineTemplates(value, pageTitle = '') {
  let output = String(value || '');
  let previous = null;
  const pageName = String(pageTitle || '').trim();

  while (output !== previous) {
    previous = output;
    output = output
      .replace(/\{\{\s*PAGENAME\s*\}\}/gi, pageName || 'this item')
      .replace(/\{\{\s*(?:Color|color)\s*\|[^|{}]*\|([^{}]+?)\s*\}\}/g, '$1')
      .replace(/\{\{\s*(?:Color|color)\s*\|([^{}]+?)\s*\}\}/g, '$1')
      .replace(/\{\{\s*(?:Item|Mutation|Outfit|Enemy|Biome|Skill|Shield|Weapon|Gear|NPC|Location|Blueprint|Aspect)\s*\|([^|{}]+?)(?:\|[^{}]*?)?\s*\}\}/g, '$1')
      .replace(/\{\{\s*i\s*\|([^|{}]+?)\|([^{}]+?)\s*\}\}/gi, '$2')
      .replace(/\{\{\s*i\s*\|([^{}]+?)\s*\}\}/gi, '$1')
      .replace(/\{\{\s*c\s*\|(?:[^|{}]+\|)+([^{}|]+?)\s*\}\}/gi, '$1')
      .replace(/\{\{\s*IconLink\s*\|[^{}]*?link\s*=\s*([^|{}]+?)(?:\|[^{}]*?)?\s*\}\}/gi, '$1')
      .replace(/\{\{\s*Stat\s*\|([^|{}]+?)(?:\|[^{}]*?)?\s*\}\}/g, '$1')
      .replace(/\{\{\s*efn\s*\|([^{}]+?)\s*\}\}/gi, '($1)')
      .replace(/\{\{\s*DLC\s*\|[^{}]+?\s*\}\}/gi, '')
      .replace(/\{\{\s*!\s*\}\}/g, '|');
  }

  return output;
}

function cleanInlineWikitext(value, pageTitle = '') {
  return normalizeWhitespace(replaceKnownInlineTemplates(String(value || ''), pageTitle))
    .replace(/<!--([\s\S]*?)-->/g, ' ')
    .replace(/<ref[^>]*>[\s\S]*?<\/ref>/gi, ' ')
    .replace(/<ref[^/>]*\/>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\[https?:\/\/[^\s\]]+\s+([^\]]+)\]/g, '$1')
    .replace(/\[https?:\/\/([^\]\s,]+),?\s*\]/g, '$1')
    .replace(/\[https?:\/\/[^\]]+\]/g, ' ')
    .replace(/\[\[(?:File|Image|Category):[^\]]*\]\]/gi, ' ')
    .replace(/\[\[([^\]|]+)\|([^\]]+)\]\]/g, '$2')
    .replace(/\[\[([^\]]+)\]\]/g, '$1')
    .replace(/'''([^']+?)'''/g, '$1')
    .replace(/''([^']+?)''/g, '$1')
    .replace(/__[^_]+__/g, ' ')
    .replace(/\{\|[^]*?\|\}/g, ' ')
    .replace(/\{\{[^]*?\}\}/g, ' ')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function stripBalancedTemplates(input) {
  const text = String(input || '');
  let out = '';
  let index = 0;
  let depth = 0;

  while (index < text.length) {
    const nextOpen = text.indexOf('{{', index);
    if (nextOpen === -1) {
      if (depth === 0) out += text.slice(index);
      break;
    }

    if (depth === 0) out += text.slice(index, nextOpen);
    index = nextOpen;
    while (index < text.length) {
      if (text.startsWith('{{', index)) {
        depth++;
        index += 2;
        continue;
      }
      if (text.startsWith('}}', index)) {
        depth = Math.max(0, depth - 1);
        index += 2;
        if (depth === 0) break;
        continue;
      }
      index++;
    }
  }

  return out;
}

function extractTopLevelTemplateBlocks(text) {
  const blocks = [];
  let index = 0;
  while (index < text.length) {
    const start = text.indexOf('{{', index);
    if (start === -1) break;
    let depth = 0;
    let end = start;
    while (end < text.length) {
      if (text.startsWith('{{', end)) {
        depth++;
        end += 2;
        continue;
      }
      if (text.startsWith('}}', end)) {
        depth--;
        end += 2;
        if (depth === 0) {
          blocks.push({ start, end, raw: text.slice(start, end) });
          break;
        }
        continue;
      }
      end++;
    }
    index = end;
  }
  return blocks;
}

function splitTopLevelPipes(input) {
  const text = String(input || '');
  const parts = [];
  let current = '';
  let templateDepth = 0;
  let linkDepth = 0;
  let tableDepth = 0;

  for (let index = 0; index < text.length; index++) {
    if (text.startsWith('{{', index)) {
      templateDepth++;
      current += '{{';
      index++;
      continue;
    }
    if (text.startsWith('}}', index)) {
      templateDepth = Math.max(0, templateDepth - 1);
      current += '}}';
      index++;
      continue;
    }
    if (text.startsWith('[[', index)) {
      linkDepth++;
      current += '[[';
      index++;
      continue;
    }
    if (text.startsWith(']]', index)) {
      linkDepth = Math.max(0, linkDepth - 1);
      current += ']]';
      index++;
      continue;
    }
    if (text.startsWith('{|', index)) {
      tableDepth++;
      current += '{|';
      index++;
      continue;
    }
    if (text.startsWith('|}', index)) {
      tableDepth = Math.max(0, tableDepth - 1);
      current += '|}';
      index++;
      continue;
    }
    if (text[index] === '|' && templateDepth === 0 && linkDepth === 0 && tableDepth === 0) {
      parts.push(current);
      current = '';
      continue;
    }
    current += text[index];
  }

  parts.push(current);
  return parts;
}

function parseTemplateBlock(rawOrInner, pageTitle = '') {
  const source = String(rawOrInner || '');
  const inner = source.trim().startsWith('{{') ? source.trim().slice(2, -2).trim() : source.trim();
  const normalizedInner = inner.replace(/^\s+/, '');
  const firstPipeIndex = normalizedInner.indexOf('|');
  const rawName = firstPipeIndex === -1 ? normalizedInner : normalizedInner.slice(0, firstPipeIndex);
  const remainder = firstPipeIndex === -1 ? '' : normalizedInner.slice(firstPipeIndex + 1);
  const segments = splitTopLevelPipes(remainder).map(segment => segment.replace(/\r/g, '')).filter(Boolean);
  const name = cleanInlineWikitext(rawName.replace(/:$/, '').trim(), pageTitle)
    .toLowerCase()
    .replace(/\s+/g, ' ');

  const params = {};
  let positional = 0;

  for (const segment of segments) {
    const trimmed = segment.trim();
    if (!trimmed) continue;
    const eqIndex = trimmed.indexOf('=');
    if (eqIndex === -1) {
      positional++;
      params[String(positional)] = trimmed;
      continue;
    }

    const key = cleanInlineWikitext(trimmed.slice(0, eqIndex), pageTitle)
      .toLowerCase()
      .replace(/\s+/g, '_');
    if (!key) continue;
    params[key] = trimmed.slice(eqIndex + 1).trim();
  }

  return { name, params };
}

function parseTemplateParameters(rawOrInner, pageTitle = '') {
  return parseTemplateBlock(rawOrInner, pageTitle).params;
}

function getTemplateHeader(raw, pageTitle = '') {
  const source = String(raw || '').replace(/^\s*\{\{\s*/, '');
  const first = source.split(/[\n|]/).map(part => part.trim()).find(Boolean) || '';
  return cleanInlineWikitext(first.replace(/:$/, ''), pageTitle)
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

function humanizeFieldName(key) {
  return String(key || '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, match => match.toUpperCase())
    .trim();
}

function renderBulletLines(value, pageTitle = '') {
  const lines = [];
  for (const rawLine of String(value || '').split('\n')) {
    const trimmed = rawLine.trim();
    if (!trimmed) continue;

    if (/^[*#;:]+/.test(trimmed)) {
      const item = cleanInlineWikitext(trimmed.replace(/^[*#;:]+/, ''), pageTitle);
      if (item) lines.push(`- ${item}`);
      continue;
    }

    const cleaned = cleanInlineWikitext(trimmed, pageTitle);
    if (cleaned) lines.push(`- ${cleaned}`);
  }
  return lines;
}

function renderItemDetailsTemplate(raw, pageTitle = '') {
  const { params } = parseTemplateBlock(raw, pageTitle);
  const lines = [];
  const consumed = new Set();

  if (params.special) {
    lines.push(...renderBulletLines(params.special, pageTitle));
    consumed.add('special');
  }

  const preferredFields = [
    'combo_type', 'ammo', 'duration', 'dmg_reduction',
    'breach_bonus', 'breach_dmg', 'breach_dps',
    'first_charge', 'first_lock', 'first_cooldown',
    'second_charge', 'second_lock', 'second_cooldown',
    'third_charge', 'third_lock', 'third_cooldown',
    'tags',
  ];

  for (const key of preferredFields) {
    const value = cleanInlineWikitext(params[key], pageTitle);
    if (!value) continue;
    lines.push(`- ${humanizeFieldName(key)}: ${value}`);
    consumed.add(key);
  }

  const forcedAffix = cleanInlineWikitext(params.forced_affix, pageTitle);
  const affixQuote = cleanInlineWikitext(params.affix_quote, pageTitle);
  if (forcedAffix) {
    lines.push(`- Legendary affix: ${forcedAffix}${affixQuote ? ` - ${affixQuote}` : ''}`);
    consumed.add('forced_affix');
    consumed.add('affix_quote');
  }

  for (const [key, rawValue] of Object.entries(params)) {
    if (consumed.has(key) || key === 'legendary') continue;
    const value = cleanInlineWikitext(rawValue, pageTitle);
    if (!value) continue;
    lines.push(`- ${humanizeFieldName(key)}: ${value}`);
  }

  return lines.length ? `\n${lines.join('\n')}\n` : '\n';
}

function renderMovelistTableTemplate(raw, pageTitle = '') {
  const { params } = parseTemplateBlock(raw, pageTitle);
  const moveName = cleanInlineWikitext(params.name, pageTitle);
  const description = cleanInlineWikitext(params.description, pageTitle);
  const notes = renderBulletLines(params.notes, pageTitle);
  const lines = [];

  if (moveName && description) lines.push(`- ${moveName}: ${description}`);
  else if (moveName) lines.push(`- ${moveName}`);
  else if (description) lines.push(`- ${description}`);

  for (const note of notes) {
    const noteText = note.replace(/^-\s*/, '');
    if (!noteText) continue;
    if (moveName) lines.push(`- ${moveName} note: ${noteText}`);
    else lines.push(`- ${noteText}`);
  }

  return lines.length ? `\n${lines.join('\n')}\n` : '\n';
}

function renderGenericTemplateSection(values, pageTitle = '') {
  const blocks = [];
  for (const value of values) {
    if (!value) continue;
    const bulletLines = renderBulletLines(value, pageTitle);
    if (bulletLines.length > 1) {
      blocks.push(bulletLines.join('\n'));
      continue;
    }
    const cleaned = cleanInlineWikitext(value, pageTitle);
    if (cleaned) blocks.push(cleaned);
  }
  return blocks;
}

function renderGenericRecipeGroups(params, pageTitle = '') {
  const groups = new Map();
  for (const [key, value] of Object.entries(params)) {
    const match = key.match(/^recipe(\d+)_(.+)$/);
    if (!match) continue;
    const index = match[1];
    const field = match[2];
    if (!groups.has(index)) groups.set(index, {});
    groups.get(index)[field] = value;
  }

  const lines = [];
  for (const group of Array.from(groups.entries()).sort((a, b) => Number(a[0]) - Number(b[0])).map(entry => entry[1])) {
    const source = cleanInlineWikitext(group.bee || group.parent || group.input, pageTitle);
    const conditions = cleanInlineWikitext(group.conditions || group.condition, pageTitle);
    const outcome = cleanInlineWikitext(group.outcome || group.result, pageTitle);
    const chance = cleanInlineWikitext(group.chance || group.percent, pageTitle);
    const parts = [];
    if (source) parts.push(source);
    if (conditions) parts.push(conditions.toLowerCase().startsWith('during') || conditions.toLowerCase().startsWith('while') ? conditions : `when ${conditions}`);
    const lhs = parts.join(' ');
    const rhs = outcome || cleanInlineWikitext(group.product || '', pageTitle);
    if (!lhs && !rhs) continue;
    lines.push(`- ${lhs || 'Recipe'}${rhs ? ` -> ${rhs}` : ''}${chance ? ` (${chance}%)` : ''}`.trim());
  }

  return lines;
}

function shouldRenderGenericTemplate(name, params) {
  if (!name) return false;
  if (GENERIC_TEMPLATE_SKIP.some(pattern => pattern.test(name))) return false;
  const keys = Object.keys(params).filter(key => !/^\d+$/.test(key));
  if (keys.length < 3) return false;
  const totalText = keys.reduce((sum, key) => sum + cleanInlineWikitext(params[key]).length, 0);
  return totalText >= 80;
}

function renderGenericTemplate(raw, pageTitle = '') {
  const { name, params } = parseTemplateBlock(raw, pageTitle);
  if (!shouldRenderGenericTemplate(name, params)) return raw;

  const lines = [];
  const consumed = new Set();

  for (const [heading, keys] of GENERIC_TEMPLATE_PROSE_FIELDS) {
    const values = [];
    for (const key of keys) {
      if (!params[key]) continue;
      values.push(params[key]);
      consumed.add(key);
    }
    const blocks = renderGenericTemplateSection(values, pageTitle);
    if (!blocks.length) continue;
    lines.push(`## ${heading}`);
    lines.push(blocks.join('\n\n'));
  }

  const recipeLines = renderGenericRecipeGroups(params, pageTitle);
  if (recipeLines.length) {
    for (const key of Object.keys(params)) {
      if (/^recipe\d+_/.test(key)) consumed.add(key);
    }
    lines.push('## Recipes');
    lines.push(recipeLines.join('\n'));
  }

  const attributeLines = [];
  for (const [key, rawValue] of Object.entries(params)) {
    if (consumed.has(key) || /^\d+$/.test(key) || GENERIC_TEMPLATE_SKIP_FIELDS.has(key)) continue;
    const value = cleanInlineWikitext(rawValue, pageTitle);
    if (!value) continue;
    attributeLines.push(`- ${humanizeFieldName(key)}: ${value}`);
  }
  if (attributeLines.length) {
    lines.push('## Attributes');
    lines.push(attributeLines.join('\n'));
  }

  return lines.length ? `\n${lines.join('\n\n')}\n` : '\n';
}

function replaceSupportedTemplateBlocks(text, pageTitle = '') {
  const blocks = extractTopLevelTemplateBlocks(String(text || ''));
  if (!blocks.length) return String(text || '');

  let output = '';
  let cursor = 0;

  for (const block of blocks) {
    output += text.slice(cursor, block.start);
    const name = getTemplateHeader(block.raw, pageTitle);

    if (/^itemdetails\b/i.test(name)) {
      output += renderItemDetailsTemplate(block.raw, pageTitle);
    } else if (/^movelisttable\/(?:start|end)\b/i.test(name)) {
      output += '\n';
    } else if (/^movelisttable\b/i.test(name)) {
      output += renderMovelistTableTemplate(block.raw, pageTitle);
    } else {
      output += renderGenericTemplate(block.raw, pageTitle);
    }

    cursor = block.end;
  }

  output += text.slice(cursor);
  return output;
}

function extractInfobox(text, pageTitle = '') {
  const blocks = extractTopLevelTemplateBlocks(text);
  for (const block of blocks) {
    const name = getTemplateHeader(block.raw, pageTitle);
    const params = parseTemplateParameters(block.raw, pageTitle);
    if (!/(infobox\b|portable infobox|sidebar)/i.test(name)) continue;

    const infobox = {};
    for (const [key, rawValue] of Object.entries(params)) {
      const normalizedKey = String(key || '').trim();
      if (!normalizedKey || /^\d+$/.test(normalizedKey)) continue;
      const value = cleanInlineWikitext(rawValue, pageTitle);
      if (value && normalizedKey.length <= 80 && value.length <= 300) {
        infobox[normalizedKey] = value;
      }
    }

    return {
      infobox,
      text: text.slice(0, block.start) + text.slice(block.end),
    };
  }

  return { infobox: {}, text };
}

function extractTables(text, pageTitle = '') {
  const tables = [];
  let output = '';
  let index = 0;

  while (index < text.length) {
    const start = text.indexOf('{|', index);
    if (start === -1) {
      output += text.slice(index);
      break;
    }

    output += text.slice(index, start);
    const end = text.indexOf('|}', start + 2);
    if (end === -1) {
      output += text.slice(start);
      break;
    }

    const raw = text.slice(start, end + 2);
    const marker = `__TABLE_${tables.length}__`;
    tables.push({ marker, raw, text: convertTableWikitext(raw, pageTitle) });
    output += `\n${marker}\n`;
    index = end + 2;
  }

  return { text: output, tables };
}

function stripTableCellAttributes(cell) {
  let value = String(cell || '').trim().replace(/^[!|]/, '').trim();
  const attrPattern = /^(?:(?:[a-z-]+)\s*=\s*(?:"[^"]*"|'[^']*'|[^\s|]+)\s*)+\|\s*/i;
  let previous = null;
  while (value && value !== previous) {
    previous = value;
    value = value.replace(attrPattern, '').trim();
  }
  return value;
}

function splitCells(line, delimiterRegex, pageTitle = '') {
  return line
    .split(delimiterRegex)
    .map(cell => cleanInlineWikitext(stripTableCellAttributes(cell), pageTitle))
    .filter(Boolean);
}

function looksDecorativeTableRow(cells) {
  if (!cells.length) return true;
  const joined = cells.join(' ').trim();
  if (!joined) return true;
  if (/^(?:sort|expand|collapse)$/i.test(joined)) return true;
  return false;
}

function convertTableWikitext(raw, pageTitle = '') {
  const lines = String(raw || '').split('\n');
  const out = [];
  let headers = [];
  let caption = '';

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed === '{|' || trimmed === '|}' || trimmed === '|-') continue;
    if (trimmed.startsWith('|+')) {
      caption = cleanInlineWikitext(stripTableCellAttributes(trimmed.slice(2)), pageTitle);
      continue;
    }
    if (trimmed.startsWith('!')) {
      headers = splitCells(trimmed, /!!/, pageTitle);
      continue;
    }
    if (trimmed.startsWith('|')) {
      const cells = splitCells(trimmed, /\|\|/, pageTitle);
      if (looksDecorativeTableRow(cells)) continue;
      if (cells.length === 1) {
        out.push(`- ${cells[0]}`);
        continue;
      }
      if (cells.length === 2) {
        out.push(`- ${cells[0]}: ${cells[1]}`);
        continue;
      }
      if (headers.length && headers.length === cells.length) {
        out.push(`- ${cells.map((item, idx) => `${headers[idx]}: ${item}`).join('; ')}`);
        continue;
      }
      out.push(`- ${cells.join(' | ')}`);
    }
  }

  if (!out.length) return '';
  return [caption ? `### ${caption}` : '', out.join('\n')].filter(Boolean).join('\n');
}

function removeNoiseBlocks(text) {
  return String(text || '')
    .replace(/<!--([\s\S]*?)-->/g, ' ')
    .replace(/<ref[^>]*>[\s\S]*?<\/ref>/gi, ' ')
    .replace(/<ref[^/>]*\/>/gi, ' ')
    .replace(/<gallery[^>]*>[\s\S]*?<\/gallery>/gi, ' ')
    .replace(/<nowiki[^>]*>[\s\S]*?<\/nowiki>/gi, ' ')
    .replace(/<noinclude[^>]*>[\s\S]*?<\/noinclude>/gi, ' ')
    .replace(/<includeonly[^>]*>[\s\S]*?<\/includeonly>/gi, ' ')
    .replace(/\[\[(?:File|Image):[^\]]*\]\]/gi, ' ')
    .replace(/\[\[Category:([^\]]+)\]\]/gi, ' ')
    .replace(/__NOTOC__|__TOC__|__NOINDEX__|__NOEDITSECTION__|__NOTITLE__/gi, ' ');
}

function replaceTables(text, tables) {
  let output = String(text || '');
  for (const table of tables) {
    output = output.replace(table.marker, table.text || ' ');
  }
  return output;
}

function compactEmptyHeadings(blocks) {
  const compacted = [];
  for (let index = 0; index < blocks.length; index++) {
    const block = blocks[index];
    if (!/^##+#?\s/.test(block)) {
      compacted.push(block);
      continue;
    }

    const next = blocks[index + 1] || '';
    if (!next || /^##+#?\s/.test(next)) continue;
    compacted.push(block);
  }
  return compacted;
}

function scoreExtraction(text, infobox, warnings, stats) {
  let score = 0.4;
  if (text.length >= 120) score += 0.08;
  if (text.length >= 400) score += 0.18;
  if (text.length >= 1200) score += 0.15;
  if (Object.keys(infobox).length >= 3) score += 0.1;
  if ((stats.tablesConverted || 0) > 0) score += 0.05;
  if ((stats.sectionsKept || 0) >= 2) score += 0.05;

  const noiseMatches = (text.match(/\b(template|navbox|citation needed|file:|category:)\b/gi) || []).length;
  if (noiseMatches > 0) {
    warnings.push(`Residual wiki noise detected (${noiseMatches})`);
    score -= Math.min(0.2, noiseMatches * 0.03);
  }

  const structuralNoiseMatches = (text.match(/\b(?:style|colspan|rowspan|scope|class)\s*=\s*(?:"[^"]*"|'[^']*'|[^\s]+)/gi) || []).length;
  if (structuralNoiseMatches > 0) {
    warnings.push(`Residual table formatting noise detected (${structuralNoiseMatches})`);
    score -= Math.min(0.25, structuralNoiseMatches * 0.05);
  }

  if (text.length < 120) warnings.push('Very short extracted text');
  if (text.length < 120) score -= 0.12;
  if ((stats.tablesConverted || 0) === 0 && !Object.keys(infobox).length) warnings.push('No infobox or table metadata found');

  return Math.max(0.05, Math.min(0.99, Number(score.toFixed(2))));
}

function extractFromWikitext(page) {
  const title = page.title || 'wiki';
  const warnings = [];
  let source = String(page.wikitext || '');

  if (!source.trim()) {
    return {
      pageTitle: title,
      infobox: {},
      text: '',
      warnings: ['Missing page content'],
      qualityScore: 0.05,
      stats: { sectionsKept: 0, tablesConverted: 0 },
    };
  }

  source = removeNoiseBlocks(source);
  const infoboxResult = extractInfobox(source, title);
  source = infoboxResult.text;
  const tablesResult = extractTables(source, title);
  source = tablesResult.text;
  source = replaceSupportedTemplateBlocks(source, title);
  source = replaceKnownInlineTemplates(source, title);
  source = stripBalancedTemplates(source);
  source = replaceTables(source, tablesResult.tables);

  const lines = source.split('\n');
  const blocks = [];
  let skipLevel = null;
  let sectionsKept = 0;

  for (let rawLine of lines) {
    const headingMatch = rawLine.match(/^(={2,6})\s*(.*?)\s*\1\s*$/);
    if (headingMatch) {
      const level = headingMatch[1].length;
      const heading = cleanInlineWikitext(headingMatch[2], title);
      if (!heading) continue;

      if (skipLevel && level <= skipLevel) skipLevel = null;
      if (STOP_SECTION_HEADINGS.has(heading.toLowerCase())) {
        skipLevel = level;
        continue;
      }
      if (skipLevel) continue;

      sectionsKept++;
      blocks.push(`${level <= 2 ? '##' : '###'} ${heading}`);
      continue;
    }

    if (skipLevel) continue;

    rawLine = rawLine.trim();
    if (!rawLine) continue;

    if (/^#{2,3}\s/.test(rawLine)) {
      blocks.push(rawLine.trim());
      continue;
    }

    if (/^[*#]+/.test(rawLine)) {
      const item = cleanInlineWikitext(rawLine.replace(/^[*#;:]+/, ''), title);
      if (item) blocks.push(`- ${item}`);
      continue;
    }

    if (/^;/.test(rawLine)) {
      const term = cleanInlineWikitext(rawLine.replace(/^;+/, ''), title);
      if (term) blocks.push(`**${term}**`);
      continue;
    }

    const cleaned = cleanInlineWikitext(rawLine, title);
    if (cleaned) blocks.push(cleaned);
  }

  const deduped = [];
  for (const block of blocks) {
    if (!block) continue;
    if (deduped[deduped.length - 1] === block) continue;
    deduped.push(block);
  }

  const compacted = compactEmptyHeadings(deduped);
  const text = normalizeWhitespace(compacted.join('\n\n').replace(/\n{3,}/g, '\n\n'));
  const stats = {
    sectionsKept,
    tablesConverted: tablesResult.tables.filter(table => table.text).length,
  };
  const qualityScore = scoreExtraction(text, infoboxResult.infobox, warnings, stats);

  return {
    pageTitle: title,
    infobox: infoboxResult.infobox,
    text,
    warnings,
    qualityScore,
    stats,
  };
}

function buildChunksFromExtractedPage(page, options = {}) {
  const extracted = extractFromWikitext(page);
  const baseId = slugifyTitle(extracted.pageTitle || page.title || 'wiki');
  const chunks = splitTextIntoChunks(extracted.text, baseId, options.limit || 1800);
  return {
    ...extracted,
    baseId,
    chunks,
  };
}

module.exports = {
  buildChunksFromExtractedPage,
  cleanInlineWikitext,
  extractFromWikitext,
};

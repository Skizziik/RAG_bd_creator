const cheerio = require('cheerio');

const UA = 'TryllDatasetBuilder/1.3 (https://github.com/Skizziik/json_creator)';

// Try to detect MediaWiki site and extract page name from URL
function detectMediaWiki(url) {
  const u = new URL(url);
  // Match /wiki/PageName or /w/PageName (minecraft.wiki uses /w/)
  const match = u.pathname.match(/\/(?:wiki|w)\/(.+)/);
  if (!match) return null;
  return {
    apiBase: `${u.origin}/api.php`,
    pageName: decodeURIComponent(match[1]).replace(/_/g, ' '),
  };
}

// Fetch via MediaWiki API (works on wiki.gg, Fandom, minecraft.wiki, Wikipedia)
async function fetchViaMediaWikiAPI(apiBase, pageName) {
  const params = new URLSearchParams({
    action: 'parse',
    page: pageName,
    format: 'json',
    prop: 'text|displaytitle',
    disablelimitreport: '1',
    disableeditsection: '1',
  });
  const res = await fetch(`${apiBase}?${params}`, { headers: { 'User-Agent': UA } });
  if (!res.ok) return null;
  const json = await res.json();
  if (json.error) return null;
  return {
    html: json.parse.text['*'],
    title: json.parse.displaytitle || json.parse.title || pageName,
  };
}

// Fallback: direct HTML fetch for non-wiki sites
async function fetchDirectHTML(url) {
  const res = await fetch(url, {
    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36' },
  });
  if (!res.ok) throw new Error(`Failed to fetch: HTTP ${res.status}`);
  return { html: await res.text(), title: null };
}

// Sections to skip (content after these headings is dropped until next h2)
const SKIP_SECTIONS = [
  'history', 'changelog', 'references', 'external links', 'see also',
  'gallery', 'trivia', 'crafting', 'recipes', 'achievements', 'set',
  'notes', 'bugs', 'effects', 'synergies',
];

// Convert a simple table (th/td rows) to "Key: Value" lines
function tableToText($, $table) {
  const lines = [];
  $table.find('tr').each((_, tr) => {
    const cells = [];
    $(tr).children('th, td').each((_, cell) => {
      const t = cleanInline($, $(cell));
      if (t) cells.push(t);
    });
    if (cells.length >= 2) {
      lines.push(`${cells[0]}: ${cells.slice(1).join(' | ')}`);
    } else if (cells.length === 1) {
      lines.push(cells[0]);
    }
  });
  return lines.join('\n');
}

// Clean infobox key/value: strip references, extra whitespace
function cleanInfoVal(str) {
  return String(str || '')
    .replace(/\[(?:\d+|edit|citation needed)\]/gi, '')
    .replace(/\s+/g, ' ')
    .trim();
}

// Clean inline text: collapse whitespace, strip reference markers [1], trim
function cleanInline($, $el) {
  return $el.text()
    .replace(/\[(?:\d+|edit|citation needed)\]/gi, '')
    .replace(/\t/g, ' ')
    .replace(/[ ]{2,}/g, ' ')
    .replace(/\n+/g, ' ')
    .trim();
}

function setInfoValue(infobox, key, val) {
  const cleanKey = cleanInfoVal(key);
  const cleanVal = cleanInfoVal(val);
  if (!cleanKey || !cleanVal) return;
  if (cleanKey.length >= 80 || cleanVal.length >= 240) return;
  infobox[cleanKey] = cleanVal;
}

function extractPairsFromRow($, $row) {
  const cells = $row.children('th, td').toArray().map(cell => ({
    tag: (cell.tagName || '').toLowerCase(),
    text: cleanInline($, $(cell)),
    colspan: Number($(cell).attr('colspan') || 1),
  })).filter(cell => cell.text);

  const pairs = [];
  if (cells.length < 2) return pairs;

  for (let index = 0; index < cells.length - 1; index++) {
    const current = cells[index];
    const next = cells[index + 1];
    if (current.tag === 'th' && next.tag === 'td') {
      pairs.push([current.text, next.text]);
      index++;
      continue;
    }
  }

  if (pairs.length) return pairs;

  if (cells.length === 2) {
    pairs.push([cells[0].text, cells[1].text]);
    return pairs;
  }

  // Fallback for layouts that pack multiple label/value cells in one row.
  if (cells.length % 2 === 0 && cells.length <= 8) {
    for (let index = 0; index < cells.length - 1; index += 2) {
      pairs.push([cells[index].text, cells[index + 1].text]);
    }
  }

  return pairs;
}

function canBeSegmentedByKnownKeys(key, otherKeys) {
  const target = key.toLowerCase().replace(/[^a-z0-9]+/g, '');
  if (!target || otherKeys.length < 2) return false;
  const normalizedParts = otherKeys
    .map(part => part.toLowerCase().replace(/[^a-z0-9]+/g, ''))
    .filter(Boolean)
    .filter(part => part !== target);
  if (!normalizedParts.length) return false;

  const best = new Array(target.length + 1).fill(-Infinity);
  best[0] = 0;
  for (let index = 0; index < target.length; index++) {
    if (best[index] < 0) continue;
    for (const part of normalizedParts) {
      if (target.startsWith(part, index)) {
        best[index + part.length] = Math.max(best[index + part.length], best[index] + 1);
      }
    }
  }
  return best[target.length] >= 2;
}

function sanitizeInfobox(infobox) {
  const result = { ...infobox };
  const keys = Object.keys(result);
  for (const key of keys) {
    if (!/[A-Za-z].*[A-Z]/.test(key.replace(/\s+/g, ''))) continue;
    if (canBeSegmentedByKnownKeys(key, Object.keys(result))) {
      delete result[key];
    }
  }
  return result;
}

function extractFromHTML(html, fallbackTitle) {
  const $ = cheerio.load(html);

  // Title: strip HTML tags from fallback, then try <title>, <h1>
  const pageTitle = (fallbackTitle ? cheerio.load(fallbackTitle).text().trim() : '')
    || $('title').first().text().trim()
    || $('h1').first().text().trim()
    || '';

  // Extract infobox metadata (works with wiki.gg, Fandom, Wikipedia infobox formats)
  const infobox = {};
  $('.infobox tr, .sidebar tr, .wikitable.infobox tr, table.infobox tr').each((_, el) => {
    const pairs = extractPairsFromRow($, $(el));
    for (const [key, val] of pairs) setInfoValue(infobox, key, val);
  });

  $('.pi-item').each((_, el) => {
    const $el = $(el);
    const key = $el.find('.pi-data-label').first().text().trim();
    const val = $el.find('.pi-data-value').first().text().trim();
    setInfoValue(infobox, key, val);
  });

  // ATLYSS / Loot Goblin-style infobox (wiki.gg custom layout)
  $('.lg-container .lg-row').each((_, el) => {
    const $row = $(el);
    const key = $row.children('.lg-label').first().text().trim();
    const val = $row.children('.lg-data').first().text().trim();
    setInfoValue(infobox, key, val);
  });

  // Druid infobox (wiki.gg custom format, e.g. mewgenics.wiki.gg)
  $('.druid-infobox .druid-label').each((_, el) => {
    const $label = $(el);
    const key = $label.text().trim().replace(/\s+/g, ' ');
    const val = $label.siblings('.druid-data').text().trim().replace(/\s+/g, ' ');
    setInfoValue(infobox, key, val);
  });

  const sanitizedInfobox = sanitizeInfobox(infobox);

  // Remove hidden elements (display:none, visibility:hidden)
  $('[style*="display: none"], [style*="display:none"], [style*="visibility: hidden"], [style*="visibility:hidden"]').remove();

  // Remove noise: navigation, templates, infoboxes, tables, footer junk
  $([
    'script', 'style', 'nav', 'footer', 'header', 'noscript',
    '.sidebar', '.infobox', '.navbox', '.navbox-container',
    '.portable-infobox', '.pi-item', '.druid-infobox',
    '.mw-editsection', '.reference', '.reflist', '.refbegin',
    '#mw-navigation', '.noprint', '.toc', '.catlinks',
    '.mw-indicators', '.vector-body-before-content',
    '.mbox-small', '.ambox', '.cmbox', '.ombox', '.tmbox', '.fmbox',
    '.mw-empty-elt', '.mw-headline-anchor',
    '.terraria', '.card', '.recipes', '.crafts',
    '.item-list', '.npc-list', '.entity-list',
    'table.terraria', 'table.crafts', 'table.sortable',
    '.gallery', '.mw-gallery-packed',
    'table.navbox', '.navbox-inner', '.navbox-list',
    '[data-navbox]', '.collapsible', '.mw-collapsible',
    '.hatnote', '.dablink', '.hat-note',
    '.msgbox', '.message-box', '.notice', '.eico', '.mbox',
    '.tt-content', '.tooltip-content', '.tooltiptext',
    '.thumbcaption', '.thumb', '.thumbinner', 'figcaption', 'figure',
    '.mw-default-size', '.floatright', '.floatleft', '.floatnone',
    '.searchaux', '.noexcerpt',
  ].join(', ')).remove();

  const mainContent = $('article, main, #mw-content-text, #content, .mw-parser-output, #bodyContent, .entry-content, .post-content').first();
  const root = mainContent.length ? mainContent : $('body');

  const blocks = [];
  let skipping = false;

  function processElement(el, $el, tag) {
    if (tag === 'h2' || tag === 'h3') {
      const heading = $el.text().trim().toLowerCase();
      if (SKIP_SECTIONS.includes(heading)) {
        skipping = true;
        return;
      }
      skipping = false;
      const hText = cleanInline($, $el);
      if (hText) blocks.push(`\n## ${hText}`);
      return;
    }

    if (skipping) return;

    if (tag === 'h4' || tag === 'h5') {
      const hText = cleanInline($, $el);
      if (hText) blocks.push(`\n### ${hText}`);
      return;
    }

    if (tag === 'p') {
      const pText = cleanInline($, $el);
      if (pText) blocks.push(pText);
      return;
    }

    if (tag === 'ul' || tag === 'ol') {
      const items = [];
      $el.children('li').each((_, li) => {
        const liText = cleanInline($, $(li));
        if (liText) items.push(`- ${liText}`);
      });
      if (items.length) blocks.push(items.join('\n'));
      return;
    }

    if (tag === 'dl') {
      $el.children('dt, dd').each((_, child) => {
        const cText = cleanInline($, $(child));
        if (cText) blocks.push(child.tagName === 'dt' ? `**${cText}**` : cText);
      });
      return;
    }

    if (tag === 'table') {
      const tText = tableToText($, $el);
      if (tText) blocks.push(tText);
      return;
    }
  }

  root.children().each((_, el) => {
    const $el = $(el);
    const tag = el.tagName?.toLowerCase();

    if (tag === 'div' || tag === 'section') {
      $el.find('> p, > ul, > ol, > dl, > h2, > h3, > h4, > h5, > table, > div > p, > div > ul, > div > ol, > div > table, > div > h2, > div > h3, > div > h4, > div > h5, > div > dl').each((_, inner) => {
        processElement(inner, $(inner), inner.tagName?.toLowerCase());
      });
      return;
    }

    processElement(el, $el, tag);
  });

  let text = blocks.join('\n\n').replace(/\n{3,}/g, '\n\n').trim();
  text = text.replace(/\n[a-z]{2}(-[a-z]+)?:.+$/gm, '').trim();

  return { text, pageTitle, infobox: sanitizedInfobox };
}

async function parseUrl(url) {
  let html, title;

  const mw = detectMediaWiki(url);
  if (mw) {
    const result = await fetchViaMediaWikiAPI(mw.apiBase, mw.pageName);
    if (result) {
      html = result.html;
      title = result.title;
    }
  }

  if (!html) {
    const result = await fetchDirectHTML(url);
    html = result.html;
    title = result.title;
  }

  const extracted = extractFromHTML(html, title);
  return { ...extracted, source: url };
}

module.exports = { parseUrl, detectMediaWiki, fetchViaMediaWikiAPI, extractFromHTML, UA };
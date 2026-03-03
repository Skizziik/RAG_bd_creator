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

function extractFromHTML(html, fallbackTitle) {
  const $ = cheerio.load(html);

  // Title: strip HTML tags from fallback, then try <title>, <h1>
  const pageTitle = (fallbackTitle ? cheerio.load(fallbackTitle).text().trim() : '')
    || $('title').first().text().trim()
    || $('h1').first().text().trim()
    || '';

  // Extract infobox metadata (works with wiki.gg, Fandom, Wikipedia infobox formats)
  const infobox = {};
  $('.infobox tr, .sidebar tr, .wikitable.infobox tr, table.infobox tr, .pi-item').each((_, el) => {
    const $el = $(el);
    let key, val;
    if ($el.hasClass('pi-item')) {
      // Fandom Portable Infobox format
      key = $el.find('.pi-data-label').text().trim().replace(/\s+/g, ' ');
      val = $el.find('.pi-data-value').text().trim().replace(/\s+/g, ' ');
    } else {
      key = $el.find('th').first().text().trim().replace(/\s+/g, ' ');
      val = $el.find('td').first().text().trim().replace(/\s+/g, ' ');
    }
    if (key && val && key.length < 60 && val.length < 200) {
      infobox[key] = val;
    }
  });

  // Remove noise: navigation, templates, infoboxes, tables, footer junk
  $([
    'script', 'style', 'nav', 'footer', 'header', 'noscript',
    '.sidebar', '.infobox', '.navbox', '.navbox-container',
    '.portable-infobox', '.pi-item',
    '.mw-editsection', '.reference', '.reflist', '.refbegin',
    '#mw-navigation', '.noprint', '.toc', '.catlinks',
    '.mw-indicators', '.vector-body-before-content',
    '.mbox-small', '.ambox', '.cmbox', '.ombox', '.tmbox', '.fmbox',
    '.mw-empty-elt', '.mw-headline-anchor',
    // Wiki.gg / Fandom specific junk
    '.terraria', '.card', '.recipes', '.crafts',
    '.item-list', '.npc-list', '.entity-list',
    'table.terraria', 'table.crafts', 'table.sortable',
    '.gallery', '.mw-gallery-packed',
    // Navigation templates at the bottom
    'table.navbox', '.navbox-inner', '.navbox-list',
    '[data-navbox]', '.collapsible', '.mw-collapsible',
    // "See also", "History", "Trivia" section markers we don't need
    '.hatnote', '.dablink',
    // Platform/version notice banners ("This is the main page...")
    '.msgbox', '.message-box', '.notice', '.eico', '.mbox',
  ].join(', ')).remove();

  // Remove "History" / "Changelog" / "References" sections and everything after
  $('h2, h3').each((_, el) => {
    const heading = $(el).text().trim().toLowerCase();
    if (['history', 'changelog', 'references', 'external links', 'see also', 'gallery', 'trivia', 'crafting', 'recipes', 'achievements', 'tips', 'set', 'notes', 'bugs'].includes(heading)) {
      // Remove this heading and all siblings after it
      $(el).nextAll().remove();
      $(el).remove();
    }
  });

  // Extract text from main content area
  const mainContent = $('article, main, #mw-content-text, #content, .mw-parser-output, #bodyContent, .entry-content, .post-content').first();
  let text = (mainContent.length ? mainContent : $('body')).text();

  // Clean whitespace and strip language links at the end
  text = text.replace(/\t/g, ' ').replace(/[ ]{2,}/g, ' ').replace(/\n{3,}/g, '\n\n').trim();
  // Remove trailing interlanguage links (e.g. "es-formal:Pico de cobre")
  text = text.replace(/\n[a-z]{2}(-[a-z]+)?:.+$/gm, '').trim();

  return { text, pageTitle, infobox };
}

async function parseUrl(url) {
  let html, title;

  // Try MediaWiki API first (bypasses Cloudflare, works on all wiki platforms)
  const mw = detectMediaWiki(url);
  if (mw) {
    const result = await fetchViaMediaWikiAPI(mw.apiBase, mw.pageName);
    if (result) {
      html = result.html;
      title = result.title;
    }
  }

  // Fallback to direct fetch
  if (!html) {
    const result = await fetchDirectHTML(url);
    html = result.html;
    title = result.title;
  }

  const extracted = extractFromHTML(html, title);
  return { ...extracted, source: url };
}

module.exports = { parseUrl };

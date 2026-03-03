const cheerio = require('cheerio');

async function parseUrl(url) {
  const res = await fetch(url, {
    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36' },
  });
  if (!res.ok) throw new Error(`Failed to fetch: HTTP ${res.status}`);
  const html = await res.text();
  const $ = cheerio.load(html);

  // Extract page title
  const pageTitle = $('title').first().text().trim()
    || $('h1').first().text().trim()
    || '';

  // Extract wiki infobox metadata
  const infobox = {};
  $('.infobox tr, .sidebar tr, .wikitable.infobox tr, table.infobox tr').each((_, row) => {
    const $row = $(row);
    const key = $row.find('th').first().text().trim().replace(/\s+/g, ' ');
    const val = $row.find('td').first().text().trim().replace(/\s+/g, ' ');
    if (key && val && key.length < 60 && val.length < 200) {
      infobox[key] = val;
    }
  });

  // Remove noise elements
  $('script, style, nav, footer, header, .sidebar, .infobox, .navbox, .mw-editsection, .reference, .reflist, #mw-navigation, .noprint, .toc').remove();

  // Extract main text
  const mainContent = $('article, main, #mw-content-text, #content, .mw-parser-output, #bodyContent, .entry-content, .post-content').first();
  let text = '';
  if (mainContent.length) {
    text = mainContent.text();
  } else {
    text = $('body').text();
  }

  // Clean up whitespace
  text = text
    .replace(/\t/g, ' ')
    .replace(/[ ]{2,}/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

  return { text, pageTitle, infobox, source: url };
}

module.exports = { parseUrl };

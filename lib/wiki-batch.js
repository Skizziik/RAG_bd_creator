const { detectMediaWiki, fetchViaMediaWikiAPI, extractFromHTML, UA } = require('./wiki');

// Retry fetch with fixed 5s delay for 429/5xx (up to 10 retries)
async function fetchWithRetry(url, opts = {}, retries = 10) {
  for (let i = 0; i <= retries; i++) {
    const res = await fetch(url, opts);
    if (res.ok) return res;
    if ((res.status === 429 || res.status >= 500) && i < retries) {
      await new Promise(r => setTimeout(r, 5000));
      continue;
    }
    throw new Error(`Wiki API error: HTTP ${res.status}`);
  }
}

const JUNK_PATTERNS = [
  'stub', 'stubs', 'maintenance', 'cleanup', 'missing', 'redirect',
  'template', 'user', 'talk', 'file', 'media', 'help', 'project',
  'portal', 'draft', 'module', 'mediawiki', 'disambiguation',
  'articles needing', 'pages with', 'pages using', 'pages lacking',
  'cs1', 'webarchive', 'short description', 'infobox',
  'coordinates', 'wikiproject', 'wikipedia', 'all pages', 'all articles',
];

function detectWikiFromUrl(url) {
  const u = new URL(url);
  const mw = detectMediaWiki(url);
  const apiBase = mw ? mw.apiBase : `${u.origin}/api.php`;
  // Extract human-readable name from hostname
  // terraria.wiki.gg → terraria, minecraft.wiki → minecraft, en.wikipedia.org → wikipedia
  const host = u.hostname.replace(/^(www|en)\./, '');
  const wikiName = host.split('.')[0];
  return { apiBase, wikiName };
}

async function fetchAllCategories(apiBase) {
  const categories = [];
  let continueToken = null;

  while (true) {
    const params = new URLSearchParams({
      action: 'query',
      list: 'allcategories',
      aclimit: '500',
      acprop: 'size|hidden',
      format: 'json',
    });
    if (continueToken) params.set('accontinue', continueToken);

    const res = await fetchWithRetry(`${apiBase}?${params}`, { headers: { 'User-Agent': UA } });
    const json = await res.json();
    if (json.error) throw new Error(`Wiki API error: ${json.error.info}`);

    for (const cat of json.query.allcategories) {
      categories.push({
        name: cat['*'] || cat.name,
        size: cat.size || 0,
        hidden: 'hidden' in cat,
      });
    }

    if (json.continue && json.continue.accontinue) {
      continueToken = json.continue.accontinue;
    } else {
      break;
    }
  }

  return categories;
}

function filterCategories(categories) {
  const accepted = [];
  const rejected = [];

  for (const cat of categories) {
    const lower = cat.name.toLowerCase();
    const isJunk = cat.hidden
      || cat.size === 0
      || JUNK_PATTERNS.some(p => lower.includes(p));

    if (isJunk) {
      rejected.push(cat);
    } else {
      accepted.push(cat);
    }
  }

  return { accepted, rejected };
}

async function fetchCategoryMembers(apiBase, categoryName) {
  const pages = [];
  let continueToken = null;

  while (true) {
    const params = new URLSearchParams({
      action: 'query',
      list: 'categorymembers',
      cmtitle: `Category:${categoryName}`,
      cmlimit: '500',
      cmtype: 'page',
      format: 'json',
    });
    if (continueToken) params.set('cmcontinue', continueToken);

    const res = await fetchWithRetry(`${apiBase}?${params}`, { headers: { 'User-Agent': UA } });
    const json = await res.json();
    if (json.error) throw new Error(`Wiki API error: ${json.error.info}`);

    for (const member of (json.query.categorymembers || [])) {
      pages.push(member.title);
    }

    if (json.continue && json.continue.cmcontinue) {
      continueToken = json.continue.cmcontinue;
    } else {
      break;
    }
  }

  return pages;
}

function splitTextIntoChunks(text, baseId, limit = 2000) {
  if (text.length <= limit) return [{ id: baseId, text }];
  const chunks = [];
  let remaining = text;
  let index = 1;
  while (remaining.length > 0) {
    let cutPoint = limit;
    if (remaining.length > limit) {
      const paraBreak = remaining.lastIndexOf('\n\n', limit);
      if (paraBreak > limit * 0.3) { cutPoint = paraBreak; }
      else {
        const sentBreak = remaining.lastIndexOf('. ', limit);
        if (sentBreak > limit * 0.3) cutPoint = sentBreak + 1;
      }
    } else {
      cutPoint = remaining.length;
    }
    chunks.push({ id: `${baseId}_${index}`, text: remaining.substring(0, cutPoint).trim() });
    remaining = remaining.substring(cutPoint).trim();
    index++;
  }
  return chunks;
}

function delay(ms, signal) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    if (signal) {
      signal.addEventListener('abort', () => { clearTimeout(timer); reject(new Error('aborted')); }, { once: true });
    }
  });
}

async function runBatchImport({ apiBase, wikiName, categories, projectName, store, broadcastFn, sessionCode, sseWriter, signal }) {
  const startTime = Date.now();
  const stats = { categoriesDone: 0, categoriesTotal: categories.length, pagesDone: 0, pagesTotal: 0, chunksCreated: 0, errors: 0, skipped: 0 };
  const parsedPages = new Set();

  // Phase 1: collect all page titles per category (with rate limiting)
  const categoryPages = new Map();
  sseWriter.write('status', { phase: 'collecting', log: 'Collecting page lists from categories...' });

  for (let i = 0; i < categories.length; i++) {
    const catName = categories[i];
    if (signal.aborted) break;
    try {
      const pages = await fetchCategoryMembers(apiBase, catName);
      const unique = pages.filter(p => !parsedPages.has(p));
      categoryPages.set(catName, unique);
      for (const p of pages) parsedPages.add(p);
      sseWriter.write('status', { phase: 'collecting', log: `Listed "${catName}" (${unique.length} pages) — ${i + 1}/${categories.length}` });
    } catch (e) {
      categoryPages.set(catName, []);
      sseWriter.write('log', { log: `Failed to list pages for "${catName}": ${e.message}`, error: true });
    }
    if (i < categories.length - 1) await delay(500, signal).catch(() => {});
  }

  stats.pagesTotal = [...categoryPages.values()].reduce((sum, p) => sum + p.length, 0);
  parsedPages.clear(); // Reset — we'll track during parsing

  // Create the project
  try {
    store.createProject(projectName, { source: 'wiki-batch' });
  } catch (e) {
    // Name taken — append timestamp
    projectName = `${projectName}_${Date.now()}`;
    store.createProject(projectName, { source: 'wiki-batch' });
  }

  sseWriter.write('started', { project: projectName, ...stats });

  // Phase 2: process each category
  for (const catName of categories) {
    if (signal.aborted) { sseWriter.write('cancelled', { ...stats, project: projectName, elapsed: Date.now() - startTime }); return; }

    const pages = categoryPages.get(catName) || [];
    if (pages.length === 0) { stats.categoriesDone++; continue; }

    // Create category
    try {
      store.createCategory(projectName, catName, { source: 'wiki-batch' });
    } catch (e) {
      sseWriter.write('log', { log: `Category "${catName}" already exists, adding to it`, error: false });
    }

    const allChunks = [];

    for (const pageTitle of pages) {
      if (signal.aborted) { sseWriter.write('cancelled', { ...stats, project: projectName, elapsed: Date.now() - startTime }); return; }
      if (parsedPages.has(pageTitle)) { stats.skipped++; stats.pagesDone++; continue; }

      sseWriter.write('progress', {
        ...stats,
        currentCategory: catName,
        currentPage: pageTitle,
        elapsed: Date.now() - startTime,
      });

      try {
        const result = await fetchViaMediaWikiAPI(apiBase, pageTitle);
        if (!result) { stats.errors++; stats.pagesDone++; sseWriter.write('log', { log: `No content for "${pageTitle}"`, error: true }); continue; }

        const extracted = extractFromHTML(result.html, result.title);
        if (!extracted.text || extracted.text.trim().length < 10) { stats.skipped++; stats.pagesDone++; continue; }

        const baseId = (extracted.pageTitle || pageTitle)
          .toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '').substring(0, 60);
        const textChunks = splitTextIntoChunks(extracted.text, baseId);

        const customFields = Object.entries(extracted.infobox || {}).map(([key, value]) => ({ key, value }));

        for (const chunk of textChunks) {
          allChunks.push({
            id: chunk.id,
            text: chunk.text,
            metadata: {
              page_title: extracted.pageTitle || pageTitle,
              source: `${apiBase.replace('/api.php', '')}/wiki/${encodeURIComponent(pageTitle.replace(/ /g, '_'))}`,
              license: 'CC BY-NC-SA 3.0',
            },
            customFields,
          });
        }

        parsedPages.add(pageTitle);
        stats.pagesDone++;
        stats.chunksCreated += textChunks.length;

        sseWriter.write('progress', {
          ...stats,
          currentCategory: catName,
          currentPage: pageTitle,
          log: `Parsed "${pageTitle}" -> ${textChunks.length} chunk${textChunks.length > 1 ? 's' : ''}`,
          elapsed: Date.now() - startTime,
        });

        await delay(700, signal);
      } catch (e) {
        if (e.message === 'aborted') { sseWriter.write('cancelled', { ...stats, project: projectName, elapsed: Date.now() - startTime }); return; }
        stats.errors++;
        stats.pagesDone++;
        sseWriter.write('log', { log: `Error parsing "${pageTitle}": ${e.message}`, error: true });
      }
    }

    // Bulk add all chunks for this category
    if (allChunks.length > 0) {
      try {
        // bulkAddChunks expects flat metadata, so flatten customFields into metadata for storage
        const flatChunks = allChunks.map(ch => ({
          id: ch.id,
          text: ch.text,
          page_title: ch.metadata.page_title,
          source: ch.metadata.source,
          license: ch.metadata.license,
          metadata: {
            ...ch.metadata,
            ...Object.fromEntries((ch.customFields || []).map(f => [f.key, f.value])),
          },
        }));
        store.bulkAddChunks(projectName, catName, flatChunks, { source: 'wiki-batch' });
      } catch (e) {
        sseWriter.write('log', { log: `Error saving chunks for "${catName}": ${e.message}`, error: true });
      }
    }

    stats.categoriesDone++;
    sseWriter.write('category-done', { ...stats, category: catName, elapsed: Date.now() - startTime });
  }

  // Broadcast data change
  if (broadcastFn && sessionCode) {
    broadcastFn(sessionCode, 'project:created', { name: projectName });
  }

  sseWriter.write('complete', {
    ...stats,
    project: projectName,
    elapsed: Date.now() - startTime,
  });
}

module.exports = { detectWikiFromUrl, fetchAllCategories, filterCategories, fetchCategoryMembers, runBatchImport };

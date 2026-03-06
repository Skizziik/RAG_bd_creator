const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const crypto = require('crypto');
const readline = require('readline');
const { spawn } = require('child_process');
const { Readable } = require('stream');
const { finished } = require('stream/promises');
const { buildChunksFromExtractedPage } = require('./wiki-wikitext');
const { UA, extractFromHTML, fetchViaMediaWikiAPI } = require('./wiki');

const JUNK_PATTERNS = [
  'stub', 'stubs', 'maintenance', 'cleanup', 'missing', 'redirect',
  'template', 'user', 'talk', 'file', 'media', 'help', 'project',
  'portal', 'draft', 'module', 'mediawiki', 'disambiguation',
  'articles needing', 'pages with', 'pages using', 'pages lacking',
  'cs1', 'webarchive', 'short description', 'infobox',
  'coordinates', 'wikiproject', 'wikipedia', 'all pages', 'all articles',
  'images', 'sprites', 'icons', 'screenshots', 'artwork', 'renders',
  'textures', 'thumbnails', 'photos', 'gifs', 'animations',
  'titlecard', 'card image', 'gallery', 'logos', 'logo',
];

const WIKIMEDIA_SUFFIXES = {
  'wikipedia.org': suffix => `${suffix}wiki`,
  'wiktionary.org': suffix => `${suffix}wiktionary`,
  'wikibooks.org': suffix => `${suffix}wikibooks`,
  'wikiquote.org': suffix => `${suffix}wikiquote`,
  'wikivoyage.org': suffix => `${suffix}wikivoyage`,
  'wikinews.org': suffix => `${suffix}wikinews`,
  'wikisource.org': suffix => `${suffix}wikisource`,
};

function safeJsonParse(value, fallback) {
  try { return JSON.parse(value); } catch { return fallback; }
}

function normalizeCategoryName(value) {
  return String(value || '')
    .replace(/^Category:/i, '')
    .replace(/_/g, ' ')
    .trim();
}

function slugify(value, fallback = 'wiki') {
  const slug = String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
  return slug || fallback;
}

function ensureDirSync(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function resolveLicense(hostname) {
  if (/wikipedia\.org|wiktionary\.org|wikibooks\.org|wikiquote\.org|wikivoyage\.org|wikinews\.org|wikisource\.org/i.test(hostname)) {
    return 'CC BY-SA 4.0';
  }
  if (/fandom\.com|wikia\.com/i.test(hostname)) {
    return 'CC BY-SA';
  }
  return 'See source wiki';
}

function isWikimediaHost(hostname) {
  return Object.keys(WIKIMEDIA_SUFFIXES).some(suffix => hostname.endsWith(suffix));
}

function detectAcquisition(url) {
  const u = new URL(url);
  const hostname = u.hostname.toLowerCase();
  const hostNoWww = hostname.replace(/^www\./, '');
  const baseUrl = `${u.protocol}//${u.host}`;
  const wikiName = slugify(hostNoWww.split('.')[0], 'wiki');
  const wikiId = slugify(hostNoWww.replace(/\./g, '-'));
  const info = {
    wikiId,
    wikiName,
    host: hostNoWww,
    baseUrl,
    apiBase: `${baseUrl}/api.php`,
    sourceUrl: url,
    family: 'mediawiki',
    acquisition: {
      kind: 'api-snapshot',
      label: 'MediaWiki API snapshot',
    },
    license: resolveLicense(hostNoWww),
  };

  if (/wiki\.gg$/i.test(hostNoWww)) info.family = 'wiki.gg';
  else if (/fandom\.com$|wikia\.com$/i.test(hostNoWww)) info.family = 'fandom';
  else if (isWikimediaHost(hostNoWww)) info.family = 'wikimedia';

  if (isWikimediaHost(hostNoWww)) {
    const matchedSuffix = Object.keys(WIKIMEDIA_SUFFIXES).find(suffix => hostNoWww.endsWith(suffix));
    const prefix = hostNoWww.replace(`.${matchedSuffix}`, '');
    const dbName = WIKIMEDIA_SUFFIXES[matchedSuffix](prefix);
    info.acquisition = {
      kind: 'wikimedia-dump',
      label: 'Wikimedia XML dump',
      dumpUrl: `https://dumps.wikimedia.org/${dbName}/latest/${dbName}-latest-pages-articles.xml.bz2`,
      dbName,
    };
  }

  return info;
}

function getCachePaths(cacheRoot, wikiId) {
  const root = path.join(cacheRoot, wikiId);
  return {
    root,
    manifest: path.join(root, 'manifest.json'),
    pages: path.join(root, 'pages.jsonl'),
    categories: path.join(root, 'categories.json'),
    dumpDir: path.join(root, 'dump'),
    dumpFile: path.join(root, 'dump', 'pages-articles.xml.bz2'),
    renderedDir: path.join(root, 'rendered-metadata'),
  };
}

async function loadManifest(cacheRoot, wikiId) {
  const paths = getCachePaths(cacheRoot, wikiId);
  try {
    return JSON.parse(await fsp.readFile(paths.manifest, 'utf8'));
  } catch {
    return null;
  }
}

async function saveManifest(cacheRoot, manifest) {
  const paths = getCachePaths(cacheRoot, manifest.wikiId);
  await fsp.mkdir(paths.root, { recursive: true });
  await fsp.writeFile(paths.manifest, JSON.stringify(manifest, null, 2), 'utf8');
}

async function prepareLocalWiki(cacheRoot, url) {
  const detected = detectAcquisition(url);
  const existing = await loadManifest(cacheRoot, detected.wikiId);
  if (existing) {
    return {
      wiki: existing,
      ready: existing.status === 'ready' && fs.existsSync(getCachePaths(cacheRoot, existing.wikiId).pages),
      cached: true,
    };
  }

  const manifest = {
    version: 1,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    status: 'new',
    pageCount: 0,
    contentPages: 0,
    categoriesCount: 0,
    lastError: null,
    ...detected,
  };
  await saveManifest(cacheRoot, manifest);
  return { wiki: manifest, ready: false, cached: false };
}

async function fetchJsonWithRetry(url, options = {}, retries = 6) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const response = await fetch(url, options);
    if (response.ok) return response.json();
    if ((response.status === 429 || response.status >= 500) && attempt < retries) {
      await new Promise(resolve => setTimeout(resolve, 1000 * (attempt + 1)));
      continue;
    }
    throw new Error(`HTTP ${response.status} for ${url}`);
  }
}

async function downloadToFile(url, destination, sseWriter, signal) {
  const response = await fetch(url, { headers: { 'User-Agent': UA }, signal });
  if (!response.ok) throw new Error(`Failed to download dump: HTTP ${response.status}`);
  if (!response.body) throw new Error('Download stream is not available');

  const total = Number(response.headers.get('content-length')) || 0;
  let downloaded = 0;
  const writer = fs.createWriteStream(destination);
  const stream = Readable.fromWeb(response.body);

  stream.on('data', chunk => {
    downloaded += chunk.length;
    if (sseWriter && total) {
      sseWriter.write('status', {
        phase: 'download',
        log: `Downloading dump... ${Math.round((downloaded / total) * 100)}%`,
        downloaded,
        total,
      });
    }
  });

  if (signal) {
    signal.addEventListener('abort', () => {
      stream.destroy(new Error('aborted'));
      writer.destroy(new Error('aborted'));
    }, { once: true });
  }

  stream.pipe(writer);
  await finished(writer);
}

function getPythonCommand() {
  return process.env.PYTHON_BIN || (process.platform === 'win32' ? 'python' : 'python3');
}

async function runDumpIndexer(paths, manifest, sseWriter, signal) {
  const python = getPythonCommand();
  const args = [
    path.join(__dirname, '..', 'scripts', 'index_mediawiki_dump.py'),
    '--input', paths.dumpFile,
    '--output-dir', paths.root,
    '--base-url', manifest.baseUrl,
  ];

  await new Promise((resolve, reject) => {
    const child = spawn(python, args, { cwd: path.join(__dirname, '..') });
    let stderr = '';

    child.stdout.on('data', chunk => {
      const lines = chunk.toString().split(/\r?\n/).filter(Boolean);
      for (const line of lines) {
        if (!line.startsWith('EVENT ')) continue;
        const payload = safeJsonParse(line.slice(6), null);
        if (!payload) continue;
        if (payload.event === 'progress') {
          sseWriter.write('progress', {
            phase: 'index',
            pagesDone: payload.pages || 0,
            pagesTotal: 0,
            categoriesDone: 0,
            categoriesTotal: 0,
            chunksCreated: 0,
            errors: 0,
            skipped: 0,
            log: `Indexed ${payload.content_pages || payload.pages || 0} pages from dump`,
          });
        } else if (payload.event === 'complete') {
          sseWriter.write('status', {
            phase: 'index',
            log: `Dump indexed: ${payload.content_pages || payload.pages || 0} content pages`,
          });
        }
      }
    });

    child.stderr.on('data', chunk => { stderr += chunk.toString(); });
    child.on('error', reject);
    child.on('close', code => {
      if (code === 0) resolve();
      else reject(new Error(stderr.trim() || `Dump indexer exited with code ${code}`));
    });

    if (signal) {
      signal.addEventListener('abort', () => {
        child.kill('SIGTERM');
        reject(new Error('aborted'));
      }, { once: true });
    }
  });
}

async function fetchAllPageTitles(apiBase, signal, sseWriter) {
  const titles = [];
  let apcontinue = null;

  while (true) {
    if (signal?.aborted) throw new Error('aborted');
    const params = new URLSearchParams({
      action: 'query',
      list: 'allpages',
      aplimit: '500',
      apnamespace: '0',
      format: 'json',
    });
    if (apcontinue) params.set('apcontinue', apcontinue);
    const json = await fetchJsonWithRetry(`${apiBase}?${params}`, { headers: { 'User-Agent': UA }, signal });
    const pages = json.query?.allpages || [];
    for (const page of pages) titles.push(page.title);
    if (sseWriter) sseWriter.write('status', { phase: 'discover', log: `Discovered ${titles.length} page titles...` });
    if (!json.continue?.apcontinue) break;
    apcontinue = json.continue.apcontinue;
  }

  return titles;
}

async function snapshotViaApi(cacheRoot, manifest, sseWriter, signal) {
  const paths = getCachePaths(cacheRoot, manifest.wikiId);
  await fsp.mkdir(paths.root, { recursive: true });

  const titles = await fetchAllPageTitles(manifest.apiBase, signal, sseWriter);
  const categoryCounts = {};
  const writer = fs.createWriteStream(paths.pages, { flags: 'w' });
  let pagesDone = 0;

  try {
    for (let index = 0; index < titles.length; index += 25) {
      if (signal?.aborted) throw new Error('aborted');
      const batch = titles.slice(index, index + 25);
      const params = new URLSearchParams({
        action: 'query',
        prop: 'revisions|categories',
        rvprop: 'content|ids|timestamp',
        rvslots: 'main',
        cllimit: 'max',
        titles: batch.join('|'),
        format: 'json',
      });
      const json = await fetchJsonWithRetry(`${manifest.apiBase}?${params}`, { headers: { 'User-Agent': UA }, signal });
      const pages = Object.values(json.query?.pages || {});
      for (const page of pages) {
        const revision = Array.isArray(page.revisions) ? page.revisions[0] : null;
        const slot = revision?.slots?.main;
        const wikitext = slot?.['*'] || revision?.['*'] || '';
        const categories = (page.categories || []).map(item => normalizeCategoryName(item.title));
        for (const category of categories) {
          categoryCounts[category] = (categoryCounts[category] || 0) + 1;
        }
        writer.write(`${JSON.stringify({
          title: page.title,
          ns: page.ns,
          redirect: !!page.redirect,
          wikitext,
          categories,
          source_url: `${manifest.baseUrl}/wiki/${encodeURIComponent(String(page.title || '').replace(/ /g, '_'))}`,
        })}\n`);
        pagesDone++;
      }
      sseWriter.write('progress', {
        phase: 'snapshot',
        pagesDone,
        pagesTotal: titles.length,
        categoriesDone: 0,
        categoriesTotal: 0,
        chunksCreated: 0,
        errors: 0,
        skipped: 0,
        log: `Cached ${pagesDone}/${titles.length} pages locally`,
      });
    }
  } finally {
    writer.end();
  }

  const categoryData = {
    pages: titles.length,
    content_pages: pagesDone,
    redirect_pages: 0,
    categories: Object.entries(categoryCounts)
      .map(([name, count]) => ({ name, count }))
      .sort((a, b) => b.count - a.count),
  };
  await fsp.writeFile(paths.categories, JSON.stringify(categoryData, null, 2), 'utf8');
  return { pageCount: titles.length, contentPages: pagesDone, categoriesCount: categoryData.categories.length };
}

async function ingestLocalWiki(cacheRoot, url, sseWriter, signal) {
  const prepared = await prepareLocalWiki(cacheRoot, url);
  const manifest = prepared.wiki;
  const paths = getCachePaths(cacheRoot, manifest.wikiId);

  if (prepared.ready) {
    sseWriter.write('started', { project: manifest.wikiId, log: 'Local cache is already ready' });
    return manifest;
  }

  manifest.status = 'acquiring';
  manifest.updatedAt = new Date().toISOString();
  manifest.lastError = null;
  await saveManifest(cacheRoot, manifest);

  try {
    ensureDirSync(paths.dumpDir);
    if (manifest.acquisition.kind === 'wikimedia-dump') {
      if (!fs.existsSync(paths.dumpFile)) {
        sseWriter.write('status', { phase: 'download', log: `Downloading dump for ${manifest.wikiName}...` });
        await downloadToFile(manifest.acquisition.dumpUrl, paths.dumpFile, sseWriter, signal);
      } else {
        sseWriter.write('status', { phase: 'download', log: 'Reusing cached dump file' });
      }
      await runDumpIndexer(paths, manifest, sseWriter, signal);
      const categoryData = JSON.parse(await fsp.readFile(paths.categories, 'utf8'));
      manifest.pageCount = categoryData.pages || 0;
      manifest.contentPages = categoryData.content_pages || 0;
      manifest.categoriesCount = (categoryData.categories || []).length;
    } else {
      sseWriter.write('status', { phase: 'snapshot', log: 'Building local wiki snapshot via MediaWiki API...' });
      const result = await snapshotViaApi(cacheRoot, manifest, sseWriter, signal);
      manifest.pageCount = result.pageCount;
      manifest.contentPages = result.contentPages;
      manifest.categoriesCount = result.categoriesCount;
    }

    manifest.status = 'ready';
    manifest.updatedAt = new Date().toISOString();
    await saveManifest(cacheRoot, manifest);
    return manifest;
  } catch (error) {
    manifest.status = 'error';
    manifest.lastError = error.message;
    manifest.updatedAt = new Date().toISOString();
    await saveManifest(cacheRoot, manifest);
    throw error;
  }
}

async function readCategoryData(cacheRoot, wikiId) {
  const paths = getCachePaths(cacheRoot, wikiId);
  const json = JSON.parse(await fsp.readFile(paths.categories, 'utf8'));
  return json.categories || [];
}

function splitAcceptedCategories(categories) {
  const accepted = [];
  const rejected = [];
  for (const category of categories) {
    const name = normalizeCategoryName(category.name);
    const lower = name.toLowerCase();
    const isJunk = !category.count || JUNK_PATTERNS.some(pattern => lower.includes(pattern));
    const entry = { name, count: category.count || 0, accepted: !isJunk };
    if (isJunk) rejected.push(entry);
    else accepted.push(entry);
  }
  return { accepted, rejected };
}

async function getLocalCategories(cacheRoot, wikiId) {
  const categories = await readCategoryData(cacheRoot, wikiId);
  const { accepted, rejected } = splitAcceptedCategories(categories);
  return {
    categories: [...accepted, ...rejected],
    totalRaw: categories.length,
    totalAccepted: accepted.length,
  };
}

function chooseCategoryForPage(pageCategories, selectedLookup, fallbackCategory) {
  for (const category of pageCategories) {
    const normalized = normalizeCategoryName(category);
    if (selectedLookup.has(normalized.toLowerCase())) return normalized;
  }
  return fallbackCategory;
}

function shouldEnrichRenderedMetadata(manifest, extracted) {
  if (!manifest) return false;
  if (!['wiki.gg', 'fandom'].includes(manifest.family)) return false;
  const infoboxSize = Object.keys(extracted?.infobox || {}).length;
  return infoboxSize < 3;
}

function getRenderedMetadataFile(paths, title) {
  const safeName = Buffer.from(String(title || ''), 'utf8').toString('base64url') || 'page';
  return path.join(paths.renderedDir, `${safeName}.json`);
}

async function loadRenderedMetadata(paths, title) {
  try {
    const file = getRenderedMetadataFile(paths, title);
    return JSON.parse(await fsp.readFile(file, 'utf8'));
  } catch {
    return null;
  }
}

async function saveRenderedMetadata(paths, title, data) {
  await fsp.mkdir(paths.renderedDir, { recursive: true });
  const file = getRenderedMetadataFile(paths, title);
  await fsp.writeFile(file, JSON.stringify(data, null, 2), 'utf8');
}

async function fetchRenderedMetadata(manifest, pageTitle) {
  let lastError = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const result = await fetchViaMediaWikiAPI(manifest.apiBase, pageTitle);
      if (result?.html) {
        const extracted = extractFromHTML(result.html, result.title || pageTitle);
        if (Object.keys(extracted.infobox || {}).length || extracted.text) {
          return {
            pageTitle: extracted.pageTitle || pageTitle,
            infobox: extracted.infobox || {},
            renderedTextLength: (extracted.text || '').length,
            fetchedAt: new Date().toISOString(),
          };
        }
      }
    } catch (error) {
      lastError = error;
    }
    await new Promise(resolve => setTimeout(resolve, 350 * (attempt + 1)));
  }
  if (lastError) throw lastError;
  return null;
}

async function maybeEnrichFromRenderedMetadata(paths, manifest, page, extracted) {
  if (!shouldEnrichRenderedMetadata(manifest, extracted)) return extracted;

  const cached = await loadRenderedMetadata(paths, page.title);
  const enrichment = cached || await fetchRenderedMetadata(manifest, page.title);
  if (!cached && enrichment) await saveRenderedMetadata(paths, page.title, enrichment);
  if (!enrichment) return extracted;

  return {
    ...extracted,
    pageTitle: enrichment.pageTitle || extracted.pageTitle,
    infobox: { ...(enrichment.infobox || {}), ...(extracted.infobox || {}) },
    qualityScore: Math.min(0.99, Number(((extracted.qualityScore || 0.2) + (Object.keys(enrichment.infobox || {}).length ? 0.12 : 0)).toFixed(2))),
    renderedMetadataEnriched: true,
  };
}

async function collectExistingPageTitles(store, projectName) {
  const existing = new Set();
  try {
    const project = store.getProject(projectName);
    for (const category of project.categories || []) {
      for (const chunk of category.chunks || []) {
        if (chunk.metadata?.page_title) existing.add(chunk.metadata.page_title);
      }
    }
  } catch {}
  return existing;
}

async function runLocalDatasetBuild({ cacheRoot, wikiId, categories, projectName, store, broadcastFn, sessionCode, sseWriter, signal, resume }) {
  const manifest = await loadManifest(cacheRoot, wikiId);
  if (!manifest || manifest.status !== 'ready') throw new Error('Local wiki cache is not ready');

  const paths = getCachePaths(cacheRoot, wikiId);
  const selected = (categories || []).map(normalizeCategoryName).filter(Boolean);
  const selectedLookup = new Set(selected.map(item => item.toLowerCase()));
  const fallbackCategory = selected[0] || 'Imported';
  const stats = {
    categoriesDone: 0,
    categoriesTotal: selected.length || 1,
    pagesDone: 0,
    pagesTotal: manifest.contentPages || manifest.pageCount || 0,
    chunksCreated: 0,
    errors: 0,
    skipped: 0,
  };
  const completedCategories = new Set();
  const flushEveryPages = 50;
  let dirtyPages = 0;
  let dirtyChunks = 0;
  let dirtyProject = false;

  try {
    store.createProject(projectName, { source: 'local-wiki' });
  } catch (error) {
    if (!resume) {
      projectName = `${projectName}_${Date.now()}`;
      store.createProject(projectName, { source: 'local-wiki' });
    }
  }

  const projectData = store.getProject(projectName);
  const existingPageTitles = new Set();
  for (const category of projectData.categories || []) {
    for (const chunk of category.chunks || []) {
      if (chunk.metadata?.page_title) existingPageTitles.add(chunk.metadata.page_title);
    }
  }

  const ensureCategory = categoryName => {
    const trimmed = String(categoryName || '').trim();
    if (!trimmed) return null;
    let category = projectData.categories.find(c => c.name.toLowerCase() === trimmed.toLowerCase());
    if (!category) {
      category = { id: crypto.randomUUID(), name: trimmed, expanded: true, chunks: [] };
      projectData.categories.push(category);
      dirtyProject = true;
    }
    return category;
  };

  const addChunksToProject = (categoryName, chunks) => {
    const category = ensureCategory(categoryName);
    if (!category) return 0;
    let added = 0;
    for (const chunk of chunks) {
      const id = String(chunk.id || '').trim();
      if (!id) continue;
      if (store._isIdTaken(projectData, id)) continue;
      category.chunks.push({
        _uid: crypto.randomUUID(),
        id,
        text: chunk.text || '',
        metadata: {
          page_title: chunk.page_title || chunk.metadata?.page_title || '',
          source: chunk.source || chunk.metadata?.source || '',
          license: chunk.license || chunk.metadata?.license || 'See source wiki',
        },
        customFields: store._parseCustomFields(chunk.metadata),
      });
      added++;
    }
    if (added > 0) {
      dirtyProject = true;
      dirtyPages++;
      dirtyChunks += added;
    }
    return added;
  };

  const flushProject = force => {
    if (!dirtyProject) return;
    if (!force && dirtyPages < flushEveryPages) return;
    store._save(projectName, projectData);
    dirtyProject = false;
    dirtyPages = 0;
    dirtyChunks = 0;
    if (broadcastFn && sessionCode) broadcastFn(sessionCode, 'data:changed', { project: projectName });
  };

  for (const category of selected.length ? selected : [fallbackCategory]) ensureCategory(category);
  store._save(projectName, projectData);
  if (broadcastFn && sessionCode) broadcastFn(sessionCode, 'project:created', { name: projectName });
  sseWriter.write('started', { project: projectName, ...stats });

  const rl = readline.createInterface({ input: fs.createReadStream(paths.pages, 'utf8'), crlfDelay: Infinity });

  for await (const line of rl) {
    if (signal?.aborted) throw new Error('aborted');
    if (!line.trim()) continue;

    const page = safeJsonParse(line, null);
    if (!page || Number(page.ns) !== 0 || page.redirect) {
      stats.skipped++;
      continue;
    }

    const normalizedPageCategories = (page.categories || []).map(normalizeCategoryName);
    if (selectedLookup.size && !normalizedPageCategories.some(category => selectedLookup.has(category.toLowerCase()))) {
      stats.skipped++;
      stats.pagesDone++;
      continue;
    }
    if (existingPageTitles.has(page.title)) {
      stats.skipped++;
      stats.pagesDone++;
      continue;
    }

    try {
      let extracted = buildChunksFromExtractedPage(page);
      extracted = await maybeEnrichFromRenderedMetadata(paths, manifest, page, extracted);
      const renderedMetadataEnriched = !!extracted.renderedMetadataEnriched;
      if (!extracted.text || extracted.text.length < 40 || !extracted.chunks.length || extracted.qualityScore < 0.18) {
        stats.skipped++;
        stats.pagesDone++;
        continue;
      }

      const categoryName = chooseCategoryForPage(normalizedPageCategories, selectedLookup, fallbackCategory);
      completedCategories.add(categoryName.toLowerCase());
      stats.categoriesDone = Math.max(stats.categoriesDone, completedCategories.size);

      const baseMetadata = {
        page_title: extracted.pageTitle || page.title,
        source: page.source_url || `${manifest.baseUrl}/wiki/${encodeURIComponent(String(page.title || '').replace(/ /g, '_'))}`,
        license: manifest.license,
      };
      for (const [key, value] of Object.entries(extracted.infobox || {})) baseMetadata[key] = value;

      const flatChunks = extracted.chunks.map(chunk => ({
        id: chunk.id,
        text: chunk.text,
        metadata: {
          ...baseMetadata,
        },
      }));

      const added = addChunksToProject(categoryName, flatChunks);
      existingPageTitles.add(page.title);
      stats.pagesDone++;
      stats.chunksCreated += added;

      flushProject(false);

      sseWriter.write('progress', {
        ...stats,
        currentCategory: categoryName,
        currentPage: page.title,
        log: `Built ${added} chunk${added !== 1 ? 's' : ''} from "${page.title}"`,
      });
    } catch (error) {
      stats.errors++;
      stats.pagesDone++;
      sseWriter.write('log', { log: `Failed to build "${page?.title || 'page'}": ${error.message}`, error: true });
    }
  }

  projectData.localWikiMeta = {
    wikiId,
    wikiName: manifest.wikiName,
    sourceUrl: manifest.sourceUrl,
    family: manifest.family,
    acquisition: manifest.acquisition.kind,
    builtAt: new Date().toISOString(),
    selectedCategories: selected,
  };
  dirtyProject = true;
  flushProject(true);

  if (stats.chunksCreated > 0) {
    store.recordHistory(projectName, 'localWikiBuild', 'Built ' + stats.chunksCreated + ' chunks from local wiki cache', 'local-wiki');
  }

  if (broadcastFn && sessionCode) broadcastFn(sessionCode, 'data:changed', { project: projectName });
  sseWriter.write('complete', { ...stats, project: projectName });
  return { projectName, stats };
}

module.exports = {
  detectAcquisition,
  getLocalCategories,
  ingestLocalWiki,
  loadManifest,
  prepareLocalWiki,
  runLocalDatasetBuild,
};









const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const DEFAULT_LICENSE = 'CC BY-NC-SA 3.0';
const STANDARD_META = ['page_title', 'source', 'license'];
const MAX_HISTORY = 50;

class Store {
  constructor(dataDir) {
    this.dataDir = dataDir || path.join(__dirname, '..', 'data');
    this._ensureDir();
  }

  _ensureDir() {
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }
  }

  _filePath(name) {
    return path.join(this.dataDir, `${name}.json`);
  }

  // ---- PROJECT ----

  listProjects() {
    this._ensureDir();
    const files = fs.readdirSync(this.dataDir).filter(f => f.endsWith('.json') && !f.endsWith('.history.json'));
    return files.map(f => {
      const name = f.replace(/\.json$/, '');
      try {
        const data = this._load(name);
        const totalChunks = data.categories.reduce((sum, c) => sum + c.chunks.length, 0);
        return { name, categories: data.categories.length, chunks: totalChunks, createdAt: data.createdAt };
      } catch {
        return { name, categories: 0, chunks: 0, createdAt: null };
      }
    });
  }

  getProject(name) {
    return this._load(name);
  }

  createProject(name, opts) {
    const safeName = name.replace(/[^a-zA-Z0-9_\-. ]/g, '').trim();
    if (!safeName) throw new Error('Invalid project name');
    if (fs.existsSync(this._filePath(safeName))) throw new Error(`Project "${safeName}" already exists`);
    const project = { name: safeName, createdAt: new Date().toISOString(), categories: [] };
    this._save(safeName, project);
    this._commit(safeName, 'createProject', `Created project '${safeName}'`, opts?.source);
    return project;
  }

  deleteProject(name) {
    const fp = this._filePath(name);
    if (!fs.existsSync(fp)) throw new Error(`Project "${name}" not found`);
    fs.unlinkSync(fp);
    const hfp = this._historyFilePath(name);
    if (fs.existsSync(hfp)) fs.unlinkSync(hfp);
    return { deleted: name };
  }

  getStats(name) {
    const data = this._load(name);
    let totalChunks = 0, totalLength = 0, longest = 0, shortest = Infinity;
    for (const cat of data.categories) {
      for (const ch of cat.chunks) {
        totalChunks++;
        const len = (ch.text || '').length;
        totalLength += len;
        if (len > longest) longest = len;
        if (len < shortest) shortest = len;
      }
    }
    return {
      project: name,
      categories: data.categories.length,
      categoryNames: data.categories.map(c => `${c.name} (${c.chunks.length} chunks)`),
      totalChunks,
      avgTextLength: totalChunks ? Math.round(totalLength / totalChunks) : 0,
      longestChunk: totalChunks ? longest : 0,
      shortestChunk: totalChunks ? shortest : 0,
      createdAt: data.createdAt,
    };
  }

  // ---- CATEGORY ----

  listCategories(projectName) {
    const data = this._load(projectName);
    return data.categories.map(c => ({ name: c.name, chunks: c.chunks.length }));
  }

  createCategory(projectName, categoryName, opts) {
    const data = this._load(projectName);
    const trimmed = categoryName.trim();
    if (!trimmed) throw new Error('Category name cannot be empty');
    if (data.categories.some(c => c.name.toLowerCase() === trimmed.toLowerCase())) {
      throw new Error(`Category "${trimmed}" already exists`);
    }
    const cat = { id: crypto.randomUUID(), name: trimmed, expanded: true, chunks: [] };
    data.categories.push(cat);
    this._save(projectName, data);
    this._commit(projectName, 'createCategory', `Created category '${trimmed}'`, opts?.source);
    return cat;
  }

  renameCategory(projectName, oldName, newName, opts) {
    const data = this._load(projectName);
    const cat = this._findCategory(data, oldName);
    const trimmed = newName.trim();
    if (!trimmed) throw new Error('New name cannot be empty');
    if (data.categories.some(c => c.name.toLowerCase() === trimmed.toLowerCase() && c.id !== cat.id)) {
      throw new Error(`Category "${trimmed}" already exists`);
    }
    cat.name = trimmed;
    this._save(projectName, data);
    this._commit(projectName, 'renameCategory', `Renamed category '${oldName}' → '${trimmed}'`, opts?.source);
    return { old: oldName, new: trimmed };
  }

  deleteCategory(projectName, categoryName, opts) {
    const data = this._load(projectName);
    const idx = data.categories.findIndex(c => c.name.toLowerCase() === categoryName.toLowerCase());
    if (idx === -1) throw new Error(`Category "${categoryName}" not found`);
    const removed = data.categories.splice(idx, 1)[0];
    this._save(projectName, data);
    this._commit(projectName, 'deleteCategory', `Deleted category '${removed.name}' (${removed.chunks.length} chunks)`, opts?.source);
    return { deleted: removed.name, chunksRemoved: removed.chunks.length };
  }

  toggleCategory(projectName, catId) {
    const data = this._load(projectName);
    const cat = data.categories.find(c => c.id === catId);
    if (!cat) throw new Error('Category not found');
    cat.expanded = !cat.expanded;
    this._save(projectName, data);
    return { id: catId, expanded: cat.expanded };
  }

  // ---- CHUNK ----

  addChunk(projectName, categoryName, chunk, opts) {
    const data = this._load(projectName);
    const cat = this._findCategory(data, categoryName);
    const id = (chunk.id || '').trim();
    if (!id) throw new Error('Chunk ID is required');
    if (this._isIdTaken(data, id)) throw new Error(`Chunk ID "${id}" already exists. Try adding _1, _2 suffix.`);

    const newChunk = {
      _uid: crypto.randomUUID(),
      id,
      text: chunk.text || '',
      metadata: {
        page_title: chunk.page_title || chunk.metadata?.page_title || '',
        source: chunk.source || chunk.metadata?.source || '',
        license: chunk.license || chunk.metadata?.license || DEFAULT_LICENSE,
      },
      customFields: this._parseCustomFields(chunk.metadata),
    };
    cat.chunks.push(newChunk);
    this._save(projectName, data);
    this._commit(projectName, 'addChunk', `Added chunk '${id}' to '${cat.name}'`, opts?.source);
    return { id: newChunk.id, _uid: newChunk._uid, category: cat.name };
  }

  addBlankChunk(projectName, catId, opts) {
    const data = this._load(projectName);
    const cat = data.categories.find(c => c.id === catId);
    if (!cat) throw new Error('Category not found');
    const newChunk = {
      _uid: crypto.randomUUID(),
      id: '',
      text: '',
      metadata: { page_title: '', source: '', license: DEFAULT_LICENSE },
      customFields: [],
    };
    cat.chunks.push(newChunk);
    this._save(projectName, data);
    this._commit(projectName, 'addBlankChunk', `Added blank chunk to '${cat.name}'`, opts?.source);
    return { _uid: newChunk._uid, catId };
  }

  bulkAddChunks(projectName, categoryName, chunks, opts) {
    const data = this._load(projectName);
    const cat = this._findCategory(data, categoryName);
    const added = [];
    const errors = [];

    for (const chunk of chunks) {
      const id = (chunk.id || '').trim();
      if (!id) { errors.push({ id: '(empty)', reason: 'ID is required' }); continue; }
      if (this._isIdTaken(data, id)) { errors.push({ id, reason: 'Duplicate ID' }); continue; }
      cat.chunks.push({
        _uid: crypto.randomUUID(),
        id,
        text: chunk.text || '',
        metadata: {
          page_title: chunk.page_title || chunk.metadata?.page_title || '',
          source: chunk.source || chunk.metadata?.source || '',
          license: chunk.license || chunk.metadata?.license || DEFAULT_LICENSE,
        },
        customFields: this._parseCustomFields(chunk.metadata),
      });
      added.push(id);
    }

    this._save(projectName, data);
    if (added.length) this._commit(projectName, 'bulkAddChunks', `Added ${added.length} chunks to '${cat.name}'`, opts?.source);
    return { added: added.length, errors: errors.length, details: errors.length ? errors : undefined, ids: added };
  }

  getChunk(projectName, chunkId) {
    const data = this._load(projectName);
    for (const cat of data.categories) {
      const ch = cat.chunks.find(c => c.id === chunkId);
      if (ch) return { ...this._formatChunk(ch), category: cat.name };
    }
    throw new Error(`Chunk "${chunkId}" not found`);
  }

  updateChunk(projectName, catId, chunkUid, updates, opts) {
    const data = this._load(projectName);
    const cat = data.categories.find(c => c.id === catId);
    if (!cat) throw new Error('Category not found');
    const ch = cat.chunks.find(c => c._uid === chunkUid);
    if (!ch) throw new Error('Chunk not found');

    if (updates.id !== undefined) {
      if (updates.id !== ch.id && this._isIdTaken(data, updates.id, ch._uid)) {
        throw new Error(`Chunk ID "${updates.id}" already exists`);
      }
      ch.id = updates.id;
    }
    if (updates.text !== undefined) ch.text = updates.text;
    if (updates.metadata) {
      if (updates.metadata.page_title !== undefined) ch.metadata.page_title = updates.metadata.page_title;
      if (updates.metadata.source !== undefined) ch.metadata.source = updates.metadata.source;
      if (updates.metadata.license !== undefined) ch.metadata.license = updates.metadata.license;
    }
    if (updates.customFields !== undefined) ch.customFields = updates.customFields;

    this._save(projectName, data);
    this._commit(projectName, 'updateChunk', `Updated chunk '${ch.id}'`, opts?.source);
    return { updated: ch.id, _uid: ch._uid };
  }

  deleteChunk(projectName, catId, chunkUid, opts) {
    const data = this._load(projectName);
    const cat = data.categories.find(c => c.id === catId);
    if (!cat) throw new Error('Category not found');
    const idx = cat.chunks.findIndex(c => c._uid === chunkUid);
    if (idx === -1) throw new Error('Chunk not found');
    const removed = cat.chunks.splice(idx, 1)[0];
    this._save(projectName, data);
    this._commit(projectName, 'deleteChunk', `Deleted chunk '${removed.id || chunkUid}'`, opts?.source);
    return { deleted: chunkUid };
  }

  duplicateChunk(projectName, catId, chunkUid, opts) {
    const data = this._load(projectName);
    const cat = data.categories.find(c => c.id === catId);
    if (!cat) throw new Error('Category not found');
    const ch = cat.chunks.find(c => c._uid === chunkUid);
    if (!ch) throw new Error('Chunk not found');

    let newId = ch.id ? ch.id + '_copy' : 'copy';
    let n = 1;
    while (this._isIdTaken(data, newId)) { newId = `${ch.id || 'copy'}_${n++}`; }

    const clone = { ...JSON.parse(JSON.stringify(ch)), _uid: crypto.randomUUID(), id: newId };
    cat.chunks.push(clone);
    this._save(projectName, data);
    this._commit(projectName, 'duplicateChunk', `Duplicated '${ch.id}' as '${newId}'`, opts?.source);
    return { original: ch.id, duplicate: newId, _uid: clone._uid };
  }

  moveChunk(projectName, chunkId, targetCategory, opts) {
    const data = this._load(projectName);
    const targetCat = this._findCategory(data, targetCategory);
    for (const cat of data.categories) {
      const idx = cat.chunks.findIndex(c => c.id === chunkId);
      if (idx === -1) continue;
      if (cat.id === targetCat.id) throw new Error('Chunk is already in that category');
      const [chunk] = cat.chunks.splice(idx, 1);
      targetCat.chunks.push(chunk);
      this._save(projectName, data);
      this._commit(projectName, 'moveChunk', `Moved '${chunkId}' from '${cat.name}' to '${targetCat.name}'`, opts?.source);
      return { moved: chunkId, from: cat.name, to: targetCat.name };
    }
    throw new Error(`Chunk "${chunkId}" not found`);
  }

  // ---- SEARCH ----

  searchChunks(projectName, query) {
    const data = this._load(projectName);
    const q = query.toLowerCase();
    const results = [];
    for (const cat of data.categories) {
      for (const ch of cat.chunks) {
        if (ch.id.toLowerCase().includes(q) || (ch.text || '').toLowerCase().includes(q)) {
          results.push({ id: ch.id, category: cat.name, preview: ch.text.substring(0, 120) + (ch.text.length > 120 ? '...' : '') });
        }
      }
    }
    return { query, found: results.length, results };
  }

  isChunkIdTaken(projectName, id, excludeUid) {
    const data = this._load(projectName);
    return this._isIdTaken(data, id, excludeUid);
  }

  // ---- EXPORT / IMPORT ----

  exportProject(projectName) {
    const data = this._load(projectName);
    const flat = [];
    for (const cat of data.categories) {
      for (const ch of cat.chunks) {
        const entry = { id: ch.id, text: ch.text, metadata: { ...ch.metadata } };
        if (ch.customFields) {
          for (const cf of ch.customFields) {
            if (cf.key && cf.key.trim()) entry.metadata[cf.key.trim()] = String(cf.value ?? '');
          }
        }
        flat.push(entry);
      }
    }
    return flat;
  }

  importJSON(projectName, jsonArray, categoryName, opts) {
    if (!Array.isArray(jsonArray)) throw new Error('Import data must be a JSON array');
    let data;
    try { data = this._load(projectName); } catch { data = this.createProject(projectName, opts); }

    const catName = categoryName || 'Imported';
    let cat = data.categories.find(c => c.name.toLowerCase() === catName.toLowerCase());
    if (!cat) {
      cat = { id: crypto.randomUUID(), name: catName, expanded: true, chunks: [] };
      data.categories.push(cat);
    }

    let imported = 0, skipped = 0;
    for (const entry of jsonArray) {
      const id = (entry.id || '').trim();
      if (!id) { skipped++; continue; }
      if (this._isIdTaken(data, id)) { skipped++; continue; }
      const meta = entry.metadata || {};
      cat.chunks.push({
        _uid: crypto.randomUUID(),
        id,
        text: entry.text || '',
        metadata: { page_title: meta.page_title || '', source: meta.source || '', license: meta.license || DEFAULT_LICENSE },
        customFields: Object.entries(meta).filter(([k]) => !STANDARD_META.includes(k)).map(([key, value]) => ({ key, value: String(value ?? '') })),
      });
      imported++;
    }

    this._save(projectName, data);
    if (imported) this._commit(projectName, 'importJSON', `Imported ${imported} chunks into '${catName}'`, opts?.source);
    return { project: projectName, category: catName, imported, skipped };
  }

  // ---- BULK UPDATE METADATA ----

  bulkUpdateMetadata(projectName, field, value, categoryName, opts) {
    const data = this._load(projectName);
    let updated = 0;
    const cats = categoryName
      ? [this._findCategory(data, categoryName)]
      : data.categories;
    for (const cat of cats) {
      for (const ch of cat.chunks) {
        if (STANDARD_META.includes(field)) {
          ch.metadata[field] = value;
        } else {
          if (!ch.customFields) ch.customFields = [];
          const existing = ch.customFields.find(cf => cf.key === field);
          if (existing) { existing.value = value; }
          else { ch.customFields.push({ key: field, value }); }
        }
        updated++;
      }
    }
    this._save(projectName, data);
    if (updated) this._commit(projectName, 'bulkUpdateMetadata', `Bulk updated '${field}' (${updated} chunks)`, opts?.source);
    return { project: projectName, field, value, updated };
  }

  // ---- MERGE PROJECTS ----

  mergeProjects(sourceName, targetName, opts) {
    const source = this._load(sourceName);
    const target = this._load(targetName);
    let categoriesMerged = 0, chunksAdded = 0, chunksSkipped = 0;

    for (const srcCat of source.categories) {
      let tgtCat = target.categories.find(c => c.name.toLowerCase() === srcCat.name.toLowerCase());
      if (!tgtCat) {
        tgtCat = { id: crypto.randomUUID(), name: srcCat.name, expanded: true, chunks: [] };
        target.categories.push(tgtCat);
        categoriesMerged++;
      }
      for (const ch of srcCat.chunks) {
        if (this._isIdTaken(target, ch.id)) { chunksSkipped++; continue; }
        tgtCat.chunks.push({ ...JSON.parse(JSON.stringify(ch)), _uid: crypto.randomUUID() });
        chunksAdded++;
      }
    }

    this._save(targetName, target);
    if (chunksAdded) this._commit(targetName, 'mergeProjects', `Merged '${sourceName}' into '${targetName}' (${chunksAdded} chunks)`, opts?.source);
    return { source: sourceName, target: targetName, categoriesMerged, chunksAdded, chunksSkipped };
  }

  // ---- EXPORT CATEGORY ----

  exportCategory(projectName, categoryName) {
    const data = this._load(projectName);
    const cat = this._findCategory(data, categoryName);
    const flat = [];
    for (const ch of cat.chunks) {
      const entry = { id: ch.id, text: ch.text, metadata: { ...ch.metadata } };
      if (ch.customFields) {
        for (const cf of ch.customFields) {
          if (cf.key && cf.key.trim()) entry.metadata[cf.key.trim()] = String(cf.value ?? '');
        }
      }
      flat.push(entry);
    }
    return flat;
  }

  // ---- HISTORY ----

  _historyFilePath(name) {
    return path.join(this.dataDir, `${name}.history.json`);
  }

  _loadHistory(name) {
    const fp = this._historyFilePath(name);
    if (!fs.existsSync(fp)) return { project: name, commits: [] };
    return JSON.parse(fs.readFileSync(fp, 'utf-8'));
  }

  _saveHistory(name, history) {
    this._ensureDir();
    fs.writeFileSync(this._historyFilePath(name), JSON.stringify(history, null, 2), 'utf-8');
  }

  _commit(projectName, action, summary, source) {
    try {
      const data = this._load(projectName);
      const history = this._loadHistory(projectName);
      const newSnapshot = JSON.stringify(data);

      // Skip if snapshot is identical to the last commit (no actual change)
      if (history.commits.length > 0 && JSON.stringify(history.commits[0].snapshot) === newSnapshot) {
        return;
      }

      const totalChunks = data.categories.reduce((sum, c) => sum + c.chunks.length, 0);
      history.commits.unshift({
        id: crypto.randomUUID(),
        timestamp: new Date().toISOString(),
        source: source || 'browser',
        action,
        summary,
        stats: { categories: data.categories.length, chunks: totalChunks },
        snapshot: JSON.parse(newSnapshot),
      });
      if (history.commits.length > MAX_HISTORY) history.commits.length = MAX_HISTORY;
      this._saveHistory(projectName, history);
    } catch { /* history logging should never break mutations */ }
  }

  getHistory(name) {
    const history = this._loadHistory(name);
    return history.commits.map(c => ({
      id: c.id, timestamp: c.timestamp, source: c.source,
      action: c.action, summary: c.summary, stats: c.stats,
    }));
  }

  getCommit(name, commitId) {
    const history = this._loadHistory(name);
    const idx = history.commits.findIndex(c => c.id === commitId);
    if (idx === -1) throw new Error('Commit not found');
    const commit = history.commits[idx];
    const prev = idx + 1 < history.commits.length ? history.commits[idx + 1].snapshot : null;
    return { ...commit, prevSnapshot: prev };
  }

  recordHistory(name, action, summary, source) {
    this._commit(name, action, summary, source || 'browser');
    return { recorded: true };
  }

  rollback(name, commitId, source) {
    const history = this._loadHistory(name);
    const commit = history.commits.find(c => c.id === commitId);
    if (!commit) throw new Error('Commit not found');
    this._save(name, commit.snapshot);
    this._commit(name, 'rollback', `Rolled back to commit from ${commit.timestamp}`, source || 'browser');
    return this._load(name);
  }

  // ---- INTERNAL ----

  _load(name) {
    const fp = this._filePath(name);
    if (!fs.existsSync(fp)) throw new Error(`Project "${name}" not found`);
    return JSON.parse(fs.readFileSync(fp, 'utf-8'));
  }

  _save(name, data) {
    this._ensureDir();
    fs.writeFileSync(this._filePath(name), JSON.stringify(data, null, 2), 'utf-8');
  }

  _findCategory(data, name) {
    const cat = data.categories.find(c => c.name.toLowerCase() === name.toLowerCase());
    if (!cat) throw new Error(`Category "${name}" not found`);
    return cat;
  }

  _isIdTaken(data, id, excludeUid) {
    for (const cat of data.categories) {
      for (const ch of cat.chunks) {
        if (ch.id === id && ch._uid !== excludeUid) return true;
      }
    }
    return false;
  }

  _parseCustomFields(metadata) {
    if (!metadata || typeof metadata !== 'object') return [];
    return Object.entries(metadata).filter(([k]) => !STANDARD_META.includes(k)).map(([key, value]) => ({ key, value: String(value ?? '') }));
  }

  _formatChunk(ch) {
    return { id: ch.id, text: ch.text, metadata: ch.metadata, customFields: ch.customFields || [] };
  }
}

module.exports = { Store };


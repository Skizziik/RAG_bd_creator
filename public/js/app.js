/* =============================================
   Dataset Builder by Tryll Engine
   Main Application (API + WebSocket)
   ============================================= */

// =============================================
// CONFIG
// =============================================
const CONFIG = {
  ONBOARDING_KEY: 'dataset_builder_onboarding_done',
  DEFAULT_LICENSE: 'CC BY-NC-SA 3.0',
};

const ONBOARDING_STEPS = [
  { target: '#newProjectBtn', text: 'Start by creating a new project. Click the <strong>+</strong> button.', position: 'bottom' },
  { target: '#addCategoryBtn', text: 'Great! Now add a <strong>category</strong> to organize your chunks.', position: 'right' },
  { target: null, text: 'Add your first <strong>chunk</strong> inside the category.', position: 'right', dynamicTarget: '[data-action="add-chunk"].btn-icon--accent' },
  { target: '#saveChunkBtn', text: 'Fill in the fields and hit <strong>Save</strong> when ready.', position: 'top' },
  { target: '#exportBtn', text: 'All done! Hit <strong>Forge JSON</strong> to export your dataset.', position: 'bottom' },
];

// =============================================
// UTILS
// =============================================
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

async function api(path, opts = {}) {
  const res = await fetch('/api' + path, {
    headers: { 'Content-Type': 'application/json', ...opts.headers },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || 'API error');
  return data;
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

// =============================================
// STORE — State management via REST API
// =============================================
class Store {
  constructor() {
    this.projectList = [];
    this.currentProjectName = null;
    this.currentProject = null;
    this._listeners = [];
    this.sessionCode = null;
    this._ws = null;
    this._mcpConnected = false;
  }

  async init() {
    const sess = await api('/session');
    this.sessionCode = sess.code;
    this._connectWS();
    await this.refreshProjectList();
  }

  _connectWS() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    this._ws = new WebSocket(`${proto}//${location.host}/ws?session=${this.sessionCode}&type=browser`);

    this._ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.event === 'mcp:connected') {
          this._mcpConnected = true;
          this._notify();
        } else if (msg.event === 'mcp:disconnected') {
          this._mcpConnected = false;
          this._notify();
        } else if (msg.event === 'data:changed') {
          this._handleRemoteChange(msg.data);
        } else if (msg.event === 'project:created') {
          this._handleRemoteProjectCreated(msg.data);
        } else if (msg.event === 'project:deleted') {
          this._handleRemoteProjectDeleted(msg.data);
        }
      } catch {}
    };

    this._ws.onclose = () => {
      this._mcpConnected = false;
      setTimeout(() => this._connectWS(), 3000);
    };
  }

  async _handleRemoteChange(data) {
    if (data && data.project) {
      await this.refreshProjectList();
      // Auto-select the project if none is selected
      if (!this.currentProjectName) {
        await this._loadProject(data.project);
      } else if (this.currentProjectName === data.project) {
        await this._loadProject(data.project);
      }
    } else {
      await this.refreshProjectList();
      if (this.currentProjectName) {
        try { await this._loadProject(this.currentProjectName); } catch {}
      }
    }
    this._notify();
  }

  async _handleRemoteProjectCreated(data) {
    await this.refreshProjectList();
    // Auto-select the newly created project
    if (data && data.name) {
      await this._loadProject(data.name);
    }
    this._notify();
  }

  async _handleRemoteProjectDeleted(data) {
    await this.refreshProjectList();
    if (data && this.currentProjectName === data.deleted) {
      this.currentProjectName = this.projectList.length ? this.projectList[0].name : null;
      if (this.currentProjectName) {
        await this._loadProject(this.currentProjectName);
      } else {
        this.currentProject = null;
      }
    }
    this._notify();
  }

  _notify() {
    this._listeners.forEach(fn => fn());
  }

  onChange(fn) {
    this._listeners.push(fn);
  }

  // ---- PROJECTS ----

  async refreshProjectList() {
    this.projectList = await api('/projects');
  }

  getProjects() {
    return this.projectList;
  }

  getCurrentProject() {
    return this.currentProject;
  }

  async selectProject(name) {
    this.currentProjectName = name;
    if (name) {
      await this._loadProject(name);
    } else {
      this.currentProject = null;
    }
    this._notify();
  }

  async _loadProject(name) {
    try {
      this.currentProject = await api(`/projects/${encodeURIComponent(name)}`);
      this.currentProjectName = name;
    } catch {
      this.currentProject = null;
      this.currentProjectName = null;
    }
  }

  async createProject(name) {
    const result = await api('/projects', { method: 'POST', body: { name, session: this.sessionCode } });
    await this.refreshProjectList();
    this.currentProjectName = result.name;
    this.currentProject = result;
    this._notify();
    return result;
  }

  async deleteProject(name) {
    await api(`/projects/${encodeURIComponent(name)}?session=${this.sessionCode}`, { method: 'DELETE' });
    await this.refreshProjectList();
    if (this.currentProjectName === name) {
      this.currentProjectName = this.projectList.length ? this.projectList[0].name : null;
      if (this.currentProjectName) {
        await this._loadProject(this.currentProjectName);
      } else {
        this.currentProject = null;
      }
    }
    this._notify();
  }

  // ---- CATEGORIES ----

  getCategories() {
    return this.currentProject ? this.currentProject.categories : [];
  }

  async addCategory(name) {
    if (!this.currentProjectName) return null;
    const result = await api(`/projects/${encodeURIComponent(this.currentProjectName)}/categories`, {
      method: 'POST', body: { name, session: this.sessionCode },
    });
    await this._loadProject(this.currentProjectName);
    this._notify();
    return result;
  }

  async deleteCategory(catId) {
    if (!this.currentProject) return;
    const cat = this.currentProject.categories.find(c => c.id === catId);
    if (!cat) return;
    await api(`/projects/${encodeURIComponent(this.currentProjectName)}/categories/${encodeURIComponent(cat.name)}?session=${this.sessionCode}`, { method: 'DELETE' });
    await this._loadProject(this.currentProjectName);
    this._notify();
  }

  async renameCategory(catId, newName) {
    if (!this.currentProject) return;
    const cat = this.currentProject.categories.find(c => c.id === catId);
    if (!cat) return;
    await api(`/projects/${encodeURIComponent(this.currentProjectName)}/categories/${encodeURIComponent(cat.name)}`, {
      method: 'PUT', body: { newName, session: this.sessionCode },
    });
    await this._loadProject(this.currentProjectName);
    this._notify();
  }

  async toggleCategory(catId) {
    if (!this.currentProject) return;
    // Toggle locally for instant UI response
    const cat = this.currentProject.categories.find(c => c.id === catId);
    if (cat) cat.expanded = !cat.expanded;
    this._notify();
    // Sync to server (fire and forget)
    api(`/projects/${encodeURIComponent(this.currentProjectName)}/categories/${catId}/toggle`, { method: 'POST' }).catch(() => {});
  }

  // ---- CHUNKS ----

  async addChunk(catId) {
    if (!this.currentProjectName) return null;
    const result = await api(`/projects/${encodeURIComponent(this.currentProjectName)}/categories/${catId}/chunks/blank`, { method: 'POST' });
    await this._loadProject(this.currentProjectName);
    this._notify();
    return { categoryId: catId, chunkUid: result._uid };
  }

  getChunk(catId, chunkUid) {
    if (!this.currentProject) return null;
    const cat = this.currentProject.categories.find(c => c.id === catId);
    if (!cat) return null;
    return cat.chunks.find(ch => ch._uid === chunkUid) || null;
  }

  async updateChunk(catId, chunkUid, data) {
    if (!this.currentProjectName) return;
    // Update locally for instant feedback
    const cat = this.currentProject?.categories.find(c => c.id === catId);
    if (cat) {
      const idx = cat.chunks.findIndex(ch => ch._uid === chunkUid);
      if (idx !== -1) cat.chunks[idx] = { ...cat.chunks[idx], ...data };
    }
    await api(`/projects/${encodeURIComponent(this.currentProjectName)}/categories/${catId}/chunks/${chunkUid}`, {
      method: 'PUT', body: { ...data, session: this.sessionCode },
    });
  }

  async deleteChunk(catId, chunkUid) {
    if (!this.currentProjectName) return;
    await api(`/projects/${encodeURIComponent(this.currentProjectName)}/categories/${catId}/chunks/${chunkUid}?session=${this.sessionCode}`, { method: 'DELETE' });
    await this._loadProject(this.currentProjectName);
    this._notify();
  }

  async duplicateChunk(catId, chunkUid) {
    if (!this.currentProjectName) return null;
    const result = await api(`/projects/${encodeURIComponent(this.currentProjectName)}/categories/${catId}/chunks/${chunkUid}/duplicate`, { method: 'POST' });
    await this._loadProject(this.currentProjectName);
    this._notify();
    return { categoryId: catId, chunkUid: result._uid };
  }

  // ---- COUNTS ----

  getTotalChunks() {
    if (!this.currentProject) return 0;
    return this.currentProject.categories.reduce((sum, cat) => sum + cat.chunks.length, 0);
  }

  isChunkIdTaken(id, excludeUid) {
    if (!id || !this.currentProject) return false;
    for (const cat of this.currentProject.categories) {
      for (const chunk of cat.chunks) {
        if (chunk._uid !== excludeUid && chunk.id === id) return true;
      }
    }
    return false;
  }

  // ---- IMPORT ----

  async importProject(name, jsonArray) {
    const result = await api(`/projects/${encodeURIComponent(name)}/import`, {
      method: 'POST', body: { data: jsonArray, session: this.sessionCode },
    });
    await this.refreshProjectList();
    this.currentProjectName = name;
    await this._loadProject(name);
    this._notify();
    return result;
  }

  // ---- EXPORT ----

  async exportJSON() {
    if (!this.currentProjectName) return null;
    return await api(`/projects/${encodeURIComponent(this.currentProjectName)}/export`);
  }

  // ---- HISTORY ----

  async getHistory() {
    if (!this.currentProjectName) return [];
    return await api(`/projects/${encodeURIComponent(this.currentProjectName)}/history`);
  }

  async getCommitDetail(commitId) {
    if (!this.currentProjectName) return null;
    return await api(`/projects/${encodeURIComponent(this.currentProjectName)}/history/${commitId}`);
  }

  async rollback(commitId) {
    if (!this.currentProjectName) return null;
    const result = await api(`/projects/${encodeURIComponent(this.currentProjectName)}/history/${commitId}/rollback`, {
      method: 'POST', body: { session: this.sessionCode },
    });
    await this._loadProject(this.currentProjectName);
    this._notify();
    return result;
  }
}

// =============================================
// APP — UI Controller
// =============================================
class App {
  constructor() {
    this.store = new Store();
    this.selected = null;
    this._onboardingStep = null;
    this._boot();
  }

  async _boot() {
    this._cacheEls();
    this._bindEvents();
    await this.store.init();
    this.store.onChange(() => this.render());
    this.render();
    this._renderSessionCode();
    this._initOnboarding();
  }

  _cacheEls() {
    this.els = {
      projectSelect: $('#projectSelect'),
      newProjectBtn: $('#newProjectBtn'),
      deleteProjectBtn: $('#deleteProjectBtn'),
      addCategoryBtn: $('#addCategoryBtn'),
      newCategoryWrap: $('#newCategoryWrap'),
      newCategoryName: $('#newCategoryName'),
      confirmCategoryBtn: $('#confirmCategoryBtn'),
      cancelCategoryBtn: $('#cancelCategoryBtn'),
      categoryTree: $('#categoryTree'),
      content: $('#content'),
      exportBtn: $('#exportBtn'),
      modalOverlay: $('#modalOverlay'),
      modalContent: $('#modalContent'),
      toastContainer: $('#toastContainer'),
      importProjectBtn: $('#importProjectBtn'),
      importFileInput: $('#importFileInput'),
      searchChunkBtn: $('#searchChunkBtn'),
      chunkSearchWrap: $('#chunkSearchWrap'),
      chunkSearchInput: $('#chunkSearchInput'),
      clearSearchBtn: $('#clearSearchBtn'),
      sessionCode: $('#sessionCode'),
      mcpStatus: $('#mcpStatus'),
      historyBtn: $('#historyBtn'),
      historyDrawer: $('#historyDrawer'),
      historyCloseBtn: $('#historyCloseBtn'),
      historyList: $('#historyList'),
    };
  }

  _bindEvents() {
    this.els.projectSelect.addEventListener('change', async (e) => {
      this.selected = null;
      await this.store.selectProject(e.target.value);
    });

    this.els.newProjectBtn.addEventListener('click', () => this._showNewProjectModal());
    this.els.importProjectBtn.addEventListener('click', () => this.els.importFileInput.click());
    this.els.importFileInput.addEventListener('change', (e) => this._handleImport(e));
    this.els.deleteProjectBtn.addEventListener('click', () => this._handleDeleteProject());

    this.els.addCategoryBtn.addEventListener('click', () => this._showCategoryInput());
    this.els.confirmCategoryBtn.addEventListener('click', () => this._confirmCategory());
    this.els.cancelCategoryBtn.addEventListener('click', () => this._hideCategoryInput());
    this.els.newCategoryName.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') this._confirmCategory();
      if (e.key === 'Escape') this._hideCategoryInput();
    });

    this.els.categoryTree.addEventListener('click', (e) => this._handleTreeClick(e));

    this.els.searchChunkBtn.addEventListener('click', () => this._toggleSearch());
    this.els.chunkSearchInput.addEventListener('input', () => this._filterChunks());
    this.els.clearSearchBtn.addEventListener('click', () => this._clearSearch());
    this.els.chunkSearchInput.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') this._clearSearch();
    });

    this.els.exportBtn.addEventListener('click', () => this._handleExport());

    this.els.historyBtn.addEventListener('click', () => this._toggleHistory());
    this.els.historyCloseBtn.addEventListener('click', () => this._closeHistory());
    this.els.historyList.addEventListener('click', (e) => this._handleHistoryClick(e));

    this.els.modalOverlay.addEventListener('click', (e) => {
      if (e.target === this.els.modalOverlay) this._closeModal();
    });

    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') this._closeModal();
    });
  }

  // ---- RENDER ----

  render() {
    this._renderProjectSelect();
    this._renderCategories();
    this._renderContent();
    this._renderMcpStatus();
    if (this._onboardingStep !== null) {
      setTimeout(() => this._showOnboardingStep(), 50);
    }
  }

  _renderSessionCode() {
    if (this.els.sessionCode) {
      this.els.sessionCode.textContent = this.store.sessionCode || '...';
    }
  }

  _renderMcpStatus() {
    if (this.els.mcpStatus) {
      this.els.mcpStatus.classList.toggle('connected', this.store._mcpConnected);
      this.els.mcpStatus.title = this.store._mcpConnected ? 'MCP Connected' : 'MCP Not Connected';
    }
  }

  _renderProjectSelect() {
    const projects = this.store.getProjects();
    const currentName = this.store.currentProjectName;
    let html = '<option value="" disabled>— Select Project —</option>';
    for (const p of projects) {
      const sel = p.name === currentName ? 'selected' : '';
      html += `<option value="${this._escAttr(p.name)}" ${sel}>${this._esc(p.name)}</option>`;
    }
    this.els.projectSelect.innerHTML = html;
    if (currentName) {
      this.els.projectSelect.value = currentName;
    } else {
      this.els.projectSelect.value = '';
    }
  }

  _renderCategories() {
    const cats = this.store.getCategories();
    if (!this.store.getCurrentProject()) {
      this.els.categoryTree.innerHTML = `
        <div style="padding:20px 16px;text-align:center;">
          <p style="color:var(--text-muted);font-size:12px;">Create or select a project to begin</p>
        </div>`;
      return;
    }
    if (cats.length === 0) {
      this.els.categoryTree.innerHTML = `
        <div style="padding:20px 16px;text-align:center;">
          <p style="color:var(--text-muted);font-size:12px;">No categories yet.<br>Hit <strong>+</strong> to create one!</p>
        </div>`;
      return;
    }

    let html = '';
    for (const cat of cats) {
      const expanded = cat.expanded;
      const isActive = this.selected && this.selected.categoryId === cat.id;
      html += `
        <div class="category-item" data-cat-id="${cat.id}">
          <div class="category-header ${isActive ? 'active' : ''}">
            <i class="bi bi-chevron-right category-chevron ${expanded ? 'expanded' : ''}" data-action="toggle" data-cat-id="${cat.id}"></i>
            <i class="bi bi-folder2-open category-icon"></i>
            <span class="category-name" data-action="toggle" data-cat-id="${cat.id}">${this._esc(cat.name)}</span>
            <span class="category-count">${cat.chunks.length}</span>
            <div class="category-actions">
              <button class="btn-icon btn-icon--accent" data-action="add-chunk" data-cat-id="${cat.id}" title="Add Chunk"><i class="bi bi-plus-lg"></i></button>
              <button class="btn-icon btn-icon--danger" data-action="delete-cat" data-cat-id="${cat.id}" title="Delete Category"><i class="bi bi-trash3"></i></button>
            </div>
          </div>
          <div class="chunk-list ${expanded ? '' : 'collapsed'}" id="chunks-${cat.id}">`;

      for (const chunk of cat.chunks) {
        const isSelected = this.selected && this.selected.chunkUid === chunk._uid;
        const label = chunk.id || 'untitled chunk';
        html += `
            <div class="chunk-item ${isSelected ? 'selected' : ''}" data-action="select-chunk" data-cat-id="${cat.id}" data-chunk-uid="${chunk._uid}">
              <i class="bi bi-file-earmark-text chunk-item-icon"></i>
              <span class="chunk-item-name">${this._esc(label)}</span>
              <button class="btn-icon btn-icon--danger chunk-item-delete" data-action="delete-chunk" data-cat-id="${cat.id}" data-chunk-uid="${chunk._uid}" title="Delete"><i class="bi bi-x-lg"></i></button>
            </div>`;
      }

      html += `
            <button class="add-chunk-btn" data-action="add-chunk" data-cat-id="${cat.id}">
              <i class="bi bi-plus"></i> Add chunk
            </button>
          </div>
        </div>`;
    }
    this.els.categoryTree.innerHTML = html;

    for (const cat of cats) {
      const el = $(`#chunks-${cat.id}`);
      if (el && cat.expanded) {
        el.style.maxHeight = el.scrollHeight + 'px';
      }
    }
  }

  _renderContent() {
    const project = this.store.getCurrentProject();

    if (!project) {
      if (this.els.content.querySelector('.welcome-screen')) return;
      this.els.content.innerHTML = `
        <div class="welcome-screen">
          <div class="welcome-logo"><img src="/img/logo.png" alt="Tryll Engine" class="welcome-logo-img"></div>
          <div class="welcome-title">Dataset <span class="accent">Builder</span></div>
          <p class="welcome-desc">
            by Tryll Engine<br><br>
            Create a project to start building your knowledge base — one chunk at a time.
          </p>
          <div style="display:flex;gap:12px;margin-top:8px;flex-wrap:wrap;justify-content:center;">
            <button class="btn btn-accent" id="welcomeNewProject">
              <i class="bi bi-plus-lg"></i> New Project
            </button>
            <button class="btn btn-secondary" id="welcomeImport">
              <i class="bi bi-upload"></i> Import JSON
            </button>
            <button class="btn btn-secondary" id="welcomeBatchWiki">
              <i class="bi bi-globe2"></i> Batch Wiki
            </button>
          </div>
        </div>`;
      const btn = $('#welcomeNewProject');
      if (btn) btn.addEventListener('click', () => this._showNewProjectModal());
      const impBtn = $('#welcomeImport');
      if (impBtn) impBtn.addEventListener('click', () => this.els.importFileInput.click());
      const batchBtn = $('#welcomeBatchWiki');
      if (batchBtn) batchBtn.addEventListener('click', () => this._showBatchWikiStep1());
      return;
    }

    if (!this.selected) {
      if (!this.els.content.querySelector('.empty-state')) {
        this.els.content.innerHTML = `
          <div class="empty-state">
            <div class="empty-state-icon"><i class="bi bi-box-seam"></i></div>
            <div class="empty-state-title">Your inventory is empty</div>
            <p class="empty-state-text">
              Select a chunk from the sidebar to edit it, or create a new category and add chunks to start building your database.
            </p>
          </div>`;
      }
      return;
    }

    const chunk = this.store.getChunk(this.selected.categoryId, this.selected.chunkUid);
    if (!chunk) {
      this.selected = null;
      this._renderContent();
      return;
    }

    const customFieldsHtml = (chunk.customFields || []).map((cf, i) => `
      <div class="custom-field-row">
        <input class="field-input custom-field-key" type="text" placeholder="Field name" value="${this._escAttr(cf.key)}" data-cf-index="${i}" data-cf-part="key">
        <input class="field-input" type="text" placeholder="Value" value="${this._escAttr(cf.value)}" data-cf-index="${i}" data-cf-part="value">
        <button class="btn-icon btn-icon--danger" data-action="remove-cf" data-cf-index="${i}" title="Remove field"><i class="bi bi-x-lg"></i></button>
      </div>
    `).join('');

    this.els.content.innerHTML = `
      <div class="chunk-editor">
        <div class="editor-header">
          <div class="editor-title">
            <i class="bi bi-file-earmark-code"></i>
            Chunk Editor
          </div>
          <div class="editor-actions">
            <button class="btn btn-ghost" id="importWikiBtn" title="Import from Wiki">
              <i class="bi bi-globe2"></i> Wiki
            </button>
            <button class="btn btn-ghost" id="duplicateChunkBtn" title="Duplicate">
              <i class="bi bi-copy"></i> Duplicate
            </button>
            <button class="btn btn-ghost" id="reportChunkBtn" title="Report">
              <i class="bi bi-flag"></i>
            </button>
            <button class="btn btn-danger" id="deleteChunkBtn">
              <i class="bi bi-trash3"></i> Delete
            </button>
          </div>
        </div>

        <div class="editor-card">
          <div class="editor-card-title"><i class="bi bi-tag"></i> Core Data</div>
          <div class="field-group">
            <label class="field-label">ID</label>
            <input class="field-input" type="text" id="chunkId" placeholder="e.g. sword_basic" value="${this._escAttr(chunk.id)}">
            <div class="id-warning hidden" id="idWarning"><i class="bi bi-exclamation-triangle"></i> This ID already exists. Try adding _1, _2, etc.</div>
          </div>
          <div class="field-group">
            <label class="field-label">Text</label>
            <textarea class="field-textarea" id="chunkText" placeholder="Main chunk content..." maxlength="2000">${this._esc(chunk.text)}</textarea>
            <div class="char-counter" id="charCounter"><span id="charCount">${(chunk.text || '').length}</span> / 2000</div>
          </div>
        </div>

        <div class="editor-card">
          <div class="editor-card-title"><i class="bi bi-database"></i> Metadata</div>
          <div class="field-group">
            <label class="field-label">Page Title</label>
            <input class="field-input" type="text" id="metaPageTitle" placeholder="Page title" value="${this._escAttr(chunk.metadata.page_title)}">
          </div>
          <div class="field-group">
            <label class="field-label">Source</label>
            <input class="field-input" type="text" id="metaSource" placeholder="Source URL or name" value="${this._escAttr(chunk.metadata.source)}">
          </div>
          <div class="field-group">
            <label class="field-label">License</label>
            <input class="field-input" type="text" id="metaLicense" placeholder="License type" value="${this._escAttr(chunk.metadata.license)}">
          </div>
        </div>

        <div class="editor-card">
          <div class="editor-card-title"><i class="bi bi-sliders"></i> Custom Fields</div>
          <div id="customFieldsContainer">
            ${customFieldsHtml}
          </div>
          <button class="add-field-btn" id="addCustomFieldBtn">
            <i class="bi bi-plus-lg"></i> Add Custom Field
          </button>
        </div>

        <div class="editor-save-bar">
          <button class="btn btn-accent btn-block" id="saveChunkBtn">
            <i class="bi bi-check-lg"></i> Save Chunk
          </button>
        </div>
      </div>`;

    this._bindEditorEvents();
  }

  // ---- EDITOR EVENTS ----

  _bindEditorEvents() {
    const saveBtn = $('#saveChunkBtn');
    const deleteBtn = $('#deleteChunkBtn');
    const duplicateBtn = $('#duplicateChunkBtn');
    const addCfBtn = $('#addCustomFieldBtn');
    const cfContainer = $('#customFieldsContainer');

    const importWikiBtn = $('#importWikiBtn');
    const reportBtn = $('#reportChunkBtn');

    if (saveBtn) saveBtn.addEventListener('click', () => this._saveCurrentChunk());
    if (deleteBtn) deleteBtn.addEventListener('click', () => this._deleteCurrentChunk());
    if (duplicateBtn) duplicateBtn.addEventListener('click', () => this._duplicateCurrentChunk());
    if (addCfBtn) addCfBtn.addEventListener('click', () => this._addCustomField());
    if (importWikiBtn) importWikiBtn.addEventListener('click', () => this._showImportWikiModal());
    if (reportBtn) reportBtn.addEventListener('click', () => this._showReportModal());

    const chunkText = $('#chunkText');
    const charCount = $('#charCount');
    const charCounter = $('#charCounter');
    if (chunkText && charCount) {
      chunkText.addEventListener('input', () => {
        const len = chunkText.value.length;
        charCount.textContent = len;
        charCounter.classList.toggle('char-counter--warn', len >= 1800);
        charCounter.classList.toggle('char-counter--limit', len >= 2000);
      });
    }

    if (cfContainer) {
      cfContainer.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-action="remove-cf"]');
        if (btn) {
          const idx = parseInt(btn.dataset.cfIndex, 10);
          this._removeCustomField(idx);
        }
      });
    }

    const handler = (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 's') {
        e.preventDefault();
        this._saveCurrentChunk();
      }
    };
    if (this._ctrlSHandler) document.removeEventListener('keydown', this._ctrlSHandler);
    this._ctrlSHandler = handler;
    document.addEventListener('keydown', handler);
  }

  // ---- TREE CLICK HANDLER ----

  _handleTreeClick(e) {
    const target = e.target.closest('[data-action]');
    if (!target) return;

    const action = target.dataset.action;
    const catId = target.dataset.catId;
    const chunkUid = target.dataset.chunkUid;

    switch (action) {
      case 'toggle':
        this.store.toggleCategory(catId);
        break;
      case 'add-chunk':
        e.stopPropagation();
        this._handleAddChunk(catId);
        break;
      case 'delete-cat':
        e.stopPropagation();
        this._handleDeleteCategory(catId);
        break;
      case 'select-chunk':
        this.selected = { categoryId: catId, chunkUid };
        this.render();
        break;
      case 'delete-chunk':
        e.stopPropagation();
        this._handleDeleteChunkFromTree(catId, chunkUid);
        break;
    }
  }

  // ---- PROJECT ACTIONS ----

  _showNewProjectModal() {
    this.els.modalContent.innerHTML = `
      <div class="modal-title"><i class="bi bi-rocket-takeoff"></i> New Project</div>
      <p class="modal-text">Give your project a name. This will be the filename when you export.</p>
      <input class="modal-input" type="text" id="modalProjectName" placeholder="e.g. weapons_database" maxlength="100" autofocus>
      <div class="modal-actions">
        <button class="btn btn-secondary" id="modalCancel">Cancel</button>
        <button class="btn btn-accent" id="modalConfirm"><i class="bi bi-plus-lg"></i> Create</button>
      </div>`;
    this.els.modalOverlay.classList.remove('hidden');

    const input = $('#modalProjectName');
    const confirm = $('#modalConfirm');
    const cancel = $('#modalCancel');

    setTimeout(() => input.focus(), 100);

    const create = async () => {
      const name = input.value.trim();
      if (!name) { input.style.borderColor = 'var(--danger)'; return; }
      try {
        this.selected = null;
        await this.store.createProject(name);
        this._closeModal();
        this._toast('Project created!', 'success');
        this._advanceOnboarding(0);
      } catch (err) {
        this._toast(err.message, 'error');
      }
    };

    confirm.addEventListener('click', create);
    cancel.addEventListener('click', () => this._closeModal());
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') create();
      if (e.key === 'Escape') this._closeModal();
    });
  }

  _handleImport(e) {
    const file = e.target.files[0];
    if (!file) return;
    e.target.value = '';

    const reader = new FileReader();
    reader.onload = async (ev) => {
      try {
        const parsed = JSON.parse(ev.target.result);
        if (!Array.isArray(parsed)) { this._toast('Invalid format: expected a JSON array.', 'error'); return; }
        if (parsed.length === 0) { this._toast('JSON array is empty.', 'error'); return; }

        const projectName = file.name.replace(/\.json$/i, '');
        await this.store.importProject(projectName, parsed);
        this.selected = null;
        this.render();
        this._toast(`Imported! ${parsed.length} chunks loaded.`, 'success');
      } catch (err) {
        this._toast('Failed: ' + err.message, 'error');
      }
    };
    reader.readAsText(file);
  }

  _handleDeleteProject() {
    const project = this.store.getCurrentProject();
    if (!project) return;

    this.els.modalContent.innerHTML = `
      <div class="modal-title"><i class="bi bi-exclamation-triangle" style="color:var(--danger)"></i> Delete Project</div>
      <p class="modal-text">Are you sure you want to delete <strong>${this._esc(project.name)}</strong>? This cannot be undone.</p>
      <div class="modal-actions">
        <button class="btn btn-secondary" id="modalCancel">Cancel</button>
        <button class="btn btn-danger" id="modalConfirm"><i class="bi bi-trash3"></i> Delete</button>
      </div>`;
    this.els.modalOverlay.classList.remove('hidden');

    $('#modalConfirm').addEventListener('click', async () => {
      this.selected = null;
      await this.store.deleteProject(project.name);
      this._closeModal();
      this._toast('Project deleted.', 'info');
    });
    $('#modalCancel').addEventListener('click', () => this._closeModal());
  }

  // ---- CATEGORY ACTIONS ----

  _showCategoryInput() {
    if (!this.store.getCurrentProject()) { this._toast('Create a project first!', 'error'); return; }
    this.els.newCategoryWrap.classList.remove('hidden');
    this.els.newCategoryName.value = '';
    this.els.newCategoryName.focus();
  }

  _hideCategoryInput() {
    this.els.newCategoryWrap.classList.add('hidden');
    this.els.newCategoryName.value = '';
  }

  async _confirmCategory() {
    const name = this.els.newCategoryName.value.trim();
    if (!name) return;
    try {
      await this.store.addCategory(name);
      this._hideCategoryInput();
      this._toast(`Category "${name}" created!`, 'success');
      this._advanceOnboarding(1);
    } catch (err) {
      this._toast(err.message, 'error');
    }
  }

  _handleDeleteCategory(catId) {
    const cats = this.store.getCategories();
    const cat = cats.find(c => c.id === catId);
    if (!cat) return;

    if (cat.chunks.length > 0) {
      this.els.modalContent.innerHTML = `
        <div class="modal-title"><i class="bi bi-exclamation-triangle" style="color:var(--danger)"></i> Delete Category</div>
        <p class="modal-text">Category <strong>${this._esc(cat.name)}</strong> has ${cat.chunks.length} chunk(s). Deleting it will remove all chunks inside. Continue?</p>
        <div class="modal-actions">
          <button class="btn btn-secondary" id="modalCancel">Cancel</button>
          <button class="btn btn-danger" id="modalConfirm"><i class="bi bi-trash3"></i> Delete</button>
        </div>`;
      this.els.modalOverlay.classList.remove('hidden');

      $('#modalConfirm').addEventListener('click', async () => {
        if (this.selected && this.selected.categoryId === catId) this.selected = null;
        await this.store.deleteCategory(catId);
        this._closeModal();
        this._toast('Category deleted.', 'info');
      });
      $('#modalCancel').addEventListener('click', () => this._closeModal());
    } else {
      (async () => {
        if (this.selected && this.selected.categoryId === catId) this.selected = null;
        await this.store.deleteCategory(catId);
        this._toast('Category deleted.', 'info');
      })();
    }
  }

  // ---- CHUNK SEARCH ----

  _toggleSearch() {
    const wrap = this.els.chunkSearchWrap;
    if (wrap.classList.contains('hidden')) {
      wrap.classList.remove('hidden');
      this.els.chunkSearchInput.focus();
    } else {
      this._clearSearch();
    }
  }

  _clearSearch() {
    this.els.chunkSearchInput.value = '';
    this.els.chunkSearchWrap.classList.add('hidden');
    this.els.categoryTree.querySelectorAll('.chunk-item').forEach(el => el.style.display = '');
    this.els.categoryTree.querySelectorAll('.category-item').forEach(el => el.style.display = '');
  }

  _filterChunks() {
    const query = this.els.chunkSearchInput.value.toLowerCase().trim();
    const cats = this.els.categoryTree.querySelectorAll('.category-item');

    cats.forEach(catEl => {
      const chunks = catEl.querySelectorAll('.chunk-item');
      let visibleCount = 0;
      chunks.forEach(chunkEl => {
        const name = chunkEl.querySelector('.chunk-item-name')?.textContent.toLowerCase() || '';
        const match = !query || name.includes(query);
        chunkEl.style.display = match ? '' : 'none';
        if (match) visibleCount++;
      });
      catEl.style.display = (!query || visibleCount > 0) ? '' : 'none';
      if (query && visibleCount > 0) {
        const chunkList = catEl.querySelector('.chunk-list');
        if (chunkList && chunkList.classList.contains('collapsed')) {
          chunkList.classList.remove('collapsed');
          chunkList.style.maxHeight = chunkList.scrollHeight + 'px';
        }
      }
    });
  }

  // ---- CHUNK ACTIONS ----

  async _handleAddChunk(catId) {
    const result = await this.store.addChunk(catId);
    if (!result) return;
    const cat = this.store.getCategories().find(c => c.id === catId);
    if (cat && !cat.expanded) await this.store.toggleCategory(catId);
    this.selected = result;
    this.render();
    setTimeout(() => { const idInput = $('#chunkId'); if (idInput) idInput.focus(); }, 50);
    this._toast('New chunk created. Fill it in!', 'success');
    this._advanceOnboarding(2);
  }

  async _saveCurrentChunk() {
    if (!this.selected) return;

    const idVal = ($('#chunkId') || {}).value || '';
    const textVal = ($('#chunkText') || {}).value || '';
    const pageTitle = ($('#metaPageTitle') || {}).value || '';
    const source = ($('#metaSource') || {}).value || '';
    const license = ($('#metaLicense') || {}).value || '';

    if (idVal && this.store.isChunkIdTaken(idVal, this.selected.chunkUid)) {
      this._toast('This ID already exists. Try adding _1, _2, etc.', 'error');
      const idInput = $('#chunkId');
      if (idInput) idInput.style.borderColor = 'var(--danger)';
      return;
    }

    const customFields = [];
    $$('.custom-field-row').forEach(row => {
      const keyInput = row.querySelector('[data-cf-part="key"]');
      const valueInput = row.querySelector('[data-cf-part="value"]');
      if (keyInput && valueInput) customFields.push({ key: keyInput.value, value: valueInput.value });
    });

    try {
      await this.store.updateChunk(this.selected.categoryId, this.selected.chunkUid, {
        id: idVal, text: textVal,
        metadata: { page_title: pageTitle, source, license },
        customFields,
      });
      this._toast('Chunk saved!', 'success');
      this._advanceOnboarding(3);
      // Re-render sidebar to update chunk name
      this._renderCategories();
    } catch (err) {
      this._toast('Save failed: ' + err.message, 'error');
    }
  }

  _deleteCurrentChunk() {
    if (!this.selected) return;
    const chunk = this.store.getChunk(this.selected.categoryId, this.selected.chunkUid);
    const label = chunk ? (chunk.id || 'untitled') : 'this chunk';

    this.els.modalContent.innerHTML = `
      <div class="modal-title"><i class="bi bi-exclamation-triangle" style="color:var(--danger)"></i> Delete Chunk</div>
      <p class="modal-text">Delete <strong>${this._esc(label)}</strong>? This cannot be undone.</p>
      <div class="modal-actions">
        <button class="btn btn-secondary" id="modalCancel">Cancel</button>
        <button class="btn btn-danger" id="modalConfirm"><i class="bi bi-trash3"></i> Delete</button>
      </div>`;
    this.els.modalOverlay.classList.remove('hidden');

    $('#modalConfirm').addEventListener('click', async () => {
      await this.store.deleteChunk(this.selected.categoryId, this.selected.chunkUid);
      this.selected = null;
      this._closeModal();
      this._toast('Chunk deleted.', 'info');
    });
    $('#modalCancel').addEventListener('click', () => this._closeModal());
  }

  _showReportModal() {
    if (!this.selected) return;
    const chunk = this.store.getChunk(this.selected.categoryId, this.selected.chunkUid);
    if (!chunk) return;
    const label = chunk.id || 'untitled';

    this.els.modalContent.innerHTML = `
      <div class="modal-title"><i class="bi bi-flag" style="color:var(--accent)"></i> Report Chunk</div>
      <p class="modal-text">Report <strong>${this._esc(label)}</strong> — select a reason:</p>
      <div class="report-reasons">
        <label class="report-reason"><input type="radio" name="reportReason" value="Bad parsing" checked> Bad parsing</label>
        <label class="report-reason"><input type="radio" name="reportReason" value="Missing info"> Missing info</label>
        <label class="report-reason"><input type="radio" name="reportReason" value="Wrong data"> Wrong data</label>
        <label class="report-reason"><input type="radio" name="reportReason" value="Other"> Other</label>
      </div>
      <textarea class="field-textarea" id="reportComment" placeholder="Optional comment..." rows="3" style="margin-top:12px"></textarea>
      <div class="modal-actions">
        <button class="btn btn-secondary" id="modalCancel">Cancel</button>
        <button class="btn btn-accent" id="reportSubmitBtn"><i class="bi bi-send"></i> Send Report</button>
      </div>`;
    this.els.modalOverlay.classList.remove('hidden');

    $('#reportSubmitBtn').addEventListener('click', async () => {
      const reason = this.els.modalContent.querySelector('input[name="reportReason"]:checked')?.value || 'Other';
      const comment = $('#reportComment')?.value?.trim() || '';

      const project = this.store.currentProject || '';
      const category = this.selected.categoryId || '';
      const customFields = (chunk.customFields || []).map(f => ({ key: f.key, value: f.value }));

      const btn = $('#reportSubmitBtn');
      btn.disabled = true;
      btn.innerHTML = '<i class="bi bi-arrow-repeat spin"></i> Sending...';

      try {
        const res = await fetch('/api/report', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            project,
            category,
            chunkId: chunk.id || '',
            chunkText: chunk.text || '',
            metadata: chunk.metadata || {},
            customFields,
            reason,
            comment,
          }),
        });
        if (!res.ok) throw new Error('Failed');
        this._closeModal();
        this._toast('Report sent!', 'success');
      } catch (e) {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-send"></i> Send Report';
        this._toast('Failed to send report', 'error');
      }
    });
    $('#modalCancel').addEventListener('click', () => this._closeModal());
  }

  _handleDeleteChunkFromTree(catId, chunkUid) {
    const chunk = this.store.getChunk(catId, chunkUid);
    const label = chunk ? (chunk.id || 'untitled') : 'this chunk';

    this.els.modalContent.innerHTML = `
      <div class="modal-title"><i class="bi bi-exclamation-triangle" style="color:var(--danger)"></i> Delete Chunk</div>
      <p class="modal-text">Delete <strong>${this._esc(label)}</strong>?</p>
      <div class="modal-actions">
        <button class="btn btn-secondary" id="modalCancel">Cancel</button>
        <button class="btn btn-danger" id="modalConfirm"><i class="bi bi-trash3"></i> Delete</button>
      </div>`;
    this.els.modalOverlay.classList.remove('hidden');

    $('#modalConfirm').addEventListener('click', async () => {
      if (this.selected && this.selected.chunkUid === chunkUid) this.selected = null;
      await this.store.deleteChunk(catId, chunkUid);
      this._closeModal();
      this._toast('Chunk deleted.', 'info');
    });
    $('#modalCancel').addEventListener('click', () => this._closeModal());
  }

  async _duplicateCurrentChunk() {
    if (!this.selected) return;
    const result = await this.store.duplicateChunk(this.selected.categoryId, this.selected.chunkUid);
    if (result) {
      this.selected = result;
      this.render();
      this._toast('Chunk duplicated!', 'success');
    }
  }

  _addCustomField() {
    const container = $('#customFieldsContainer');
    if (!container) return;
    const index = container.querySelectorAll('.custom-field-row').length;
    const row = document.createElement('div');
    row.className = 'custom-field-row';
    row.innerHTML = `
      <input class="field-input custom-field-key" type="text" placeholder="Field name" data-cf-index="${index}" data-cf-part="key">
      <input class="field-input" type="text" placeholder="Value" data-cf-index="${index}" data-cf-part="value">
      <button class="btn-icon btn-icon--danger" data-action="remove-cf" data-cf-index="${index}" title="Remove field"><i class="bi bi-x-lg"></i></button>`;
    container.appendChild(row);
    row.querySelector('.custom-field-key').focus();
  }

  _removeCustomField(index) {
    const rows = $$('.custom-field-row');
    if (rows[index]) rows[index].remove();
    // Re-index remaining rows
    $$('.custom-field-row').forEach((row, i) => {
      row.querySelectorAll('[data-cf-index]').forEach(el => el.dataset.cfIndex = i);
    });
  }

  async _saveCurrentChunkSilent() {
    if (!this.selected) return;
    const idVal = ($('#chunkId') || {}).value || '';
    const textVal = ($('#chunkText') || {}).value || '';
    const pageTitle = ($('#metaPageTitle') || {}).value || '';
    const source = ($('#metaSource') || {}).value || '';
    const license = ($('#metaLicense') || {}).value || '';
    const customFields = [];
    $$('.custom-field-row').forEach(row => {
      const keyInput = row.querySelector('[data-cf-part="key"]');
      const valueInput = row.querySelector('[data-cf-part="value"]');
      if (keyInput && valueInput) customFields.push({ key: keyInput.value, value: valueInput.value });
    });
    await this.store.updateChunk(this.selected.categoryId, this.selected.chunkUid, {
      id: idVal, text: textVal,
      metadata: { page_title: pageTitle, source, license },
      customFields,
    });
  }

  // ---- EXPORT ----

  async _handleExport() {
    const project = this.store.getCurrentProject();
    if (!project) { this._toast('No project selected!', 'error'); return; }

    await this._saveCurrentChunkSilent();

    try {
      const data = await this.store.exportJSON();
      if (!data || data.length === 0) { this._toast('No chunks to export.', 'error'); return; }

      const json = JSON.stringify(data, null, 2);
      const blob = new Blob([json], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${project.name.replace(/[^a-zA-Z0-9_-]/g, '_')}.json`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      this._toast(`Forged! ${data.length} chunks exported.`, 'success');
      this._advanceOnboarding(4);
    } catch (err) {
      this._toast('Export failed: ' + err.message, 'error');
    }
  }

  // ---- MODAL ----

  _closeModal() {
    this.els.modalOverlay.classList.add('hidden');
    this.els.modalContent.classList.remove('modal--faq');
  }

  _showFAQModal() {
    const faqData = [
      // ---- Basics ----
      ['What is Dataset Builder?',
        'A tool for building structured <strong>JSON datasets for RAG</strong> (Retrieval-Augmented Generation) pipelines. ' +
        'Organize knowledge into projects, categories, and chunks — then export as a flat JSON array ready for embedding and retrieval.'],
      ['How do I create a project?',
        'Click the <strong>+</strong> button in the top bar and enter a name. ' +
        'The project name is also used as the filename when you export.'],
      ['What are categories?',
        'Categories are folders that group related chunks — for example <em>Mobs</em>, <em>Weapons</em>, or <em>Biomes</em>. ' +
        'Click <strong>+ New Category</strong> in the sidebar to create one. You can rename or delete categories at any time.'],
      ['What is a chunk?',
        'A chunk is a single knowledge entry with a unique <strong>ID</strong>, <strong>text</strong> content, ' +
        '<strong>metadata</strong> (page title, source, license), and optional <strong>custom fields</strong>. ' +
        'Chunks are the building blocks of your dataset.'],
      ['Can I use duplicate chunk IDs?',
        'No — each chunk ID must be unique across the entire project. ' +
        'If you need similar names, add a suffix like <strong>_1</strong>, <strong>_2</strong>, etc. ' +
        'When duplicating a chunk, a <strong>_copy</strong> suffix is added automatically.'],
      ['What\'s the text character limit?',
        'Each chunk supports up to <strong>2000 characters</strong>. ' +
        'The counter below the text field turns <strong>yellow at 1800</strong> and <strong>red at 2000</strong>.'],
      // ---- Custom Fields ----
      ['What are Custom Fields?',
        'Custom fields let you add any extra metadata beyond the standard three (page title, source, license). ' +
        'Click <strong>Add Custom Field</strong> under the chunk editor, enter a field name and value, then save. ' +
        'On export, custom fields are merged into the <code>metadata</code> object alongside standard fields.'],
      // ---- Import & Export ----
      ['How do I export my dataset?',
        'Click the <strong>Forge JSON</strong> button in the top bar. ' +
        'Your browser downloads a <code>.json</code> file — a flat array of chunks with all metadata merged, ready for RAG pipelines. ' +
        'You can also export a single category via the category context menu.'],
      ['How do I import an existing dataset?',
        'Click the <strong>upload icon</strong> in the top bar and select a <code>.json</code> file. ' +
        'The file should be an array of objects with <code>id</code>, <code>text</code>, and <code>metadata</code> fields. ' +
        'Imported chunks go into an <strong>Imported</strong> category (or one you specify via MCP).'],
      // ---- Version History ----
      ['What is Version History?',
        'Every change you make — adding chunks, editing metadata, renaming categories — creates a <strong>commit</strong> in the project\'s history. ' +
        'Click the <strong>clock icon</strong> in the top bar to open the history drawer and see a timeline of all changes.'],
      ['How do diffs work?',
        'When you expand a commit, the app shows a colored diff:<br>' +
        '<strong style="color:#4ade80;">Green</strong> — added categories or chunks<br>' +
        '<strong style="color:#f87171;">Red</strong> — deleted categories or chunks<br>' +
        '<strong style="color:#fbbf24;">Yellow</strong> — modified chunks (shows exactly which fields changed and their old → new values)'],
      ['Can I undo changes?',
        'Yes — open Version History, find the commit you want to go back to, and click <strong>Rollback to this point</strong>. ' +
        'The project is restored to that commit\'s state. A new "rollback" commit is created, so you can always undo the rollback itself.'],
      ['What does "Browser" vs "MCP" mean in history?',
        'Each commit shows who made the change: ' +
        'a <strong style="color:#60a5fa;">blue dot</strong> means it was done in the browser, ' +
        'a <strong style="color:#4ade80;">green dot</strong> means it came from Claude Code via MCP. ' +
        'This helps track who changed what during collaborative sessions.'],
      // ---- MCP & Session ----
      ['What is MCP?',
        '<strong>MCP (Model Context Protocol)</strong> lets you use <strong>Claude Code</strong> as an AI assistant for building datasets. ' +
        'Install the companion MCP server: <code>npm i -g tryll-dataset-builder-mcp</code>, then add it to Claude Code: ' +
        '<code>claude mcp add dataset-builder -- npx tryll-dataset-builder-mcp</code>. ' +
        'Claude can then create projects, add chunks, parse URLs, and more — all via natural language.'],
      ['What is the Session Code?',
        'The 6-character code in the top bar connects your browser to Claude Code for <strong>real-time sync</strong>. ' +
        'Tell Claude: <em>"Connect to session ABC123"</em> — after that, any changes Claude makes appear instantly in your browser, and vice versa. ' +
        'History tracks whether each change was made by the browser or by MCP.'],
      ['What can Claude do via MCP?',
        'Claude has <strong>27 tools</strong> including: create/delete projects, manage categories, add/edit/delete chunks, ' +
        'bulk-add chunks, parse web pages into chunks, search chunks, import/export JSON, ' +
        'bulk-update metadata, merge projects, view version history, and rollback to any commit.'],
      // ---- Data & Storage ----
      ['Where is my data stored?',
        'Data is stored as <strong>JSON files on the server</strong> (<code>data/</code> folder). ' +
        'Each project has a <code>.json</code> data file and a <code>.history.json</code> file for version history. ' +
        'Export regularly to back up your work — the server filesystem may be reset on redeployment.'],
      ['What does the exported JSON look like?',
        'A flat array where each chunk\'s standard metadata and custom fields are merged into one <code>metadata</code> object:<br>' +
        '<code>[{ "id": "...", "text": "...", "metadata": { "page_title": "...", "source": "...", ... } }]</code>'],
    ];

    const faqItems = faqData.map(([q, a]) => `
      <div class="faq-item">
        <div class="faq-question"><span>${q}</span> <i class="bi bi-chevron-down"></i></div>
        <div class="faq-answer">${a}</div>
      </div>`).join('');

    this.els.modalContent.innerHTML = `
      <div class="modal-title"><i class="bi bi-chat-dots"></i> FAQ</div>
      <div class="faq-list">${faqItems}</div>
      <div class="modal-actions">
        <button class="btn btn-accent" id="modalClose">Close</button>
      </div>`;
    this.els.modalContent.classList.add('modal--faq');
    this.els.modalOverlay.classList.remove('hidden');

    this.els.modalContent.querySelectorAll('.faq-question').forEach(q => {
      q.addEventListener('click', () => q.parentElement.classList.toggle('open'));
    });

    $('#modalClose').addEventListener('click', () => {
      this.els.modalContent.classList.remove('modal--faq');
      this._closeModal();
    });
  }

  // ---- WIKI IMPORT ----

  _showImportWikiModal() {
    if (!this.selected) return;

    this.els.modalContent.innerHTML = `
      <div class="modal-title"><i class="bi bi-globe2"></i> Import from Wiki</div>
      <p class="modal-text">Paste a wiki page URL. The content, title, and infobox data will be parsed into chunks.</p>
      <input class="field-input" type="url" id="modalWikiUrl" placeholder="https://minecraft.wiki/w/Acacia_Log" autofocus>
      <div id="wikiImportStatus" class="wiki-import-status hidden"></div>
      <div class="modal-actions">
        <button class="btn btn-secondary" id="modalCancel">Cancel</button>
        <button class="btn btn-accent" id="modalConfirm"><i class="bi bi-download"></i> Import</button>
      </div>`;
    this.els.modalOverlay.classList.remove('hidden');

    const input = $('#modalWikiUrl');
    const confirm = $('#modalConfirm');
    const cancel = $('#modalCancel');
    const status = $('#wikiImportStatus');

    const doImport = async () => {
      const url = input.value.trim();
      if (!url) { input.classList.add('input-error'); return; }
      input.classList.remove('input-error');

      confirm.disabled = true;
      confirm.innerHTML = '<i class="bi bi-arrow-repeat spin"></i> Fetching...';
      status.classList.remove('hidden', 'wiki-import-error');
      status.textContent = 'Fetching and parsing page...';

      try {
        const parsed = await api('/wiki/parse', { method: 'POST', body: { url } });

        // Generate base ID from page title
        const baseId = (parsed.pageTitle || 'wiki')
          .toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '').substring(0, 60);

        // Split text into chunks
        const chunks = splitTextIntoChunks(parsed.text || '', baseId);

        // Build custom fields from infobox
        const customFields = Object.entries(parsed.infobox || {}).map(([key, value]) => ({ key, value }));

        // Fill current chunk editor form
        const firstChunk = chunks[0];
        const chunkId = $('#chunkId');
        const chunkText = $('#chunkText');
        const metaPageTitle = $('#metaPageTitle');
        const metaSource = $('#metaSource');
        const charCount = $('#charCount');
        const charCounter = $('#charCounter');

        if (chunkId) chunkId.value = firstChunk.id;
        if (chunkText) chunkText.value = firstChunk.text;
        if (metaPageTitle) metaPageTitle.value = parsed.pageTitle || '';
        if (metaSource) metaSource.value = parsed.source || '';
        if (charCount) charCount.textContent = firstChunk.text.length;
        if (charCounter) {
          charCounter.classList.toggle('char-counter--warn', firstChunk.text.length >= 1800);
          charCounter.classList.toggle('char-counter--limit', firstChunk.text.length >= 2000);
        }

        // Fill custom fields
        const cfContainer = $('#customFieldsContainer');
        if (cfContainer) {
          cfContainer.innerHTML = '';
          customFields.forEach((cf, i) => {
            const row = document.createElement('div');
            row.className = 'custom-field-row';
            row.innerHTML = `
              <input class="field-input custom-field-key" type="text" placeholder="Field name"
                     value="${this._escAttr(cf.key)}" data-cf-index="${i}" data-cf-part="key">
              <input class="field-input" type="text" placeholder="Value"
                     value="${this._escAttr(cf.value)}" data-cf-index="${i}" data-cf-part="value">
              <button class="btn-icon btn-icon--danger" data-action="remove-cf"
                      data-cf-index="${i}" title="Remove field"><i class="bi bi-x-lg"></i></button>`;
            cfContainer.appendChild(row);
          });
        }

        // Save the current (first) chunk immediately
        const license = ($('#metaLicense') || {}).value || CONFIG.DEFAULT_LICENSE;
        await this.store.updateChunk(this.selected.categoryId, this.selected.chunkUid, {
          id: firstChunk.id, text: firstChunk.text,
          metadata: { page_title: parsed.pageTitle || '', source: parsed.source || '', license },
          customFields,
        });

        // Create overflow chunks via bulk API
        if (chunks.length > 1) {
          const cat = this.store.currentProject.categories.find(c => c.id === this.selected.categoryId);
          if (cat) {
            const overflowData = chunks.slice(1).map(ch => ({
              id: ch.id, text: ch.text,
              metadata: {
                page_title: parsed.pageTitle || '',
                source: parsed.source || '',
                license,
                ...Object.fromEntries(customFields.map(cf => [cf.key, cf.value])),
              },
            }));
            await api(`/projects/${encodeURIComponent(this.store.currentProjectName)}/categories/${encodeURIComponent(cat.name)}/chunks/bulk`, {
              method: 'POST',
              body: { chunks: overflowData, session: this.store.sessionCode, source: 'browser' },
            });
          }
          await this.store._loadProject(this.store.currentProjectName);
          this.store._notify();
          this._toast(`Wiki imported! ${chunks.length} chunks created.`, 'success');
        } else {
          await this.store._loadProject(this.store.currentProjectName);
          this.store._notify();
          this._toast('Wiki imported!', 'success');
        }

        this._closeModal();
      } catch (err) {
        status.textContent = err.message;
        status.classList.add('wiki-import-error');
        confirm.disabled = false;
        confirm.innerHTML = '<i class="bi bi-download"></i> Import';
      }
    };

    confirm.addEventListener('click', doImport);
    cancel.addEventListener('click', () => this._closeModal());
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') doImport();
      if (e.key === 'Escape') this._closeModal();
    });
  }

  // ---- TOAST ----

  _toast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast toast--${type}`;
    const icons = { success: 'bi-check-circle', error: 'bi-x-circle', info: 'bi-info-circle' };
    toast.innerHTML = `<i class="bi ${icons[type] || icons.info}"></i> ${this._esc(message)}`;
    this.els.toastContainer.appendChild(toast);
    setTimeout(() => {
      toast.classList.add('leaving');
      setTimeout(() => toast.remove(), 300);
    }, 2500);
  }

  // ---- ONBOARDING ----

  _initOnboarding() {
    this._obTip = $('#onboardingTip');
    this._obText = $('#onboardingText');
    this._obBadge = $('#onboardingBadge');
    this._obSkip = $('#onboardingSkip');
    this._obRestart = $('#restartOnboarding');

    this._obSkip.addEventListener('click', () => this._endOnboarding());
    this._obRestart.addEventListener('click', (e) => {
      e.preventDefault();
      this._startOnboarding();
    });

    $('#openFAQBtn').addEventListener('click', (e) => {
      e.preventDefault();
      this._showFAQModal();
    });

    if (!localStorage.getItem(CONFIG.ONBOARDING_KEY)) {
      setTimeout(() => this._startOnboarding(), 500);
    }
  }

  _startOnboarding() {
    localStorage.removeItem(CONFIG.ONBOARDING_KEY);
    this._onboardingStep = 0;
    this._showOnboardingStep();
  }

  _endOnboarding() {
    this._onboardingStep = null;
    localStorage.setItem(CONFIG.ONBOARDING_KEY, '1');
    this._obTip.classList.add('hidden');
    document.querySelectorAll('.onboarding-pulse').forEach(el => el.classList.remove('onboarding-pulse'));
  }

  _advanceOnboarding(completedStep) {
    if (this._onboardingStep !== completedStep) return;
    this._onboardingStep = completedStep + 1;
    if (this._onboardingStep >= ONBOARDING_STEPS.length) {
      this._endOnboarding();
      return;
    }
    setTimeout(() => this._showOnboardingStep(), 300);
  }

  _showOnboardingStep() {
    if (this._onboardingStep === null) return;
    const step = ONBOARDING_STEPS[this._onboardingStep];
    if (!step) { this._endOnboarding(); return; }

    document.querySelectorAll('.onboarding-pulse').forEach(el => el.classList.remove('onboarding-pulse'));

    const targetSel = step.target || step.dynamicTarget;
    const targetEl = targetSel ? document.querySelector(targetSel) : null;

    if (!targetEl) {
      this._obTip.classList.add('hidden');
      return;
    }

    this._obBadge.textContent = this._onboardingStep + 1;
    this._obText.innerHTML = step.text;
    targetEl.classList.add('onboarding-pulse');
    this._obTip.classList.remove('hidden');
    const tipRect = this._obTip.getBoundingClientRect();
    const elRect = targetEl.getBoundingClientRect();

    let top, left;
    switch (step.position) {
      case 'bottom': top = elRect.bottom + 10; left = elRect.left + elRect.width / 2 - tipRect.width / 2; break;
      case 'top': top = elRect.top - tipRect.height - 10; left = elRect.left + elRect.width / 2 - tipRect.width / 2; break;
      case 'right': top = elRect.top + elRect.height / 2 - tipRect.height / 2; left = elRect.right + 10; break;
      case 'left': top = elRect.top + elRect.height / 2 - tipRect.height / 2; left = elRect.left - tipRect.width - 10; break;
    }

    left = Math.max(8, Math.min(left, window.innerWidth - tipRect.width - 8));
    top = Math.max(8, Math.min(top, window.innerHeight - tipRect.height - 8));

    this._obTip.style.top = top + 'px';
    this._obTip.style.left = left + 'px';
  }

  // ---- HISTORY ----

  _toggleHistory() {
    const drawer = this.els.historyDrawer;
    if (drawer.classList.contains('visible')) {
      this._closeHistory();
    } else {
      this._openHistory();
    }
  }

  async _openHistory() {
    if (!this.store.currentProjectName) {
      this._toast('Select a project first', 'warning');
      return;
    }
    const drawer = this.els.historyDrawer;
    drawer.classList.remove('hidden');
    requestAnimationFrame(() => drawer.classList.add('visible'));
    this.els.historyList.innerHTML = '<div class="history-diff-loading"><i class="bi bi-arrow-repeat spin"></i> Loading...</div>';
    try {
      const commits = await this.store.getHistory();
      this._renderHistoryList(commits);
    } catch (e) {
      this.els.historyList.innerHTML = `<div class="history-empty"><i class="bi bi-exclamation-triangle"></i><p>${this._esc(e.message)}</p></div>`;
    }
  }

  _closeHistory() {
    const drawer = this.els.historyDrawer;
    drawer.classList.remove('visible');
    setTimeout(() => drawer.classList.add('hidden'), 300);
  }

  _renderHistoryList(commits) {
    if (!commits || commits.length === 0) {
      this.els.historyList.innerHTML = `
        <div class="history-empty">
          <i class="bi bi-clock-history"></i>
          <p>No history yet.<br>Changes will appear here as you edit.</p>
        </div>`;
      return;
    }

    const html = `<div class="history-timeline">${commits.map(c => `
      <div class="history-commit" data-commit-id="${this._escAttr(c.id)}">
        <div class="history-source-dot history-source-dot--${c.source === 'mcp' ? 'mcp' : 'browser'}"></div>
        <div class="history-commit-header">
          <div class="history-commit-info">
            <div class="history-commit-summary">${this._esc(c.summary)}</div>
            <div class="history-commit-meta">
              <span class="history-commit-source history-commit-source--${c.source === 'mcp' ? 'mcp' : 'browser'}">
                <i class="bi bi-${c.source === 'mcp' ? 'robot' : 'person'}"></i> ${c.source === 'mcp' ? 'MCP' : 'Browser'}
              </span>
              <span>${this._formatTimeAgo(c.timestamp)}</span>
              <span class="history-commit-stats">${c.stats.categories} cat · ${c.stats.chunks} chunks</span>
            </div>
          </div>
          <i class="bi bi-chevron-down history-commit-expand"></i>
        </div>
        <div class="history-diff-panel" id="diff-${this._escAttr(c.id)}"></div>
      </div>
    `).join('')}</div>`;

    this.els.historyList.innerHTML = html;
  }

  async _handleHistoryClick(e) {
    // Expand/collapse commit
    const header = e.target.closest('.history-commit-header');
    if (header) {
      const commit = header.closest('.history-commit');
      const commitId = commit.dataset.commitId;
      if (commit.classList.contains('expanded')) {
        commit.classList.remove('expanded');
      } else {
        commit.classList.add('expanded');
        const panel = commit.querySelector('.history-diff-panel');
        if (!panel.dataset.loaded) {
          panel.innerHTML = '<div class="history-diff-loading"><i class="bi bi-arrow-repeat spin"></i> Loading diff...</div>';
          try {
            const detail = await this.store.getCommitDetail(commitId);
            const diffs = this._computeDiff(detail.prevSnapshot, detail.snapshot);
            this._renderDiffPanel(panel, diffs, commitId);
            panel.dataset.loaded = '1';
          } catch (err) {
            panel.innerHTML = `<div class="history-diff-loading">${this._esc(err.message)}</div>`;
          }
        }
      }
      return;
    }

    // Rollback button
    const rollbackBtn = e.target.closest('.history-rollback-btn');
    if (rollbackBtn) {
      const commitId = rollbackBtn.dataset.commitId;
      this._handleRollback(commitId);
    }
  }

  _renderDiffPanel(panel, diffs, commitId) {
    if (diffs.length === 0) {
      panel.innerHTML = `
        <div class="history-diff-list">
          <div class="history-diff history-diff--modified">
            <i class="bi bi-info-circle history-diff-icon"></i>
            <span class="history-diff-text">No visible changes</span>
          </div>
        </div>
        <button class="history-rollback-btn" data-commit-id="${this._escAttr(commitId)}">
          <i class="bi bi-arrow-counterclockwise"></i> Rollback to this point
        </button>`;
      return;
    }

    const icons = { added: 'bi-plus-circle-fill', deleted: 'bi-dash-circle-fill', modified: 'bi-pencil-fill' };

    const html = `
      <div class="history-diff-list">
        ${diffs.map(d => `
          <div class="history-diff history-diff--${d.type}">
            <i class="bi ${icons[d.type]} history-diff-icon"></i>
            <span class="history-diff-text">${this._esc(d.text)}</span>
          </div>
        `).join('')}
      </div>
      <button class="history-rollback-btn" data-commit-id="${this._escAttr(commitId)}">
        <i class="bi bi-arrow-counterclockwise"></i> Rollback to this point
      </button>`;

    panel.innerHTML = html;
  }

  async _handleRollback(commitId) {
    this.els.modalContent.innerHTML = `
      <div class="modal-title"><i class="bi bi-arrow-counterclockwise"></i> Rollback?</div>
      <p class="modal-text">
        This will restore the project to this commit's state. A new "rollback" commit will be created so you can undo this later.
      </p>
      <div class="modal-actions">
        <button class="btn btn-secondary" id="modalCancel">Cancel</button>
        <button class="btn btn-danger" id="confirmRollback"><i class="bi bi-arrow-counterclockwise"></i> Rollback</button>
      </div>
    `;
    this.els.modalOverlay.classList.remove('hidden');

    $('#confirmRollback').addEventListener('click', async () => {
      this._closeModal();
      try {
        await this.store.rollback(commitId);
        this._toast('Rolled back successfully', 'success');
        this._closeHistory();
      } catch (err) {
        this._toast('Rollback failed: ' + err.message, 'error');
      }
    });

    $('#modalCancel').addEventListener('click', () => this._closeModal());
  }

  _computeDiff(prev, curr) {
    const diffs = [];

    if (!prev) {
      // No previous snapshot — show current state as "added"
      for (const cat of curr.categories || []) {
        diffs.push({ type: 'added', text: `Category "${cat.name}" (${cat.chunks.length} chunks)` });
        for (const ch of cat.chunks) {
          diffs.push({ type: 'added', text: `Chunk "${ch.id}" in "${cat.name}"` });
        }
      }
      return diffs;
    }

    const prevCats = new Map((prev.categories || []).map(c => [c.id, c]));
    const currCats = new Map((curr.categories || []).map(c => [c.id, c]));

    // Deleted categories
    for (const [id, cat] of prevCats) {
      if (!currCats.has(id)) {
        diffs.push({ type: 'deleted', text: `Category "${cat.name}" removed (${cat.chunks.length} chunks)` });
      }
    }

    // Added categories
    for (const [id, cat] of currCats) {
      if (!prevCats.has(id)) {
        diffs.push({ type: 'added', text: `Category "${cat.name}" added (${cat.chunks.length} chunks)` });
      }
    }

    // Modified categories
    for (const [id, currCat] of currCats) {
      const prevCat = prevCats.get(id);
      if (!prevCat) continue;

      // Renamed
      if (prevCat.name !== currCat.name) {
        diffs.push({ type: 'modified', text: `Category renamed: "${prevCat.name}" → "${currCat.name}"` });
      }

      // Compare chunks
      const prevChunks = new Map(prevCat.chunks.map(ch => [ch._uid, ch]));
      const currChunks = new Map(currCat.chunks.map(ch => [ch._uid, ch]));

      for (const [uid, ch] of prevChunks) {
        if (!currChunks.has(uid)) {
          diffs.push({ type: 'deleted', text: `Chunk "${ch.id}" removed from "${currCat.name}"` });
        }
      }

      for (const [uid, ch] of currChunks) {
        if (!prevChunks.has(uid)) {
          diffs.push({ type: 'added', text: `Chunk "${ch.id}" added to "${currCat.name}"` });
        }
      }

      for (const [uid, currCh] of currChunks) {
        const prevCh = prevChunks.get(uid);
        if (!prevCh) continue;

        const changes = [];
        if (prevCh.id !== currCh.id) changes.push(`id: "${prevCh.id}" → "${currCh.id}"`);
        if (prevCh.text !== currCh.text) {
          const prevLen = (prevCh.text || '').length;
          const currLen = (currCh.text || '').length;
          changes.push(`text changed (${prevLen} → ${currLen} chars)`);
        }

        const metaKeys = new Set([
          ...Object.keys(prevCh.metadata || {}),
          ...Object.keys(currCh.metadata || {}),
        ]);
        for (const k of metaKeys) {
          const pv = (prevCh.metadata || {})[k];
          const cv = (currCh.metadata || {})[k];
          if (pv !== cv) changes.push(`${k}: "${pv || ''}" → "${cv || ''}"`);
        }

        // Compare custom fields
        const prevCf = (prevCh.customFields || []);
        const currCf = (currCh.customFields || []);
        const prevCfMap = new Map(prevCf.map(f => [f.key, f.value]));
        const currCfMap = new Map(currCf.map(f => [f.key, f.value]));
        const allCfKeys = new Set([...prevCfMap.keys(), ...currCfMap.keys()]);
        for (const k of allCfKeys) {
          if (!k) continue;
          const pv = prevCfMap.get(k);
          const cv = currCfMap.get(k);
          if (pv !== cv) {
            if (pv === undefined) changes.push(`${k}: added "${cv}"`);
            else if (cv === undefined) changes.push(`${k}: removed`);
            else changes.push(`${k}: "${pv}" → "${cv}"`);
          }
        }

        if (changes.length > 0) {
          diffs.push({ type: 'modified', text: `Chunk "${currCh.id}" in "${currCat.name}": ${changes.join(', ')}` });
        }
      }
    }

    // Check for chunks moved between categories
    // (chunk existed in prev cat A but now in curr cat B)
    const allPrevChunkUids = new Map();
    for (const cat of prev.categories || []) {
      for (const ch of cat.chunks) allPrevChunkUids.set(ch._uid, { cat: cat.name, ch });
    }
    const allCurrChunkUids = new Map();
    for (const cat of curr.categories || []) {
      for (const ch of cat.chunks) allCurrChunkUids.set(ch._uid, { cat: cat.name, ch });
    }

    for (const [uid, currInfo] of allCurrChunkUids) {
      const prevInfo = allPrevChunkUids.get(uid);
      if (prevInfo && prevInfo.cat !== currInfo.cat) {
        // Already handled as add+delete within categories, but let's add a move note
        // Remove the separate add/delete and replace with move
        const addIdx = diffs.findIndex(d => d.type === 'added' && d.text.includes(`"${currInfo.ch.id}"`) && d.text.includes(`"${currInfo.cat}"`));
        const delIdx = diffs.findIndex(d => d.type === 'deleted' && d.text.includes(`"${prevInfo.ch.id}"`) && d.text.includes(`"${prevInfo.cat}"`));
        if (addIdx !== -1) diffs.splice(addIdx, 1);
        if (delIdx !== -1) diffs.splice(delIdx > addIdx ? delIdx - 1 : delIdx, 1);
        diffs.push({ type: 'modified', text: `Chunk "${currInfo.ch.id}" moved: "${prevInfo.cat}" → "${currInfo.cat}"` });
      }
    }

    return diffs;
  }

  _formatTimeAgo(isoString) {
    const diff = Date.now() - new Date(isoString).getTime();
    const secs = Math.floor(diff / 1000);
    if (secs < 60) return 'just now';
    const mins = Math.floor(secs / 60);
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    const days = Math.floor(hrs / 24);
    if (days < 30) return `${days}d ago`;
    return new Date(isoString).toLocaleDateString();
  }

  // ---- ESCAPE HELPERS ----

  _esc(str) {
    const div = document.createElement('div');
    div.textContent = str || '';
    return div.innerHTML;
  }

  _escAttr(str) {
    return (str || '').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  // ---- BATCH WIKI IMPORT ----

  _showBatchWikiStep1() {
    this.els.modalContent.innerHTML = `
      <div class="modal-title"><i class="bi bi-globe2"></i> Batch Wiki Import</div>
      <p class="modal-text">
        Paste any page URL from a MediaWiki-based wiki — the app will detect the site and fetch all its categories.<br><br>
        <strong>Supported:</strong> wiki.gg, Fandom, Wikipedia, minecraft.wiki, and any MediaWiki site.
      </p>
      <input class="field-input" type="url" id="batchWikiUrl"
        placeholder="https://terraria.wiki.gg/wiki/Copper_Pickaxe" autofocus>
      <div id="batchDetectStatus" class="wiki-import-status hidden"></div>
      <div class="modal-actions">
        <button class="btn btn-secondary" id="modalCancel">Cancel</button>
        <button class="btn btn-accent" id="batchDetectBtn"><i class="bi bi-search"></i> Fetch Categories</button>
      </div>`;
    this.els.modalOverlay.classList.remove('hidden');

    const input = $('#batchWikiUrl');
    const statusEl = $('#batchDetectStatus');
    const detectBtn = $('#batchDetectBtn');
    const cancelBtn = $('#modalCancel');

    setTimeout(() => input.focus(), 100);

    const doDetect = async () => {
      const url = input.value.trim();
      if (!url) { input.classList.add('input-error'); return; }
      input.classList.remove('input-error');

      statusEl.className = 'wiki-import-status';
      statusEl.innerHTML = '<i class="bi bi-arrow-repeat spin"></i> Detecting wiki...';

      detectBtn.disabled = true;
      try {
        const result = await api('/wiki/batch/detect', { method: 'POST', body: { url } });
        this._showBatchWikiStep2(result.apiBase, result.wikiName);
      } catch (e) {
        statusEl.className = 'wiki-import-status wiki-import-error';
        statusEl.textContent = e.message || 'Could not detect a wiki at this URL';
        detectBtn.disabled = false;
      }
    };

    detectBtn.addEventListener('click', doDetect);
    cancelBtn.addEventListener('click', () => this._closeModal());
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') doDetect();
      if (e.key === 'Escape') this._closeModal();
    });
  }

  async _showBatchWikiStep2(apiBase, wikiName) {
    this.els.modalContent.innerHTML = `
      <div class="modal-title"><i class="bi bi-globe2"></i> ${this._esc(wikiName)} — Categories</div>
      <p class="modal-text"><i class="bi bi-arrow-repeat spin"></i> Loading categories...</p>`;
    this.els.modalOverlay.querySelector('.modal').classList.add('modal--batch');

    let allCategories;
    try {
      const result = await api('/wiki/batch/categories', { method: 'POST', body: { apiBase } });
      allCategories = result.categories;
    } catch (e) {
      this.els.modalContent.innerHTML = `
        <div class="modal-title"><i class="bi bi-globe2"></i> Error</div>
        <p class="modal-text wiki-import-error">${this._esc(e.message)}</p>
        <div class="modal-actions">
          <button class="btn btn-secondary" id="modalCancel">Close</button>
        </div>`;
      $('#modalCancel').addEventListener('click', () => this._closeModal());
      return;
    }

    const accepted = allCategories.filter(c => c.accepted);
    const rejected = allCategories.filter(c => !c.accepted);

    this.els.modalContent.innerHTML = `
      <div class="modal-title"><i class="bi bi-globe2"></i> ${this._esc(wikiName)} — Select Categories</div>
      <input class="batch-category-search" type="text" id="batchCatSearch" placeholder="Search categories...">
      <div class="batch-category-controls">
        <button class="btn btn-secondary" id="batchSelectAll">Select All</button>
        <button class="btn btn-secondary" id="batchDeselectAll">Deselect All</button>
        <span class="batch-category-count" id="batchCatCount">${accepted.length} of ${allCategories.length} selected</span>
      </div>
      <div class="batch-category-scroll" id="batchCatList"></div>
      <label style="display:block;font-size:13px;color:var(--text-secondary);margin-bottom:6px;">Project name</label>
      <input class="field-input" type="text" id="batchProjectName" value="${this._escAttr(wikiName)}_knowledge_base" style="margin-bottom:0;">
      <div class="modal-actions">
        <button class="btn btn-secondary" id="batchBackBtn"><i class="bi bi-arrow-left"></i> Back</button>
        <button class="btn btn-accent" id="batchStartBtn"><i class="bi bi-play-fill"></i> Start Import</button>
      </div>`;

    const listEl = $('#batchCatList');
    const renderList = (filter = '') => {
      const lower = filter.toLowerCase();
      let html = '';
      const sorted = [...accepted.sort((a, b) => b.size - a.size), ...rejected.sort((a, b) => b.size - a.size)];
      for (const cat of sorted) {
        if (lower && !cat.name.toLowerCase().includes(lower)) continue;
        const cls = cat.accepted ? '' : ' filtered';
        const checked = cat.accepted ? 'checked' : '';
        html += `<label class="batch-category-item${cls}">
          <input type="checkbox" value="${this._escAttr(cat.name)}" ${checked}>
          <span>${this._esc(cat.name)}</span>
          <span class="cat-size">${cat.size}</span>
        </label>`;
      }
      listEl.innerHTML = html || '<p style="color:var(--text-secondary);font-size:13px;">No categories found</p>';
      this._updateBatchCatCount();
    };

    renderList();

    const searchEl = $('#batchCatSearch');
    searchEl.addEventListener('input', () => renderList(searchEl.value));

    $('#batchSelectAll').addEventListener('click', () => {
      listEl.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = true);
      this._updateBatchCatCount();
    });
    $('#batchDeselectAll').addEventListener('click', () => {
      listEl.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = false);
      this._updateBatchCatCount();
    });
    listEl.addEventListener('change', () => this._updateBatchCatCount());

    $('#batchBackBtn').addEventListener('click', () => {
      this.els.modalOverlay.querySelector('.modal').classList.remove('modal--batch');
      this._showBatchWikiStep1();
    });

    $('#batchStartBtn').addEventListener('click', () => {
      const selected = [...listEl.querySelectorAll('input[type="checkbox"]:checked')].map(cb => cb.value);
      const projectName = $('#batchProjectName').value.trim() || `${wikiName}_knowledge_base`;
      if (!selected.length) { this._toast('Select at least one category', 'error'); return; }
      this._closeModal();
      this.els.modalOverlay.querySelector('.modal').classList.remove('modal--batch');
      this._startBatchImport(apiBase, wikiName, selected, projectName);
    });
  }

  _updateBatchCatCount() {
    const listEl = $('#batchCatList');
    const countEl = $('#batchCatCount');
    if (!listEl || !countEl) return;
    const total = listEl.querySelectorAll('input[type="checkbox"]').length;
    const checked = listEl.querySelectorAll('input[type="checkbox"]:checked').length;
    countEl.textContent = `${checked} of ${total} selected`;
  }

  async _startBatchImport(apiBase, wikiName, categories, projectName) {
    // Remove any existing panel
    const old = document.querySelector('.batch-panel');
    if (old) old.remove();

    // Create floating panel
    const panel = document.createElement('div');
    panel.className = 'batch-panel';
    panel.innerHTML = `
      <div class="batch-panel-header" id="batchPanelHeader">
        <i class="bi bi-globe2"></i>
        <div class="batch-panel-title">
          Importing: ${this._esc(wikiName)}
          <small>${categories.length} cat</small>
        </div>
        <button class="batch-panel-toggle" id="batchToggle" title="Collapse"><i class="bi bi-dash-lg"></i></button>
      </div>
      <div class="batch-panel-body">
        <div class="batch-stats-row">
          <div class="batch-stat">
            <div class="batch-stat-label">Categories</div>
            <div class="batch-stat-value" id="batchStatCat">0 / ${categories.length}</div>
            <div class="batch-progress-bar"><div class="batch-progress-fill" id="batchBarCat"></div></div>
          </div>
          <div class="batch-stat">
            <div class="batch-stat-label">Pages</div>
            <div class="batch-stat-value" id="batchStatPage">0 / ?</div>
            <div class="batch-progress-bar"><div class="batch-progress-fill" id="batchBarPage"></div></div>
          </div>
          <div class="batch-stat">
            <div class="batch-stat-label">Chunks</div>
            <div class="batch-stat-value" id="batchStatChunks">0</div>
          </div>
          <div class="batch-stat">
            <div class="batch-stat-label">Err / Skip</div>
            <div class="batch-stat-value" id="batchStatErrors">0 / 0</div>
          </div>
        </div>

        <div class="batch-current-op" id="batchCurrentOp">
          <i class="bi bi-arrow-repeat spin"></i> Preparing import...
        </div>

        <div class="batch-timing">
          <span id="batchElapsed">0:00</span>
          <span id="batchEta">ETA: calculating...</span>
        </div>

        <div class="batch-log" id="batchLog"></div>

        <div class="batch-actions" id="batchActions">
          <button class="btn btn-danger" id="batchCancelBtn">
            <i class="bi bi-x-circle"></i> Cancel
          </button>
        </div>
      </div>`;
    document.body.appendChild(panel);

    // Toggle collapse
    panel.querySelector('#batchToggle').addEventListener('click', (e) => {
      e.stopPropagation();
      panel.classList.toggle('batch-panel--collapsed');
      const icon = panel.querySelector('#batchToggle i');
      icon.className = panel.classList.contains('batch-panel--collapsed') ? 'bi bi-plus-lg' : 'bi bi-dash-lg';
    });
    panel.querySelector('#batchPanelHeader').addEventListener('click', () => {
      if (panel.classList.contains('batch-panel--collapsed')) {
        panel.classList.remove('batch-panel--collapsed');
        panel.querySelector('#batchToggle i').className = 'bi bi-dash-lg';
      }
    });

    // Timer
    const startTime = Date.now();
    this._batchTimer = setInterval(() => {
      const el = panel.querySelector('#batchElapsed');
      if (el) el.textContent = this._formatElapsed(Date.now() - startTime);
    }, 1000);

    // Cancel
    this._batchAbort = new AbortController();
    this._batchLastStats = {};
    this._batchProjectName = projectName;
    this._batchPanel = panel;
    panel.querySelector('#batchCancelBtn').addEventListener('click', () => {
      this._batchAbort.abort();
    });

    // SSE stream
    try {
      const response = await fetch('/api/wiki/batch/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          apiBase, wikiName,
          categories,
          session: this.store.sessionCode,
        }),
        signal: this._batchAbort.signal,
      });

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const events = buffer.split('\n\n');
        buffer = events.pop();

        for (const raw of events) {
          if (!raw.trim()) continue;
          const lines = raw.split('\n');
          let eventType = 'message', data = '';
          for (const line of lines) {
            if (line.startsWith('event: ')) eventType = line.slice(7);
            if (line.startsWith('data: ')) data = line.slice(6);
          }
          if (data) this._handleBatchEvent(eventType, JSON.parse(data));
        }
      }
    } catch (e) {
      if (e.name === 'AbortError') {
        this._batchShowComplete({ ...this._batchLastStats, project: this._batchProjectName }, true);
      } else {
        this._batchLog(`Connection error: ${e.message}`, 'error');
        clearInterval(this._batchTimer);
      }
      return;
    }

    clearInterval(this._batchTimer);
  }

  _handleBatchEvent(type, data) {
    const panel = this._batchPanel;
    if (!panel) return;

    if (type === 'progress' || type === 'category-done' || type === 'status') {
      this._batchLastStats = data;
    }
    if (type === 'progress') {
      const catEl = panel.querySelector('#batchStatCat');
      const pageEl = panel.querySelector('#batchStatPage');
      const chunkEl = panel.querySelector('#batchStatChunks');
      const errEl = panel.querySelector('#batchStatErrors');
      const catBar = panel.querySelector('#batchBarCat');
      const pageBar = panel.querySelector('#batchBarPage');
      const currentOp = panel.querySelector('#batchCurrentOp');
      const etaEl = panel.querySelector('#batchEta');

      if (catEl) catEl.textContent = `${data.categoriesDone} / ${data.categoriesTotal}`;
      if (pageEl) pageEl.textContent = `${data.pagesDone} / ${data.pagesTotal}`;
      if (chunkEl) chunkEl.textContent = data.chunksCreated;
      if (errEl) errEl.textContent = `${data.errors} / ${data.skipped}`;
      if (catBar && data.categoriesTotal) catBar.style.width = `${(data.categoriesDone / data.categoriesTotal) * 100}%`;
      if (pageBar && data.pagesTotal) pageBar.style.width = `${(data.pagesDone / data.pagesTotal) * 100}%`;
      if (currentOp && data.currentPage) {
        currentOp.innerHTML = `<i class="bi bi-arrow-repeat spin"></i> Parsing "${this._esc(data.currentPage)}" <span style="opacity:0.5">in ${this._esc(data.currentCategory)}</span>`;
      }
      if (etaEl && data.pagesDone > 0 && data.pagesTotal > 0) {
        const remaining = ((data.elapsed / data.pagesDone) * (data.pagesTotal - data.pagesDone)) / 1000;
        etaEl.textContent = `ETA: ~${this._formatElapsed(remaining * 1000)} remaining`;
      }
      if (data.log) this._batchLog(data.log, 'success');
    }

    if (type === 'log') {
      this._batchLog(data.log, data.error ? 'error' : 'dim');
    }

    if (type === 'status') {
      this._batchLog(data.log || data.phase, 'dim');
    }

    if (type === 'category-done') {
      const catEl = panel.querySelector('#batchStatCat');
      const catBar = panel.querySelector('#batchBarCat');
      if (catEl) catEl.textContent = `${data.categoriesDone} / ${data.categoriesTotal}`;
      if (catBar && data.categoriesTotal) catBar.style.width = `${(data.categoriesDone / data.categoriesTotal) * 100}%`;
      this._batchLog(`Category "${data.category}" done`, 'success');
    }

    if (type === 'started') {
      this._batchProjectName = data.project;
      this._batchLog(`Project "${data.project}" created`, 'success');
      // Load project in sidebar so it appears immediately
      this.store.refreshProjectList().then(() => this.store.selectProject(data.project));
    }

    if (type === 'complete') {
      this._batchShowComplete(data);
    }

    if (type === 'cancelled') {
      this._batchShowComplete(data, true);
    }

    if (type === 'error') {
      this._batchLog(`Fatal error: ${data.message}`, 'error');
      clearInterval(this._batchTimer);
    }
  }

  _batchLog(message, type = '') {
    const panel = this._batchPanel;
    if (!panel) return;
    const log = panel.querySelector('#batchLog');
    if (!log) return;
    const cls = type ? ` batch-log-entry--${type}` : '';
    const entry = document.createElement('div');
    entry.className = `batch-log-entry${cls}`;
    entry.textContent = message;
    log.appendChild(entry);
    log.scrollTop = log.scrollHeight;
  }

  _batchShowComplete(data, cancelled = false) {
    clearInterval(this._batchTimer);
    const panel = this._batchPanel;
    if (!panel) return;

    const projectName = data.project || this._batchProjectName;
    const elapsed = this._formatElapsed(data.elapsed || 0);

    panel.classList.add(cancelled ? 'batch-panel--cancelled' : 'batch-panel--done');
    // Expand if collapsed so user sees the result
    panel.classList.remove('batch-panel--collapsed');
    const toggleIcon = panel.querySelector('#batchToggle i');
    if (toggleIcon) toggleIcon.className = 'bi bi-dash-lg';

    const currentOp = panel.querySelector('#batchCurrentOp');
    const actions = panel.querySelector('#batchActions');
    const catBar = panel.querySelector('#batchBarCat');
    const pageBar = panel.querySelector('#batchBarPage');

    if (!cancelled) {
      if (catBar) catBar.style.width = '100%';
      if (pageBar) pageBar.style.width = '100%';
    }

    if (currentOp) {
      currentOp.innerHTML = cancelled
        ? '<i class="bi bi-pause-circle" style="color:var(--text-secondary)"></i> Import cancelled'
        : '<i class="bi bi-check-circle" style="color:#34d399"></i> Import complete!';
    }

    const summary = `${data.categoriesDone || 0} categories, ${data.pagesDone || 0} pages, ${data.chunksCreated || 0} chunks — ${elapsed}`;
    this._batchLog(cancelled ? `Cancelled. ${summary}` : `Done! ${summary}`, cancelled ? 'error' : 'success');

    if (actions) {
      actions.innerHTML = `
        <button class="btn btn-accent" id="batchOpenProject">
          <i class="bi bi-folder2-open"></i> Open Project
        </button>
        <button class="btn btn-secondary" id="batchClosePanel">
          <i class="bi bi-x-lg"></i> Close
        </button>`;
      panel.querySelector('#batchOpenProject').addEventListener('click', async () => {
        if (projectName) {
          await this.store.refreshProjectList();
          await this.store.selectProject(projectName);
        }
        panel.remove();
        this._batchPanel = null;
      });
      panel.querySelector('#batchClosePanel').addEventListener('click', () => {
        panel.remove();
        this._batchPanel = null;
      });
    }
  }

  _formatElapsed(ms) {
    const totalSec = Math.floor(ms / 1000);
    const min = Math.floor(totalSec / 60);
    const sec = totalSec % 60;
    return `${min}:${sec.toString().padStart(2, '0')}`;
  }
}

// =============================================
// BOOT
// =============================================
document.addEventListener('DOMContentLoaded', () => {
  window.app = new App();
});

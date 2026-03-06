# Dataset Builder by Tryll Engine

A web-based tool for building structured RAG knowledge base datasets. Create projects, organize data into categories and chunks, edit metadata, track version history with GitHub-style diffs, and export RAG-ready JSON — all from the browser.

Connects with the [MCP server](https://github.com/Skizziik/tryll_dataset_builder) for AI-powered dataset building via Claude Code.

**Live**: [trylljsoncreator.onrender.com](https://trylljsoncreator.onrender.com)

Built by [Tryll Engine](https://tryllengine.com) | [Discord](https://discord.gg/CMnMrmapyB)

---

## Features

- **Project Management** — Create, import, delete, and merge projects
- **Category System** — Organize chunks into categories (e.g., Mobs, Weapons, Biomes)
- **Chunk Editor** — Edit ID, text content, standard metadata (page_title, source, license) and unlimited custom fields
- **Version History** — Git-like commit timeline with colored diffs (green/red/yellow), source tracking (Browser/MCP), and rollback to any previous state
- **Real-Time Sync** — MCP server connects via WebSocket for live collaboration with Claude Code
- **Search** — Find chunks by ID or text content
- **Export** — One-click export as flat JSON array, ready for RAG pipelines
- **Import** — Import existing JSON datasets
- **Bulk Operations** — Update metadata across all chunks, merge projects
- **Dark Theme** — Designed for long editing sessions
- **Onboarding Guide** — Interactive 5-step tutorial for new users

---

## Quick Start

### Self-hosted

```bash
git clone https://github.com/Skizziik/json_creator.git
cd json_creator
npm install
npm start
```

Server starts on `http://localhost:3000`.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3000` | Server port |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Browser (SPA)                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐  │
│  │ Topbar   │  │ Sidebar  │  │ Editor   │  │  History    │  │
│  │ Projects │  │ Category │  │ Chunk    │  │  Drawer     │  │
│  │ Session  │  │ Tree     │  │ Fields   │  │  Timeline   │  │
│  │ Export   │  │ Search   │  │ Metadata │  │  Diffs      │  │
│  └──────────┘  └──────────┘  └──────────┘  └────────────┘  │
└───────────────────────┬─────────────────────────────────────┘
                        │ REST API + WebSocket
┌───────────────────────┴─────────────────────────────────────┐
│                    Express Server (Node.js)                   │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────┐  │
│  │  REST API        │  │  WebSocket       │  │  Session   │  │
│  │  30+ endpoints   │  │  Real-time sync  │  │  Manager   │  │
│  └────────┬─────────┘  └──────────────────┘  └───────────┘  │
│           │                                                   │
│  ┌────────┴─────────────────────────────────────────────┐    │
│  │  Store (lib/store.js)                                 │    │
│  │  Project CRUD · Category CRUD · Chunk CRUD            │    │
│  │  Search · Import/Export · Merge · Bulk Update          │    │
│  │  History Engine (50 commits, snapshots, rollback)      │    │
│  └────────┬─────────────────────────────────────────────┘    │
└───────────┼──────────────────────────────────────────────────┘
            │
   ┌────────┴────────┐
   │   data/          │
   │   project.json   │  ← project data
   │   project        │
   │   .history.json  │  ← version history (snapshots)
   └─────────────────┘
```

---

## REST API

All endpoints return JSON. Mutation endpoints accept optional `source` (`"browser"` | `"mcp"`) and `session` (6-char code for WebSocket broadcast) parameters.

### Health & Session

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Server status |
| GET | `/api/session` | Generate a 6-character session code for MCP connection |

### Projects

| Method | Endpoint | Body / Query | Description |
|--------|----------|--------------|-------------|
| GET | `/api/projects` | — | List all projects |
| GET | `/api/projects/:name` | — | Get full project (categories + chunks) |
| POST | `/api/projects` | `{ name }` | Create project |
| DELETE | `/api/projects/:name` | — | Delete project + history |
| GET | `/api/projects/:name/stats` | — | Detailed statistics |

### Categories

| Method | Endpoint | Body / Query | Description |
|--------|----------|--------------|-------------|
| GET | `/api/projects/:name/categories` | — | List categories |
| POST | `/api/projects/:name/categories` | `{ name }` | Create category |
| PUT | `/api/projects/:name/categories/:catName` | `{ newName }` | Rename category |
| DELETE | `/api/projects/:name/categories/:catName` | — | Delete category + chunks |
| POST | `/api/projects/:name/categories/:catId/toggle` | — | Toggle expand/collapse |

### Chunks

| Method | Endpoint | Body / Query | Description |
|--------|----------|--------------|-------------|
| POST | `/api/projects/:name/categories/:cat/chunks` | `{ id, text, metadata }` | Add chunk |
| POST | `/api/projects/:name/categories/:cat/chunks/bulk` | `{ chunks: [...] }` | Bulk add |
| POST | `/api/projects/:name/categories/:catId/chunks/blank` | — | Add empty chunk |
| PUT | `/api/projects/:name/categories/:catId/chunks/:uid` | `{ id?, text?, metadata? }` | Update chunk |
| DELETE | `/api/projects/:name/categories/:catId/chunks/:uid` | — | Delete chunk |
| POST | `/api/projects/:name/categories/:catId/chunks/:uid/duplicate` | — | Clone chunk |
| POST | `/api/projects/:name/chunks/:chunkId/move` | `{ targetCategory }` | Move chunk |

### Search & Export

| Method | Endpoint | Params | Description |
|--------|----------|--------|-------------|
| GET | `/api/projects/:name/search` | `?q=query` | Search chunks |
| GET | `/api/projects/:name/export` | — | Export as flat JSON array |
| POST | `/api/projects/:name/import` | `{ data, category? }` | Import JSON array |
| GET | `/api/projects/:name/categories/:cat/export` | — | Export single category |

### Bulk Operations

| Method | Endpoint | Body | Description |
|--------|----------|------|-------------|
| POST | `/api/projects/:name/bulk-metadata` | `{ field, value, category? }` | Update metadata field across chunks |
| POST | `/api/projects/:name/merge` | `{ target }` | Merge source into target project |

### Version History

| Method | Endpoint | Body | Description |
|--------|----------|------|-------------|
| GET | `/api/projects/:name/history` | — | Get commit timeline (last 50, no snapshots) |
| GET | `/api/projects/:name/history/:commitId` | — | Get commit + snapshot + previous snapshot |
| POST | `/api/projects/:name/history/:commitId/rollback` | `{ source? }` | Rollback to commit state |

---

## Version History System

Every data mutation creates a "commit" — a full snapshot of the project state. The history system tracks:

- **Who**: `source` field — `"browser"` (blue dot) or `"mcp"` (green dot)
- **What**: `action` + `summary` — human-readable description
- **When**: ISO 8601 timestamp
- **State**: full project snapshot for rollback

### Tracked Actions

| Action | Summary Example |
|--------|----------------|
| `createProject` | Created project 'minecraft' |
| `createCategory` | Created category 'Mobs' |
| `renameCategory` | Renamed category 'Mobs' → 'Enemies' |
| `deleteCategory` | Deleted category 'Mobs' (15 chunks) |
| `addChunk` | Added chunk 'creeper' to 'Mobs' |
| `addBlankChunk` | Added blank chunk to 'Mobs' |
| `bulkAddChunks` | Added 10 chunks to 'Mobs' |
| `updateChunk` | Updated chunk 'creeper' |
| `deleteChunk` | Deleted chunk 'creeper' |
| `duplicateChunk` | Duplicated 'creeper' as 'creeper_copy' |
| `moveChunk` | Moved 'creeper' from 'Mobs' to 'Enemies' |
| `importJSON` | Imported 25 chunks into 'Imported' |
| `bulkUpdateMetadata` | Bulk updated 'license' (30 chunks) |
| `mergeProjects` | Merged 'test' into 'production' |
| `rollback` | Rolled back to commit from 2026-02-27T14:30:00Z |

### Diff Display

When expanding a commit in the history drawer, the UI computes a client-side diff:

- **Green** — Added categories/chunks
- **Red** — Deleted categories/chunks
- **Yellow** — Modified chunks (field-level changes), renamed categories, moved chunks

### Rollback

- Click "Rollback to this point" on any commit
- Confirmation modal explains what will happen
- Project data is restored from the commit's snapshot
- A new "rollback" commit is created (so you can undo the rollback)
- Max 50 commits per project (FIFO — oldest are dropped)

### History File Format

Stored as `data/<project>.history.json`:

```json
{
  "project": "minecraft",
  "commits": [
    {
      "id": "uuid",
      "timestamp": "2026-02-27T14:30:00.000Z",
      "source": "mcp",
      "action": "addChunk",
      "summary": "Added chunk 'creeper' to 'Mobs'",
      "stats": { "categories": 3, "chunks": 12 },
      "snapshot": { "...full project JSON..." }
    }
  ]
}
```

---

## WebSocket Events

The server manages sessions for real-time sync between browser clients and MCP clients.

### Connection

```
Browser:  ws://host/ws?session=ABC123&type=browser
MCP:      ws://host/ws?session=ABC123&type=mcp
```

### Events

| Event | Direction | Description |
|-------|-----------|-------------|
| `connected` | Server → Client | Connection established |
| `error` | Server → Client | Invalid session code |
| `data:changed` | Server → Browsers | Data updated (from MCP or another browser) |
| `mcp:connected` | Server → Browsers | MCP client connected |
| `mcp:disconnected` | Server → Browsers | MCP client disconnected |

---

## Data Formats

### Project JSON

```json
{
  "name": "minecraft",
  "createdAt": "2026-02-27T10:00:00.000Z",
  "categories": [
    {
      "id": "uuid",
      "name": "Mobs",
      "expanded": true,
      "chunks": [
        {
          "_uid": "uuid",
          "id": "creeper",
          "text": "A Creeper is a hostile mob...",
          "metadata": {
            "page_title": "Creeper",
            "source": "Minecraft Wiki",
            "license": "CC BY-NC-SA 3.0"
          },
          "customFields": [
            { "key": "health", "value": "20" },
            { "key": "behavior", "value": "explodes" }
          ]
        }
      ]
    }
  ]
}
```

### Export Format (RAG-ready)

```json
[
  {
    "id": "creeper",
    "text": "A Creeper is a hostile mob that silently approaches...",
    "metadata": {
      "page_title": "Creeper",
      "source": "Minecraft Wiki",
      "license": "CC BY-NC-SA 3.0",
      "health": "20",
      "behavior": "explodes"
    }
  }
]
```

---

## MCP Integration

Install the companion MCP server to build datasets from Claude Code:

```bash
npm install -g tryll-dataset-builder-mcp
claude mcp add dataset-builder -- npx tryll-dataset-builder-mcp
```

Then tell Claude: *"Connect to session ABC123"* (code from browser topbar).

See [MCP server docs](https://github.com/Skizziik/tryll_dataset_builder) for the full list of 27 tools.

---

## Tech Stack

- **Backend**: Node.js, Express, WebSocket (`ws`)
- **Frontend**: Vanilla JavaScript (SPA), CSS custom properties
- **Storage**: JSON files on disk (no database)
- **Hosting**: Render (or any Node.js host)
- **Font**: Inter (Google Fonts)
- **Icons**: Bootstrap Icons

---

## Links

- **Live App**: [trylljsoncreator.onrender.com](https://trylljsoncreator.onrender.com)
- **MCP Server**: [github.com/Skizziik/tryll_dataset_builder](https://github.com/Skizziik/tryll_dataset_builder)
- **npm**: [tryll-dataset-builder-mcp](https://www.npmjs.com/package/tryll-dataset-builder-mcp)
- **Tryll Engine**: [tryllengine.com](https://tryllengine.com)
- **Discord**: [discord.gg/CMnMrmapyB](https://discord.gg/CMnMrmapyB)

## License

MIT

---

## Docker Quick Start

```bash
docker compose up --build
```

Then open `http://localhost:3000`.

Persistent data is stored in `./data`, including:

- project JSON files
- version history
- local wiki caches (`data/wiki-cache/<wiki-id>`)
- downloaded dumps and local page snapshots

### Local Wiki Build Flow

1. Open the app in the browser.
2. Click `Batch Wiki`.
3. Paste a page URL from the target wiki.
4. The app will detect the wiki and build a local cache.
5. Select categories from the local cache.
6. Start dataset build.

For Wikimedia wikis, the app prefers XML dumps.
For other MediaWiki-style wikis, it falls back to a full local snapshot via the MediaWiki API.

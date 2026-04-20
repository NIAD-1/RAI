# Research Bot

WhatsApp-ready research agent that can sit behind the existing `pocket-watcher/bridge/index.js` bridge.

## What it does

- Accepts any topic over WhatsApp through `POST /bridge/incoming`
- Builds a fast research brief from:
  - live web search via Tavily
  - YouTube results + transcript/description extraction
  - public Reddit discussions
  - Open Library catalog matches
  - Gutendex public-domain books
- Supports `report: <topic>` for a full PDF report with references

## Important note on books

The bot is wired for legal/public-domain or catalog-accessible book sources only.
It does not intentionally fetch pirated or copyrighted book downloads.

## Run

```bash
cd research-bot
cp .env.example .env
cargo run
```

## Use with the existing WhatsApp bridge

Point the bridge backend at this server:

```bash
cd pocket-watcher/bridge
BACKEND_URL=http://localhost:3000 npm start
```

## WhatsApp examples

- `Explain the future of lithium batteries`
- `Best books and videos to understand the Israel-Iran conflict`
- `Give me current research on warehouse automation`
- `report: impact of AI on healthcare supply chains`


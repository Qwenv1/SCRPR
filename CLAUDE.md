# SCRPR

Scraping dashboard hosted on Netlify with a Python backend for PDF parsing.

## Architecture

- **Netlify Function** (`netlify/functions/scrape.mts`): Main API with 4 actions — `scrape`, `crawl`, `extract`, `structure`. Uses cheerio for HTML parsing, Anthropic SDK for PDF text extraction and AI structuring.
- **Dashboard SPA** (`public/index.html`): Single-file dark-themed frontend with Extract URL and Pensions tabs. No build step.
- **Python Backend** (`scrpr-backend/`): FastAPI + Scrapling + PyMuPDF for stealth fetching and PDF table extraction. Deployed separately (Docker/Procfile).

### Backend Fallback

When a direct fetch returns 403 (blocked by anti-bot), the Netlify function automatically retries via the Python backend if `SCRPR_BACKEND_URL` is configured. The Python backend uses Scrapling's `StealthyFetcher` (headless browser with anti-detection) and PyMuPDF for native PDF table extraction — no AI needed for PDF parsing.

## Commands

```bash
# Type-check the Netlify function
npx tsc --noEmit

# Install JS dependencies
npm install

# Run Python backend locally
cd scrpr-backend && pip install -r requirements.txt && uvicorn main:app --reload
```

## Key Files

| File | Purpose |
|------|---------|
| `netlify/functions/scrape.mts` | Netlify serverless API (scrape/crawl/extract/structure) |
| `public/index.html` | Dashboard SPA (no framework, vanilla JS) |
| `scrpr-backend/main.py` | Python FastAPI backend with Scrapling + PyMuPDF |
| `netlify.toml` | Netlify build/deploy config, function timeout, security headers |
| `tsconfig.json` | TypeScript config targeting ES2022/NodeNext |
| `.env.example` | Required env vars: `HEALTH_API_TOKEN`, `ANTHROPIC_API_KEY`, optional `SCRPR_BACKEND_URL` |

## Auth

All API endpoints require `X-Admin-Token` header (or `Authorization: Bearer <token>`). Token is checked against `HEALTH_API_TOKEN` or `ADMIN_API_TOKEN` env var (Netlify) or `ADMIN_TOKEN` (Python backend).

## SSRF Protection

Both backends validate URLs to block private/local addresses: localhost, 127.0.0.1, 10.x, 172.16-31.x, 192.168.x, 169.254.x, ::1, .local domains.

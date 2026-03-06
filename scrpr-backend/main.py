"""
SCRPR Backend — FastAPI + Scrapling

POST /api/scrape?action=scrape   — Scrape a single URL (HTML or PDF)
POST /api/scrape?action=crawl    — Crawl from a URL, following links up to a depth limit
POST /api/scrape?action=extract  — Extract structured data using CSS selectors

All actions require admin token via X-Admin-Token header.
"""

import os
import re
import hmac
import time
import tempfile
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests as http_requests
import pymupdf
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

load_dotenv()

# ── Scrapling imports ──
from scrapling import Fetcher, StealthyFetcher

# ── App ──

app = FastAPI(title="SCRPR", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["Content-Type", "X-Admin-Token", "Authorization"],
)

# ── Rate limiter (in-memory) ──

_rate_map: dict[str, dict] = {}


def check_rate(ip: str, limit: int = 30, window_ms: int = 60_000) -> tuple[bool, int]:
    now = time.time() * 1000
    entry = _rate_map.get(ip)
    if not entry or now > entry["reset_at"]:
        _rate_map[ip] = {"count": 1, "reset_at": now + window_ms}
        return True, limit - 1
    entry["count"] += 1
    if entry["count"] > limit:
        return False, 0
    return True, limit - entry["count"]


# ── Auth ──


def authenticate(request: Request) -> None:
    """Validate admin token. Raises HTTPException on failure."""
    admin_token = os.environ.get("ADMIN_TOKEN", "")
    if not admin_token:
        raise HTTPException(503, detail="ADMIN_TOKEN not configured")

    # Block token in URL params
    if request.query_params.get("token") or request.query_params.get("api_key"):
        raise HTTPException(400, detail="Authentication via URL parameters is not permitted. Use X-Admin-Token header.")

    auth = request.headers.get("X-Admin-Token") or ""
    if not auth:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.lower().startswith("bearer "):
            auth = auth_header[7:]

    if not hmac.compare_digest(auth, admin_token):
        raise HTTPException(401, detail="Unauthorized")


# ── URL validation (SSRF prevention) ──

_BLOCKED_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0", "[::1]", "[::]"}
_BLOCKED_PREFIXES = ("192.168.", "10.", "169.254.",
                     "172.16.", "172.17.", "172.18.", "172.19.",
                     "172.20.", "172.21.", "172.22.", "172.23.", "172.24.", "172.25.",
                     "172.26.", "172.27.", "172.28.", "172.29.", "172.30.", "172.31.")


def validate_url(raw: str) -> str:
    """Return validated URL or raise HTTPException."""
    try:
        parsed = urlparse(raw)
    except Exception:
        raise HTTPException(400, detail="Invalid URL")

    if parsed.scheme not in ("http", "https"):
        raise HTTPException(400, detail="Only http/https URLs allowed")

    host = (parsed.hostname or "").lower()
    if host in _BLOCKED_HOSTS or host.endswith(".local") or any(host.startswith(p) for p in _BLOCKED_PREFIXES):
        raise HTTPException(400, detail="Blocked URL (private/local address)")

    return raw


# ── Request models ──


class ScrapeRequest(BaseModel):
    url: str
    format: str = "markdown"
    include_metadata: bool = False
    headers: Optional[dict[str, str]] = None
    stealth: bool = True


class CrawlRequest(BaseModel):
    url: str
    max_depth: int = Field(default=2, le=3)
    max_pages: int = Field(default=10, le=20)
    include_pattern: Optional[str] = None
    exclude_pattern: Optional[str] = None
    format: str = "markdown"


class ExtractRequest(BaseModel):
    url: str
    selectors: dict[str, dict]
    headers: Optional[dict[str, str]] = None


# ── PDF parsing ──

MAX_PDF_SIZE = 50 * 1024 * 1024  # 50 MB


def is_pdf_url(url: str) -> bool:
    """Check if URL likely points to a PDF."""
    return urlparse(url).path.lower().endswith(".pdf")


def fetch_and_parse_pdf(url: str, headers: dict[str, str] | None = None) -> dict:
    """Download a PDF and extract text/tables as markdown."""
    req_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    if headers:
        req_headers.update(headers)

    resp = http_requests.get(url, headers=req_headers, timeout=60, stream=True)
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")
    content_length = int(resp.headers.get("Content-Length", 0))
    if content_length > MAX_PDF_SIZE:
        raise ValueError(f"PDF too large: {content_length} bytes (max {MAX_PDF_SIZE})")

    pdf_bytes = resp.content

    # Verify it's actually a PDF
    if not pdf_bytes[:5] == b"%PDF-":
        # Not a PDF — content-type lied or redirect happened
        if "application/pdf" not in content_type and not is_pdf_url(url):
            raise ValueError("URL did not return a PDF")

    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")

    pages: list[dict] = []
    all_text_parts: list[str] = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text("text")

        # Extract tables if available
        tables_md = ""
        try:
            tabs = page.find_tables()
            if tabs and tabs.tables:
                for table in tabs.tables:
                    df = table.to_pandas()
                    if not df.empty:
                        tables_md += "\n" + df.to_markdown(index=False) + "\n"
        except Exception:
            pass  # table extraction is best-effort

        page_content = text.strip()
        if tables_md:
            page_content += "\n\n**Tables:**\n" + tables_md

        pages.append({
            "page": page_num + 1,
            "text": text.strip(),
            "tables_markdown": tables_md.strip(),
        })
        if page_content:
            all_text_parts.append(f"--- Page {page_num + 1} ---\n\n{page_content}")

    metadata = doc.metadata or {}
    doc.close()

    combined = "\n\n".join(all_text_parts)

    return {
        "page_count": len(pages),
        "metadata": {
            "title": metadata.get("title", ""),
            "author": metadata.get("author", ""),
            "subject": metadata.get("subject", ""),
            "creator": metadata.get("creator", ""),
            "producer": metadata.get("producer", ""),
            "creation_date": metadata.get("creationDate", ""),
        },
        "content": combined,
        "pages": pages,
    }


# ── Scrapling fetcher helpers ──


def get_fetcher(stealth: bool = True):
    """Return appropriate Scrapling fetcher."""
    if stealth:
        return StealthyFetcher(auto_match=True)
    return Fetcher(auto_match=True)


def fetch_page(url: str, stealth: bool = True, headers: dict[str, str] | None = None):
    """Fetch a page using Scrapling and return the response object."""
    fetcher = get_fetcher(stealth)
    kwargs = {}
    if headers:
        kwargs["headers"] = headers
    return fetcher.get(url, **kwargs)


# ── Content conversion helpers ──


def page_to_markdown(response) -> str:
    """Convert a Scrapling response to markdown."""
    # Remove non-content elements
    for tag in response.css("script, style, nav, footer, header, aside, iframe, noscript, svg"):
        tag.remove()

    lines: list[str] = []

    # Try to find main content area
    main = response.css("main, article, [role=main]")
    root = main[0] if main else response

    for el in root.css("h1, h2, h3, h4, h5, h6, p, li, pre, code, blockquote, a, img, table, hr, br"):
        tag = el.tag
        text = el.text.strip() if el.text else ""

        if tag == "br":
            lines.append("")
        elif tag == "hr":
            lines.append("\n---\n")
        elif tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            level = int(tag[1])
            if text:
                lines.append(f"\n{'#' * level} {text}\n")
        elif tag == "p":
            if text:
                lines.append(f"\n{text}\n")
        elif tag == "a":
            href = el.attrib.get("href", "")
            if text and href:
                lines.append(f"[{text}]({href})")
            elif text:
                lines.append(text)
        elif tag == "img":
            alt = el.attrib.get("alt", "")
            src = el.attrib.get("src", "")
            if src:
                lines.append(f"![{alt}]({src})")
        elif tag == "li":
            if text:
                lines.append(f"- {text}")
        elif tag in ("pre", "code"):
            if text:
                lines.append(f"\n```\n{text}\n```\n")
        elif tag == "blockquote":
            if text:
                lines.append(f"\n> {text}\n")
        elif tag == "table":
            rows = []
            for tr in el.css("tr"):
                cells = [td.text.strip() for td in tr.css("td, th") if td.text]
                if cells:
                    rows.append(cells)
            if rows:
                lines.append("\n| " + " | ".join(rows[0]) + " |")
                lines.append("| " + " | ".join("---" for _ in rows[0]) + " |")
                for row in rows[1:]:
                    lines.append("| " + " | ".join(row) + " |")
                lines.append("")

    result = "\n".join(lines)
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result.strip()


def page_to_text(response) -> str:
    """Convert a Scrapling response to plain text."""
    for tag in response.css("script, style, noscript"):
        tag.remove()
    text = response.text or ""
    return re.sub(r"\s+", " ", text).strip()


def page_to_html(response) -> str:
    """Return cleaned HTML from main content area."""
    for tag in response.css("script, style, noscript, svg, iframe"):
        tag.remove()
    main = response.css("main, article, [role=main]")
    root = main[0] if main else response
    return root.html or ""


def format_content(response, fmt: str) -> str:
    """Format page content based on requested format."""
    if fmt == "markdown":
        return page_to_markdown(response)
    elif fmt == "html":
        return page_to_html(response)
    elif fmt == "text":
        return page_to_text(response)
    elif fmt == "raw":
        return response.html or ""
    return page_to_markdown(response)


def extract_metadata(response, url: str) -> dict[str, str]:
    """Extract page metadata."""
    meta: dict[str, str] = {"url": url}

    title_el = response.css("title")
    if title_el:
        meta["title"] = title_el[0].text.strip() if title_el[0].text else ""

    for el in response.css("meta[name], meta[property]"):
        name = el.attrib.get("name") or el.attrib.get("property", "")
        content = el.attrib.get("content", "")
        if name and content:
            meta[name.replace("og:", "og_")] = content

    canonical = response.css('link[rel="canonical"]')
    if canonical:
        href = canonical[0].attrib.get("href", "")
        if href:
            meta["canonical"] = href

    html_el = response.css("html")
    if html_el:
        lang = html_el[0].attrib.get("lang", "")
        if lang:
            meta["language"] = lang

    return meta


def extract_links(response, base_url: str) -> list[str]:
    """Extract all links from the page."""
    links = set()
    for el in response.css("a[href]"):
        href = el.attrib.get("href", "")
        if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:"):
            continue
        try:
            absolute = urljoin(base_url, href)
            links.add(absolute)
        except Exception:
            pass
    return list(links)


# ── Handlers ──


async def handle_scrape(body: ScrapeRequest) -> dict:
    url = validate_url(body.url)
    start = time.time()

    # PDF detection — bypass Scrapling, use PyMuPDF
    if is_pdf_url(url):
        try:
            pdf_result = fetch_and_parse_pdf(url, headers=body.headers)
            return {
                "success": True,
                "data": {
                    "url": url,
                    "status": 200,
                    "title": pdf_result["metadata"].get("title", "") or os.path.basename(urlparse(url).path),
                    "content": pdf_result["content"],
                    "word_count": len(pdf_result["content"].split()),
                    "page_count": pdf_result["page_count"],
                    "format": "pdf",
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                    "elapsed_ms": int((time.time() - start) * 1000),
                    "metadata": pdf_result["metadata"],
                    "pages": pdf_result["pages"],
                },
            }
        except Exception as e:
            raise HTTPException(422, detail=f"PDF parsing failed: {str(e)}")

    # Standard HTML scrape
    response = fetch_page(url, stealth=body.stealth, headers=body.headers)

    # Check if response is actually a PDF (content-type detection)
    content_type = ""
    try:
        content_type = response.headers.get("content-type", "") if hasattr(response, "headers") else ""
    except Exception:
        pass

    if "application/pdf" in content_type:
        try:
            pdf_result = fetch_and_parse_pdf(url, headers=body.headers)
            return {
                "success": True,
                "data": {
                    "url": url,
                    "status": 200,
                    "title": pdf_result["metadata"].get("title", "") or os.path.basename(urlparse(url).path),
                    "content": pdf_result["content"],
                    "word_count": len(pdf_result["content"].split()),
                    "page_count": pdf_result["page_count"],
                    "format": "pdf",
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                    "elapsed_ms": int((time.time() - start) * 1000),
                    "metadata": pdf_result["metadata"],
                    "pages": pdf_result["pages"],
                },
            }
        except Exception as e:
            raise HTTPException(422, detail=f"PDF parsing failed: {str(e)}")

    title_el = response.css("title")
    title = title_el[0].text.strip() if title_el and title_el[0].text else ""
    content = format_content(response, body.format)

    result = {
        "url": url,
        "status": response.status,
        "title": title,
        "content": content,
        "word_count": len(content.split()),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_ms": int((time.time() - start) * 1000),
    }

    if body.include_metadata:
        result["metadata"] = extract_metadata(response, url)

    return {"success": True, "data": result}


async def handle_crawl(body: CrawlRequest) -> dict:
    start_url = validate_url(body.url)
    base_origin = urlparse(start_url).scheme + "://" + urlparse(start_url).netloc

    visited: set[str] = set()
    results: list[dict] = []
    queue: list[tuple[str, int]] = [(start_url, 0)]

    include_re = re.compile(body.include_pattern) if body.include_pattern else None
    exclude_re = re.compile(body.exclude_pattern) if body.exclude_pattern else None

    start = time.time()

    while queue and len(results) < body.max_pages:
        url, depth = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        try:
            page_start = time.time()

            # Handle PDF links found during crawl
            if is_pdf_url(url):
                try:
                    pdf_result = fetch_and_parse_pdf(url)
                    results.append({
                        "url": url,
                        "status": 200,
                        "title": pdf_result["metadata"].get("title", "") or os.path.basename(urlparse(url).path),
                        "content": pdf_result["content"],
                        "format": "pdf",
                        "page_count": pdf_result["page_count"],
                        "links": [],
                        "word_count": len(pdf_result["content"].split()),
                        "scraped_at": datetime.now(timezone.utc).isoformat(),
                        "elapsed_ms": int((time.time() - page_start) * 1000),
                    })
                except Exception as e:
                    results.append({
                        "url": url,
                        "status": 0,
                        "title": "",
                        "content": f"PDF parse error: {str(e)}",
                        "scraped_at": datetime.now(timezone.utc).isoformat(),
                        "elapsed_ms": int((time.time() - page_start) * 1000),
                    })
                continue

            response = fetch_page(url, stealth=True)

            title_el = response.css("title")
            title = title_el[0].text.strip() if title_el and title_el[0].text else ""
            content = format_content(response, body.format)
            links = extract_links(response, url)

            results.append({
                "url": url,
                "status": response.status,
                "title": title,
                "content": content,
                "links": links,
                "word_count": len(content.split()),
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "elapsed_ms": int((time.time() - page_start) * 1000),
            })

            if depth < body.max_depth:
                for link in links:
                    if link in visited:
                        continue
                    parsed_link = urlparse(link)
                    link_origin = parsed_link.scheme + "://" + parsed_link.netloc
                    if link_origin != base_origin:
                        continue
                    if include_re and not include_re.search(link):
                        continue
                    if exclude_re and exclude_re.search(link):
                        continue
                    queue.append((link, depth + 1))

        except Exception as e:
            results.append({
                "url": url,
                "status": 0,
                "title": "",
                "content": f"Error: {str(e)}",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "elapsed_ms": 0,
            })

    return {
        "success": True,
        "data": {
            "pages_scraped": len(results),
            "pages_discovered": len(visited),
            "total_elapsed_ms": int((time.time() - start) * 1000),
            "results": results,
        },
    }


async def handle_extract(body: ExtractRequest) -> dict:
    url = validate_url(body.url)
    start = time.time()

    response = fetch_page(url, stealth=True, headers=body.headers)

    extracted: dict[str, str | list[str]] = {}

    for key, spec in body.selectors.items():
        selector = spec.get("selector", "")
        attribute = spec.get("attribute")
        multiple = spec.get("multiple", False)

        elements = response.css(selector)

        if multiple:
            values = []
            for el in elements:
                if attribute:
                    val = el.attrib.get(attribute, "")
                else:
                    val = el.text.strip() if el.text else ""
                if val:
                    values.append(val)
            extracted[key] = values
        else:
            el = elements[0] if elements else None
            if el:
                if attribute:
                    extracted[key] = el.attrib.get(attribute, "")
                else:
                    extracted[key] = el.text.strip() if el.text else ""
            else:
                extracted[key] = "" if not multiple else []

    return {
        "success": True,
        "data": {
            "url": url,
            "status": response.status,
            "extracted": extracted,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_ms": int((time.time() - start) * 1000),
        },
    }


# ── Routes ──


@app.post("/api/scrape")
async def scrape_endpoint(request: Request, action: str = "scrape"):
    # Rate limit
    ip = (request.headers.get("x-forwarded-for") or "unknown").split(",")[0].strip()
    ok, remaining = check_rate(ip)
    if not ok:
        raise HTTPException(429, detail="Rate limit exceeded")

    # Auth
    authenticate(request)

    body = await request.json()

    if action == "scrape":
        return await handle_scrape(ScrapeRequest(**body))
    elif action == "crawl":
        return await handle_crawl(CrawlRequest(**body))
    elif action == "extract":
        return await handle_extract(ExtractRequest(**body))
    else:
        raise HTTPException(400, detail="Unknown action. Use: scrape, crawl, extract")


@app.get("/health")
async def health():
    return {"status": "ok", "engine": "scrapling+pymupdf"}

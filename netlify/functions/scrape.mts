/**
 * Scraping Dashboard API
 *
 * POST /api/scrape?action=scrape   — Scrape a single URL, return content in requested format
 * POST /api/scrape?action=crawl    — Crawl from a URL, following links up to a depth limit
 * POST /api/scrape?action=extract  — Extract structured data using CSS/XPath selectors
 * POST /api/scrape?action=structure — Use AI to parse raw text into structured tables
 *
 * All actions require admin token via X-Admin-Token header.
 */
import type { Context, Config } from "@netlify/functions";
import * as cheerio from "cheerio";
import Anthropic from "@anthropic-ai/sdk";

// ── Inline rate limiter (simple in-memory, resets per cold start) ──

interface RateLimitBucket {
  count: number;
  resetAt: number;
}

const rateBuckets = new Map<string, RateLimitBucket>();

function checkRateLimit(
  req: Request,
  endpoint: string,
  maxRequests: number,
  windowMs: number = 60_000
): { limited: boolean; retryAfterSeconds: number } {
  const ip =
    req.headers.get("x-nf-client-connection-ip") ||
    req.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ||
    "unknown";
  const key = `${endpoint}:${ip}`;
  const now = Date.now();
  const bucket = rateBuckets.get(key);

  if (!bucket || now > bucket.resetAt) {
    rateBuckets.set(key, { count: 1, resetAt: now + windowMs });
    return { limited: false, retryAfterSeconds: 0 };
  }

  bucket.count++;
  if (bucket.count > maxRequests) {
    const retryAfterSeconds = Math.max(
      1,
      Math.ceil((bucket.resetAt - now) / 1000)
    );
    return { limited: true, retryAfterSeconds };
  }

  return { limited: false, retryAfterSeconds: 0 };
}

// ── Types ──

interface ScrapeRequest {
  url: string;
  format?: "markdown" | "html" | "text" | "raw";
  include_metadata?: boolean;
  wait_for?: number;
  headers?: Record<string, string>;
  stealth?: boolean;
}

interface CrawlRequest {
  url: string;
  max_depth?: number;
  max_pages?: number;
  include_pattern?: string;
  exclude_pattern?: string;
  format?: "markdown" | "html" | "text";
}

interface ExtractRequest {
  url: string;
  selectors: Record<
    string,
    {
      selector: string;
      type?: "css" | "xpath";
      attribute?: string;
      multiple?: boolean;
    }
  >;
  headers?: Record<string, string>;
}

interface StructureRequest {
  content: string;
  hint?: string;
}

interface StructuredTable {
  name: string;
  columns: string[];
  rows: (string | number | null)[][];
}

interface PageResult {
  url: string;
  status: number;
  title: string;
  content: string;
  metadata?: Record<string, string>;
  links?: string[];
  word_count?: number;
  scraped_at: string;
  elapsed_ms: number;
}

// ── Stealth headers ──

const STEALTH_HEADERS: Record<string, string> = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept:
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
  DNT: "1",
  Connection: "keep-alive",
  "Upgrade-Insecure-Requests": "1",
  "Sec-Fetch-Dest": "document",
  "Sec-Fetch-Mode": "navigate",
  "Sec-Fetch-Site": "none",
  "Sec-Fetch-User": "?1",
  "Cache-Control": "max-age=0",
};

// ── Auth helper ──

function authenticate(req: Request): Response | null {
  const url = new URL(req.url);
  if (url.searchParams.has("token") || url.searchParams.has("api_key")) {
    return json(
      {
        error:
          "Authentication via URL parameters is not permitted. Use X-Admin-Token header.",
      },
      400
    );
  }
  const adminToken =
    process.env.HEALTH_API_TOKEN || process.env.ADMIN_API_TOKEN;
  if (!adminToken) return json({ error: "Admin token not configured" }, 503);
  const auth =
    req.headers.get("X-Admin-Token") ||
    req.headers.get("Authorization")?.replace(/^Bearer\s+/i, "") ||
    "";
  const authBuf = new TextEncoder().encode(auth.padEnd(adminToken.length));
  const tokenBuf = new TextEncoder().encode(adminToken.padEnd(auth.length));
  let diff = auth.length ^ adminToken.length;
  for (let i = 0; i < authBuf.length; i++) diff |= authBuf[i] ^ tokenBuf[i];
  if (diff !== 0) return json({ error: "Unauthorized" }, 401);
  return null;
}

// ── URL validation (SSRF prevention) ──

function validateUrl(raw: string): URL | null {
  try {
    const u = new URL(raw);
    if (u.protocol !== "http:" && u.protocol !== "https:") return null;
    const host = u.hostname.toLowerCase();
    if (
      host === "localhost" ||
      host === "127.0.0.1" ||
      host === "0.0.0.0" ||
      host === "[::1]" ||
      host === "[::]" ||
      host.startsWith("192.168.") ||
      host.startsWith("10.") ||
      host.startsWith("169.254.") ||
      /^172\.(1[6-9]|2\d|3[01])\./.test(host) ||
      host.endsWith(".local")
    )
      return null;
    return u;
  } catch {
    return null;
  }
}

// ── PDF detection ──

function isPdfUrl(url: string): boolean {
  try {
    return new URL(url).pathname.toLowerCase().endsWith(".pdf");
  } catch {
    return false;
  }
}

function isPdfResponse(headers: Headers): boolean {
  const ct = headers.get("content-type") || "";
  return ct.includes("application/pdf");
}

// ── Fetch page with stealth options ──

interface FetchResult {
  html: string;
  status: number;
  responseHeaders: Headers;
  isPdf: boolean;
  pdfBuffer?: ArrayBuffer;
  via?: "direct" | "backend";
  backendPdfResult?: any;
}

async function fetchDirect(
  url: string,
  opts: { headers?: Record<string, string>; stealth?: boolean } = {}
): Promise<FetchResult> {
  const fetchHeaders: Record<string, string> = {
    ...(opts.stealth !== false ? STEALTH_HEADERS : {}),
    ...(opts.headers || {}),
  };
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);
  try {
    const resp = await fetch(url, {
      headers: fetchHeaders,
      signal: controller.signal,
      redirect: "follow",
    });

    if (isPdfResponse(resp.headers) || isPdfUrl(url)) {
      const buffer = await resp.arrayBuffer();
      return {
        html: "",
        status: resp.status,
        responseHeaders: resp.headers,
        isPdf: true,
        pdfBuffer: buffer,
        via: "direct",
      };
    }

    const html = await resp.text();
    return {
      html,
      status: resp.status,
      responseHeaders: resp.headers,
      isPdf: false,
      via: "direct",
    };
  } finally {
    clearTimeout(timeout);
  }
}

// ── Proxy through Python backend (Scrapling + PyMuPDF) ──

async function fetchViaBackend(
  url: string,
  opts: { headers?: Record<string, string> } = {}
): Promise<FetchResult> {
  const backendUrl = process.env.SCRPR_BACKEND_URL;
  if (!backendUrl) throw new Error("SCRPR_BACKEND_URL not configured");
  const adminToken =
    process.env.ADMIN_API_TOKEN || process.env.HEALTH_API_TOKEN || "";

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 60000);
  try {
    const resp = await fetch(`${backendUrl}/api/scrape?action=scrape`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Admin-Token": adminToken,
      },
      body: JSON.stringify({
        url,
        format: "markdown",
        stealth: true,
        headers: opts.headers,
      }),
      signal: controller.signal,
    });

    const data = (await resp.json()) as any;
    if (!data.success) {
      throw new Error(data.error || "Backend scrape failed");
    }

    const d = data.data;
    const isPdf = d.format === "pdf";
    return {
      html: isPdf ? "" : d.content,
      status: d.status || 200,
      responseHeaders: new Headers({
        "content-type": isPdf ? "application/pdf" : "text/html",
      }),
      isPdf,
      via: "backend",
      backendPdfResult: isPdf ? d : undefined,
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchPage(
  url: string,
  opts: { headers?: Record<string, string>; stealth?: boolean } = {}
): Promise<FetchResult> {
  const result = await fetchDirect(url, opts);

  // If direct fetch got blocked (403) and Python backend is configured, retry via backend
  if (result.status === 403 && process.env.SCRPR_BACKEND_URL) {
    try {
      return await fetchViaBackend(url, { headers: opts.headers });
    } catch {
      // Backend fallback failed — return original 403 result
      return result;
    }
  }

  return result;
}

// ── Extract text from PDF using Claude's native PDF support ──

async function extractPdfText(
  pdfBuffer: ArrayBuffer
): Promise<{ text: string; pageCount: number }> {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    throw new Error(
      "ANTHROPIC_API_KEY not configured — needed for PDF text extraction"
    );
  }

  const anthropic = new Anthropic({ apiKey });
  const base64 = Buffer.from(pdfBuffer).toString("base64");

  const resp = await anthropic.messages.create({
    model: "claude-haiku-4-5-20251001",
    max_tokens: 16384,
    messages: [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: {
              type: "base64",
              media_type: "application/pdf",
              data: base64,
            },
          },
          {
            type: "text",
            text: "Extract ALL text content from this PDF document. Preserve the structure (headings, tables, lists). For tables, format them as markdown tables. Output the raw text content only, no commentary.",
          },
        ],
      },
    ],
  });

  const textBlock = resp.content.find((b) => b.type === "text");
  const text = textBlock?.text || "";

  // Estimate page count from PDF binary (count /Type /Page occurrences)
  const view = new Uint8Array(pdfBuffer);
  let pageCount = 0;
  const needle = [47, 84, 121, 112, 101, 32, 47, 80, 97, 103, 101]; // "/Type /Page"
  for (let i = 0; i < view.length - needle.length; i++) {
    let match = true;
    for (let j = 0; j < needle.length; j++) {
      if (view[i + j] !== needle[j]) {
        match = false;
        break;
      }
    }
    if (match) pageCount++;
  }
  if (pageCount > 1) pageCount = Math.max(1, pageCount - 1);
  if (pageCount === 0) pageCount = 1;

  return { text, pageCount };
}

// ── HTML → Markdown conversion ──

function htmlToMarkdown($: cheerio.CheerioAPI): string {
  $(
    "script, style, nav, footer, header, aside, iframe, noscript, svg, [role=banner], [role=navigation], [role=complementary]"
  ).remove();
  const lines: string[] = [];

  function processNode(el: cheerio.Cheerio<any>): void {
    el.contents().each((_: number, node: any) => {
      if (node.type === "text") {
        const text = $(node).text().trim();
        if (text) lines.push(text);
        return;
      }
      if (node.type !== "tag") return;
      const $n = $(node);
      const tag: string = node.tagName?.toLowerCase() || "";

      if (tag === "br") {
        lines.push("");
        return;
      }
      if (tag === "hr") {
        lines.push("\n---\n");
        return;
      }

      if (/^h[1-6]$/.test(tag)) {
        const level = parseInt(tag[1]);
        const text = $n.text().trim();
        if (text) lines.push("\n" + "#".repeat(level) + " " + text + "\n");
        return;
      }

      if (tag === "p") {
        const text = $n.text().trim();
        if (text) lines.push("\n" + text + "\n");
        return;
      }

      if (tag === "a") {
        const href = $n.attr("href");
        const text = $n.text().trim();
        if (text && href) lines.push(`[${text}](${href})`);
        else if (text) lines.push(text);
        return;
      }

      if (tag === "img") {
        const alt = $n.attr("alt") || "";
        const src = $n.attr("src") || "";
        if (src) lines.push(`![${alt}](${src})`);
        return;
      }

      if (tag === "li") {
        const text = $n.text().trim();
        if (text) lines.push("- " + text);
        return;
      }

      if (tag === "pre" || tag === "code") {
        const text = $n.text().trim();
        if (text) lines.push("\n```\n" + text + "\n```\n");
        return;
      }

      if (tag === "blockquote") {
        const text = $n.text().trim();
        if (text) lines.push("\n> " + text + "\n");
        return;
      }

      if (tag === "table") {
        const rows: string[][] = [];
        $n.find("tr").each((_: number, tr: any) => {
          const cells: string[] = [];
          $(tr)
            .find("td, th")
            .each((_: number, cell: any) => {
              cells.push($(cell).text().trim());
            });
          if (cells.length) rows.push(cells);
        });
        if (rows.length > 0) {
          lines.push("\n| " + rows[0].join(" | ") + " |");
          lines.push("| " + rows[0].map(() => "---").join(" | ") + " |");
          for (let i = 1; i < rows.length; i++) {
            lines.push("| " + rows[i].join(" | ") + " |");
          }
          lines.push("");
        }
        return;
      }

      processNode($n);
    });
  }

  const $main = $("main, article, [role=main]").first();
  processNode($main.length ? $main : $("body"));

  return lines.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

// ── Extract metadata ──

function extractMetadata(
  $: cheerio.CheerioAPI,
  url: string
): Record<string, string> {
  const meta: Record<string, string> = { url };
  const title = $("title").first().text().trim();
  if (title) meta.title = title;
  $("meta[name], meta[property]").each((_: number, el: any) => {
    const name = $(el).attr("name") || $(el).attr("property") || "";
    const content = $(el).attr("content") || "";
    if (name && content) meta[name.replace(/^og:/, "og_")] = content;
  });
  const canonical = $('link[rel="canonical"]').attr("href");
  if (canonical) meta.canonical = canonical;
  const lang = $("html").attr("lang");
  if (lang) meta.language = lang;
  return meta;
}

// ── Extract links ──

function extractLinks($: cheerio.CheerioAPI, baseUrl: string): string[] {
  const links = new Set<string>();
  $("a[href]").each((_: number, el: any) => {
    const href = $(el).attr("href");
    if (
      !href ||
      href.startsWith("#") ||
      href.startsWith("javascript:") ||
      href.startsWith("mailto:")
    )
      return;
    try {
      const abs = new URL(href, baseUrl).href;
      links.add(abs);
    } catch {
      /* ignore malformed */
    }
  });
  return [...links];
}

// ── Clean HTML ──

function cleanHtml($: cheerio.CheerioAPI): string {
  const $clone = cheerio.load($.html());
  $clone("script, style, noscript, svg, iframe").remove();
  const $main = $clone("main, article, [role=main]").first();
  return ($main.length ? $main : $clone("body")).html()?.trim() || "";
}

// ── Scrape action ──

async function handleScrape(body: ScrapeRequest): Promise<Response> {
  const parsed = validateUrl(body.url);
  if (!parsed) return json({ error: "Invalid or blocked URL" }, 400);
  const format = body.format || "markdown";
  const start = Date.now();

  const fetchResult = await fetchPage(body.url, {
    headers: body.headers,
    stealth: body.stealth ?? true,
  });

  // PDF already extracted by Python backend (Scrapling + PyMuPDF)
  if (fetchResult.backendPdfResult) {
    const d = fetchResult.backendPdfResult;
    const result: PageResult = {
      url: body.url,
      status: d.status || 200,
      title: d.title || "PDF Document",
      content: d.content,
      word_count: d.word_count || d.content.split(/\s+/).filter(Boolean).length,
      scraped_at: new Date().toISOString(),
      elapsed_ms: Date.now() - start,
    };
    if (body.include_metadata) {
      result.metadata = { url: body.url, content_type: "application/pdf" };
    }
    return json({
      success: true,
      data: { ...result, format: "pdf", page_count: d.page_count, via: "backend" },
    });
  }

  // PDF handling — use Claude to extract text
  if (fetchResult.isPdf && fetchResult.pdfBuffer) {
    try {
      const { text, pageCount } = await extractPdfText(fetchResult.pdfBuffer);
      const result: PageResult = {
        url: body.url,
        status: fetchResult.status,
        title:
          parsed.pathname
            .split("/")
            .pop()
            ?.replace(/\.pdf$/i, "")
            .replace(/[_-]/g, " ") || "PDF Document",
        content: text,
        word_count: text.split(/\s+/).filter(Boolean).length,
        scraped_at: new Date().toISOString(),
        elapsed_ms: Date.now() - start,
      };
      if (body.include_metadata) {
        result.metadata = { url: body.url, content_type: "application/pdf" };
      }
      return json({
        success: true,
        data: { ...result, format: "pdf", page_count: pageCount },
      });
    } catch (e: any) {
      return json({ error: `PDF extraction failed: ${e.message}` }, 422);
    }
  }

  const { html, status } = fetchResult;

  // Backend already returned markdown — use it directly
  if (fetchResult.via === "backend" && html) {
    const result: PageResult = {
      url: body.url,
      status,
      title: "",
      content: html,
      word_count: html.split(/\s+/).filter(Boolean).length,
      scraped_at: new Date().toISOString(),
      elapsed_ms: Date.now() - start,
    };
    return json({ success: true, data: { ...result, via: "backend" } });
  }

  const $ = cheerio.load(html);
  const title = $("title").first().text().trim();

  let content: string;
  switch (format) {
    case "markdown":
      content = htmlToMarkdown($);
      break;
    case "html":
      content = cleanHtml($);
      break;
    case "text": {
      $("script, style, noscript").remove();
      content = $("body").text().replace(/\s+/g, " ").trim();
      break;
    }
    case "raw":
      content = html;
      break;
    default:
      content = htmlToMarkdown($);
  }

  const result: PageResult = {
    url: body.url,
    status,
    title,
    content,
    word_count: content.split(/\s+/).filter(Boolean).length,
    scraped_at: new Date().toISOString(),
    elapsed_ms: Date.now() - start,
  };

  if (body.include_metadata) {
    result.metadata = extractMetadata($, body.url);
  }

  return json({ success: true, data: result });
}

// ── Crawl action ──

async function handleCrawl(body: CrawlRequest): Promise<Response> {
  const parsed = validateUrl(body.url);
  if (!parsed) return json({ error: "Invalid or blocked URL" }, 400);
  const maxDepth = Math.min(body.max_depth ?? 2, 3);
  const maxPages = Math.min(body.max_pages ?? 10, 20);
  const format = body.format || "markdown";
  const baseOrigin = parsed.origin;
  const visited = new Set<string>();
  const results: PageResult[] = [];
  const queue: { url: string; depth: number }[] = [
    { url: body.url, depth: 0 },
  ];
  let includeRe: RegExp | null = null;
  let excludeRe: RegExp | null = null;
  try {
    if (body.include_pattern) includeRe = new RegExp(body.include_pattern);
    if (body.exclude_pattern) excludeRe = new RegExp(body.exclude_pattern);
  } catch {
    return json({ error: "Invalid include_pattern or exclude_pattern regex" }, 400);
  }
  const start = Date.now();

  while (queue.length > 0 && results.length < maxPages) {
    const { url, depth } = queue.shift()!;
    if (visited.has(url)) continue;
    visited.add(url);

    try {
      const pageStart = Date.now();
      const fetchResult = await fetchPage(url, { stealth: true });

      // Skip PDFs during crawl (just note them)
      if (fetchResult.isPdf) {
        results.push({
          url,
          status: fetchResult.status,
          title: "PDF Document",
          content: "[PDF file — use direct scrape for text extraction]",
          scraped_at: new Date().toISOString(),
          elapsed_ms: Date.now() - pageStart,
        });
        continue;
      }

      const { html, status } = fetchResult;
      const $ = cheerio.load(html);
      const title = $("title").first().text().trim();
      let content: string;
      switch (format) {
        case "markdown":
          content = htmlToMarkdown($);
          break;
        case "html":
          content = cleanHtml($);
          break;
        case "text": {
          $("script, style, noscript").remove();
          content = $("body").text().replace(/\s+/g, " ").trim();
          break;
        }
        default:
          content = htmlToMarkdown($);
      }
      const links = extractLinks($, url);
      results.push({
        url,
        status,
        title,
        content,
        links,
        word_count: content.split(/\s+/).filter(Boolean).length,
        scraped_at: new Date().toISOString(),
        elapsed_ms: Date.now() - pageStart,
      });

      if (depth < maxDepth) {
        for (const link of links) {
          if (visited.has(link)) continue;
          try {
            const linkUrl = new URL(link);
            if (linkUrl.origin !== baseOrigin) continue;
            if (includeRe && !includeRe.test(link)) continue;
            if (excludeRe && excludeRe.test(link)) continue;
            queue.push({ url: link, depth: depth + 1 });
          } catch {
            /* skip */
          }
        }
      }
    } catch (e: any) {
      results.push({
        url,
        status: 0,
        title: "",
        content: `Error: ${e.message}`,
        scraped_at: new Date().toISOString(),
        elapsed_ms: 0,
      });
    }
  }

  return json({
    success: true,
    data: {
      pages_scraped: results.length,
      pages_discovered: visited.size,
      total_elapsed_ms: Date.now() - start,
      results,
    },
  });
}

// ── Extract action ──

async function handleExtract(body: ExtractRequest): Promise<Response> {
  const parsed = validateUrl(body.url);
  if (!parsed) return json({ error: "Invalid or blocked URL" }, 400);
  if (!body.selectors || Object.keys(body.selectors).length === 0) {
    return json({ error: "At least one selector is required" }, 400);
  }
  const start = Date.now();
  const { html, status } = await fetchPage(body.url, {
    headers: body.headers,
    stealth: true,
  });
  const $ = cheerio.load(html);
  const extracted: Record<string, string | string[]> = {};

  for (const [key, spec] of Object.entries(body.selectors)) {
    const { selector, attribute, multiple } = spec;
    const els = $(selector);
    if (multiple) {
      const values: string[] = [];
      els.each((_: number, el: any) => {
        const val = attribute ? $(el).attr(attribute) : $(el).text().trim();
        if (val) values.push(val);
      });
      extracted[key] = values;
    } else {
      const el = els.first();
      extracted[key] = attribute
        ? el.attr(attribute) || ""
        : el.text().trim();
    }
  }

  return json({
    success: true,
    data: {
      url: body.url,
      status,
      extracted,
      scraped_at: new Date().toISOString(),
      elapsed_ms: Date.now() - start,
    },
  });
}

// ── Structure action (Claude-powered) ──

async function handleStructure(body: StructureRequest): Promise<Response> {
  if (!body.content || body.content.length < 10) {
    return json({ error: "Content is required (min 10 chars)" }, 400);
  }
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey)
    return json({ error: "ANTHROPIC_API_KEY not configured" }, 503);

  const start = Date.now();
  const anthropic = new Anthropic({ apiKey });
  const truncated = body.content.slice(0, 50_000);
  const hintLine = body.hint ? `\nHint about the data: ${body.hint}\n` : "";

  const resp = await anthropic.messages.create({
    model: "claude-haiku-4-5-20251001",
    max_tokens: 16384,
    messages: [
      {
        role: "user",
        content: `You are a data extraction expert. Parse the following raw text content into structured tables.
${hintLine}
Return ONLY a JSON array of table objects. Each table object must have:
- "name": string (descriptive table name)
- "columns": string[] (column headers)
- "rows": (string|number|null)[][] (data rows, using numbers for numeric values)

Rules:
- Extract ALL data rows, not just samples
- Clean up values: remove extra whitespace, normalize currency/percentages
- For financial data: keep numbers as numbers (no $ or % symbols in numeric cells), but keep the column header clear (e.g. "Commitment ($M)" or "IRR (%)")
- If the text contains multiple logical tables, return each as a separate object
- If data appears to be a single table, return an array with one object
- Return valid JSON only, no markdown fences or explanation

Raw text content:
${truncated}`,
      },
    ],
  });

  const textBlock = resp.content.find((b) => b.type === "text");
  const raw = textBlock?.text || "[]";

  let tables: StructuredTable[];
  try {
    const cleaned = raw
      .replace(/^```(?:json)?\s*\n?/i, "")
      .replace(/\n?```\s*$/i, "")
      .trim();
    const parsed = JSON.parse(cleaned);
    tables = Array.isArray(parsed) ? parsed : [parsed];
  } catch {
    return json(
      {
        error: "Failed to parse structured data from AI response",
        raw_response: raw.slice(0, 500),
      },
      422
    );
  }

  return json({
    success: true,
    data: {
      tables,
      elapsed_ms: Date.now() - start,
    },
  });
}

// ── Handler ──

export default async (req: Request, context: Context) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: corsHeaders() });
  }
  if (req.method !== "POST") {
    return json({ error: "POST required" }, 405);
  }

  // Rate limit
  const rl = checkRateLimit(req, "scrape", 30);
  if (rl.limited)
    return json(
      { error: "Rate limit exceeded", retry_after: rl.retryAfterSeconds },
      429
    );

  // Auth
  const authErr = authenticate(req);
  if (authErr) return authErr;

  const url = new URL(req.url);
  const action = url.searchParams.get("action") || "scrape";

  try {
    const body = await req.json();
    switch (action) {
      case "scrape":
        return await handleScrape(body as ScrapeRequest);
      case "crawl":
        return await handleCrawl(body as CrawlRequest);
      case "extract":
        return await handleExtract(body as ExtractRequest);
      case "structure":
        return await handleStructure(body as StructureRequest);
      default:
        return json(
          { error: "Unknown action. Use: scrape, crawl, extract, structure" },
          400
        );
    }
  } catch (e: any) {
    console.error("Scrape API error:", e);
    return json({ error: e.message || "Scrape failed" }, 500);
  }
};

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      ...corsHeaders(),
    },
  });
}

function corsHeaders(): Record<string, string> {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-Admin-Token, Authorization",
  };
}

export const config: Config = { path: "/api/scrape" };

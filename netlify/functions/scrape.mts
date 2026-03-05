/**
 * MKT-SCRAPE API
 *
 * POST /api/scrape?action=scrape   — Scrape a single URL, return content in requested format
 * POST /api/scrape?action=crawl    — Crawl from a URL, following links up to a depth limit
 * POST /api/scrape?action=extract  — Extract structured data using CSS selectors
 *
 * All actions require admin token via X-Admin-Token header.
 * Inspired by Scrapling's fetcher architecture and Firecrawl's API surface.
 */
import type { Context, Config } from "@netlify/functions";
import { load as cheerioLoad, type CheerioAPI } from "cheerio";

// ── Types ──

interface ScrapeRequest {
  url: string;
  format?: "markdown" | "html" | "text" | "raw";
  include_metadata?: boolean;
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
  selectors: Record<string, { selector: string; type?: "css" | "xpath"; attribute?: string; multiple?: boolean }>;
  headers?: Record<string, string>;
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

// ── Stealth headers (Scrapling-inspired) ──

const STEALTH_HEADERS: Record<string, string> = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
  "DNT": "1",
  "Connection": "keep-alive",
  "Upgrade-Insecure-Requests": "1",
  "Sec-Fetch-Dest": "document",
  "Sec-Fetch-Mode": "navigate",
  "Sec-Fetch-Site": "none",
  "Sec-Fetch-User": "?1",
  "Cache-Control": "max-age=0",
};

// ── Simple in-memory rate limiter ──

const rateMap = new Map<string, { count: number; resetAt: number }>();

function checkRate(ip: string, limit = 30, windowMs = 60_000): { ok: boolean; remaining: number } {
  const now = Date.now();
  const entry = rateMap.get(ip);
  if (!entry || now > entry.resetAt) {
    rateMap.set(ip, { count: 1, resetAt: now + windowMs });
    return { ok: true, remaining: limit - 1 };
  }
  entry.count++;
  if (entry.count > limit) return { ok: false, remaining: 0 };
  return { ok: true, remaining: limit - entry.count };
}

// ── Auth helper ──

function authenticate(req: Request): Response | null {
  const url = new URL(req.url);
  if (url.searchParams.has("token") || url.searchParams.has("api_key")) {
    return json({ error: "Authentication via URL parameters is not permitted. Use X-Admin-Token header." }, 400);
  }
  const adminToken = Netlify.env.get("ADMIN_TOKEN");
  if (!adminToken) return json({ error: "ADMIN_TOKEN not configured — set it in Netlify environment variables" }, 503);
  const auth = req.headers.get("X-Admin-Token") || req.headers.get("Authorization")?.replace(/^Bearer\s+/i, "") || "";
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
      host.startsWith("192.168.") ||
      host.startsWith("10.") ||
      host.startsWith("172.") ||
      host.endsWith(".local") ||
      host === "[::1]"
    ) return null;
    return u;
  } catch {
    return null;
  }
}

// ── Fetch page with stealth options ──

async function fetchPage(url: string, opts: { headers?: Record<string, string>; stealth?: boolean } = {}): Promise<{ html: string; status: number }> {
  const fetchHeaders: Record<string, string> = {
    ...(opts.stealth !== false ? STEALTH_HEADERS : {}),
    ...(opts.headers || {}),
  };
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const resp = await fetch(url, {
      headers: fetchHeaders,
      signal: controller.signal,
      redirect: "follow",
    });
    const html = await resp.text();
    return { html, status: resp.status };
  } finally {
    clearTimeout(timeout);
  }
}

// ── HTML to Markdown conversion ──

function htmlToMarkdown($: CheerioAPI): string {
  $("script, style, nav, footer, header, aside, iframe, noscript, svg, [role=banner], [role=navigation], [role=complementary]").remove();

  const lines: string[] = [];

  function processNode(el: ReturnType<CheerioAPI>) {
    el.contents().each((_, node: any) => {
      if (node.type === "text") {
        const text = $(node).text().trim();
        if (text) lines.push(text);
        return;
      }
      if (node.type !== "tag") return;
      const $n = $(node);
      const tag = (node as any).tagName?.toLowerCase() || "";

      if (tag === "br") { lines.push(""); return; }
      if (tag === "hr") { lines.push("\n---\n"); return; }

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
        $n.find("tr").each((_, tr) => {
          const cells: string[] = [];
          $(tr).find("td, th").each((_, cell) => {
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

  return lines
    .join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

// ── Extract metadata ──

function extractMetadata($: CheerioAPI, url: string): Record<string, string> {
  const meta: Record<string, string> = { url };
  const title = $("title").first().text().trim();
  if (title) meta.title = title;
  $("meta[name], meta[property]").each((_, el) => {
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

function extractLinks($: CheerioAPI, baseUrl: string): string[] {
  const links = new Set<string>();
  $("a[href]").each((_, el) => {
    const href = $(el).attr("href");
    if (!href || href.startsWith("#") || href.startsWith("javascript:") || href.startsWith("mailto:")) return;
    try {
      const abs = new URL(href, baseUrl).href;
      links.add(abs);
    } catch { /* ignore malformed */ }
  });
  return [...links];
}

// ── Clean HTML ──

function cleanHtml($: CheerioAPI): string {
  const $clone = cheerioLoad($.html());
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

  const { html, status } = await fetchPage(body.url, {
    headers: body.headers,
    stealth: body.stealth ?? true,
  });

  const $ = cheerioLoad(html);
  const title = $("title").first().text().trim();

  let content: string;
  switch (format) {
    case "markdown": content = htmlToMarkdown($); break;
    case "html": content = cleanHtml($); break;
    case "text": {
      $("script, style, noscript").remove();
      content = $("body").text().replace(/\s+/g, " ").trim();
      break;
    }
    case "raw": content = html; break;
    default: content = htmlToMarkdown($);
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
  const queue: { url: string; depth: number }[] = [{ url: body.url, depth: 0 }];

  const includeRe = body.include_pattern ? new RegExp(body.include_pattern) : null;
  const excludeRe = body.exclude_pattern ? new RegExp(body.exclude_pattern) : null;

  const start = Date.now();

  while (queue.length > 0 && results.length < maxPages) {
    const { url, depth } = queue.shift()!;
    if (visited.has(url)) continue;
    visited.add(url);

    try {
      const pageStart = Date.now();
      const { html, status } = await fetchPage(url, { stealth: true });
      const $ = cheerioLoad(html);
      const title = $("title").first().text().trim();

      let content: string;
      switch (format) {
        case "markdown": content = htmlToMarkdown($); break;
        case "html": content = cleanHtml($); break;
        case "text": {
          $("script, style, noscript").remove();
          content = $("body").text().replace(/\s+/g, " ").trim();
          break;
        }
        default: content = htmlToMarkdown($);
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
          } catch { /* skip */ }
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
  const { html, status } = await fetchPage(body.url, { headers: body.headers, stealth: true });
  const $ = cheerioLoad(html);

  const extracted: Record<string, string | string[]> = {};

  for (const [key, spec] of Object.entries(body.selectors)) {
    const { selector, attribute, multiple } = spec;
    const els = $(selector);

    if (multiple) {
      const values: string[] = [];
      els.each((_, el) => {
        const val = attribute ? $(el).attr(attribute) : $(el).text().trim();
        if (val) values.push(val);
      });
      extracted[key] = values;
    } else {
      const el = els.first();
      extracted[key] = attribute ? (el.attr(attribute) || "") : el.text().trim();
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

// ── Handler ──

export default async (req: Request, context: Context) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: corsHeaders() });
  }

  if (req.method !== "POST") {
    return json({ error: "POST required" }, 405);
  }

  // Rate limit by IP
  const ip = req.headers.get("x-forwarded-for")?.split(",")[0]?.trim() || "unknown";
  const rate = checkRate(ip);
  if (!rate.ok) return json({ error: "Rate limit exceeded" }, 429);

  // Auth
  const authErr = authenticate(req);
  if (authErr) return authErr;

  const url = new URL(req.url);
  const action = url.searchParams.get("action") || "scrape";

  try {
    const body = await req.json();

    switch (action) {
      case "scrape": return await handleScrape(body as ScrapeRequest);
      case "crawl": return await handleCrawl(body as CrawlRequest);
      case "extract": return await handleExtract(body as ExtractRequest);
      default: return json({ error: "Unknown action. Use: scrape, crawl, extract" }, 400);
    }
  } catch (e: any) {
    console.error("Scrape API error:", e);
    return json({ error: e.message || "Scrape failed" }, 500);
  }
};

function json(data: any, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeaders() },
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

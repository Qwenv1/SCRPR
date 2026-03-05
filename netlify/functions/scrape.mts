/**
 * Scraping Dashboard API
 *
 * POST /api/scrape?action=scrape   — Scrape a single URL
 * POST /api/scrape?action=crawl    — Crawl from a URL
 * POST /api/scrape?action=extract  — Extract with CSS selectors
 * POST /api/scrape?action=structure — AI-powered table parsing
 */
import type { Context, Config } from "@netlify/functions";
import { load as cheerioLoad, type CheerioAPI } from "cheerio";
import Anthropic from "@anthropic-ai/sdk";

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

function authenticate(req: Request): Response | null {
  const url = new URL(req.url);
  if (url.searchParams.has("token") || url.searchParams.has("api_key")) {
    return json({ error: "Authentication via URL parameters is not permitted. Use X-Admin-Token header." }, 400);
  }
  const adminToken = Netlify.env.get("HEALTH_API_TOKEN") || Netlify.env.get("ADMIN_API_TOKEN");
  if (!adminToken) return json({ error: "Admin token not configured" }, 503);
  const auth = req.headers.get("X-Admin-Token") || req.headers.get("Authorization")?.replace(/^Bearer\s+/i, "") || "";
  const authBuf = new TextEncoder().encode(auth.padEnd(adminToken.length));
  const tokenBuf = new TextEncoder().encode(adminToken.padEnd(auth.length));
  let diff = auth.length ^ adminToken.length;
  for (let i = 0; i < authBuf.length; i++) diff |= authBuf[i] ^ tokenBuf[i];
  if (diff !== 0) return json({ error: "Unauthorized" }, 401);
  return null;
}

function isPrivateHost(host: string): boolean {
  if (host === "localhost" || host === "127.0.0.1" || host === "0.0.0.0" || host === "[::1]" || host.endsWith(".local")) return true;
  if (host.startsWith("10.")) return true;
  if (host.startsWith("192.168.")) return true;
  if (host.startsWith("172.")) {
    const parts = host.split(".");
    const second = parseInt(parts[1], 10);
    if (second >= 16 && second <= 31) return true;
  }
  if (host.startsWith("169.254.")) return true;
  return false;
}

function validateUrl(raw: string): URL | null {
  if (!raw || typeof raw !== "string") return null;
  try {
    const u = new URL(raw.trim());
    if (u.protocol !== "http:" && u.protocol !== "https:") return null;
    if (isPrivateHost(u.hostname.toLowerCase())) return null;
    return u;
  } catch {
    return null;
  }
}

async function fetchPage(url: string, opts: { headers?: Record<string, string>; stealth?: boolean } = {}): Promise<{ html: string; status: number; responseHeaders: Headers }> {
  const fetchHeaders: Record<string, string> = {
    ...(opts.stealth !== false ? STEALTH_HEADERS : {}),
    ...(opts.headers || {}),
  };
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const resp = await fetch(url, { headers: fetchHeaders, signal: controller.signal, redirect: "follow" });
    const html = await resp.text();
    return { html, status: resp.status, responseHeaders: resp.headers };
  } finally {
    clearTimeout(timeout);
  }
}

function htmlToMarkdown($: CheerioAPI): string {
  $("script, style, nav, footer, header, aside, iframe, noscript, svg, [role=banner], [role=navigation], [role=complementary]").remove();
  const lines: string[] = [];
  function processNode(el: ReturnType<CheerioAPI>) {
    el.contents().each((_, node: any) => {
      if (node.type === "text") { const text = $(node).text().trim(); if (text) lines.push(text); return; }
      if (node.type !== "tag") return;
      const $n = $(node);
      const tag = (node as any).tagName?.toLowerCase() || "";
      if (tag === "br") { lines.push(""); return; }
      if (tag === "hr") { lines.push("\n---\n"); return; }
      if (/^h[1-6]$/.test(tag)) { const level = parseInt(tag[1]); const text = $n.text().trim(); if (text) lines.push("\n" + "#".repeat(level) + " " + text + "\n"); return; }
      if (tag === "p") { const text = $n.text().trim(); if (text) lines.push("\n" + text + "\n"); return; }
      if (tag === "a") { const href = $n.attr("href"); const text = $n.text().trim(); if (text && href) lines.push(`[${text}](${href})`); else if (text) lines.push(text); return; }
      if (tag === "img") { const alt = $n.attr("alt") || ""; const src = $n.attr("src") || ""; if (src) lines.push(`![${alt}](${src})`); return; }
      if (tag === "li") { const text = $n.text().trim(); if (text) lines.push("- " + text); return; }
      if (tag === "pre" || tag === "code") { const text = $n.text().trim(); if (text) lines.push("\n```\n" + text + "\n```\n"); return; }
      if (tag === "blockquote") { const text = $n.text().trim(); if (text) lines.push("\n> " + text + "\n"); return; }
      if (tag === "table") {
        const rows: string[][] = [];
        $n.find("tr").each((_, tr) => { const cells: string[] = []; $(tr).find("td, th").each((_, cell) => { cells.push($(cell).text().trim()); }); if (cells.length) rows.push(cells); });
        if (rows.length > 0) {
          lines.push("\n| " + rows[0].join(" | ") + " |");
          lines.push("| " + rows[0].map(() => "---").join(" | ") + " |");
          for (let i = 1; i < rows.length; i++) lines.push("| " + rows[i].join(" | ") + " |");
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

function extractMetadata($: CheerioAPI, url: string): Record<string, string> {
  const meta: Record<string, string> = { url };
  const title = $("title").first().text().trim();
  if (title) meta.title = title;
  $('meta[name], meta[property]').each((_, el) => {
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

function extractLinks($: CheerioAPI, baseUrl: string): string[] {
  const links = new Set<string>();
  $("a[href]").each((_, el) => {
    const href = $(el).attr("href");
    if (!href || href.startsWith("#") || href.startsWith("javascript:") || href.startsWith("mailto:")) return;
    try { links.add(new URL(href, baseUrl).href); } catch {}
  });
  return [...links];
}

function cleanHtml($: CheerioAPI): string {
  const $clone = cheerioLoad($.html());
  $clone("script, style, noscript, svg, iframe").remove();
  const $main = $clone("main, article, [role=main]").first();
  return ($main.length ? $main : $clone("body")).html()?.trim() || "";
}

async function handleScrape(body: ScrapeRequest): Promise<Response> {
  const parsed = validateUrl(body.url);
  if (!parsed) return json({ error: "Invalid or blocked URL. Ensure it starts with http:// or https:// and is a public address." }, 400);
  const format = body.format || "markdown";
  const start = Date.now();
  const { html, status } = await fetchPage(body.url, { headers: body.headers, stealth: body.stealth ?? true });
  const $ = cheerioLoad(html);
  const title = $("title").first().text().trim();
  let content: string;
  switch (format) {
    case "markdown": content = htmlToMarkdown($); break;
    case "html": content = cleanHtml($); break;
    case "text": { $("script, style, noscript").remove(); content = $("body").text().replace(/\s+/g, " ").trim(); break; }
    case "raw": content = html; break;
    default: content = htmlToMarkdown($);
  }
  const result: PageResult = { url: body.url, status, title, content, word_count: content.split(/\s+/).filter(Boolean).length, scraped_at: new Date().toISOString(), elapsed_ms: Date.now() - start };
  if (body.include_metadata) result.metadata = extractMetadata($, body.url);
  return json({ success: true, data: result });
}

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
        case "text": { $("script, style, noscript").remove(); content = $("body").text().replace(/\s+/g, " ").trim(); break; }
        default: content = htmlToMarkdown($);
      }
      const links = extractLinks($, url);
      results.push({ url, status, title, content, links, word_count: content.split(/\s+/).filter(Boolean).length, scraped_at: new Date().toISOString(), elapsed_ms: Date.now() - pageStart });
      if (depth < maxDepth) {
        for (const link of links) {
          if (visited.has(link)) continue;
          try { const lu = new URL(link); if (lu.origin !== baseOrigin) continue; if (includeRe && !includeRe.test(link)) continue; if (excludeRe && excludeRe.test(link)) continue; queue.push({ url: link, depth: depth + 1 }); } catch {}
        }
      }
    } catch (e: any) {
      results.push({ url, status: 0, title: "", content: `Error: ${e.message}`, scraped_at: new Date().toISOString(), elapsed_ms: 0 });
    }
  }
  return json({ success: true, data: { pages_scraped: results.length, pages_discovered: visited.size, total_elapsed_ms: Date.now() - start, results } });
}

async function handleExtract(body: ExtractRequest): Promise<Response> {
  const parsed = validateUrl(body.url);
  if (!parsed) return json({ error: "Invalid or blocked URL" }, 400);
  if (!body.selectors || Object.keys(body.selectors).length === 0) return json({ error: "At least one selector is required" }, 400);
  const start = Date.now();
  const { html, status } = await fetchPage(body.url, { headers: body.headers, stealth: true });
  const $ = cheerioLoad(html);
  const extracted: Record<string, string | string[]> = {};
  for (const [key, spec] of Object.entries(body.selectors)) {
    const { selector, attribute, multiple } = spec;
    const els = $(selector);
    if (multiple) { const values: string[] = []; els.each((_, el) => { const val = attribute ? $(el).attr(attribute) : $(el).text().trim(); if (val) values.push(val); }); extracted[key] = values; }
    else { const el = els.first(); extracted[key] = attribute ? (el.attr(attribute) || "") : el.text().trim(); }
  }
  return json({ success: true, data: { url: body.url, status, extracted, scraped_at: new Date().toISOString(), elapsed_ms: Date.now() - start } });
}

async function handleStructure(body: StructureRequest): Promise<Response> {
  if (!body.content || body.content.length < 10) return json({ error: "Content is required (min 10 chars)" }, 400);
  const apiKey = Netlify.env.get("ANTHROPIC_API_KEY");
  if (!apiKey) return json({ error: "ANTHROPIC_API_KEY not configured" }, 503);
  const start = Date.now();
  const anthropic = new Anthropic({ apiKey });
  const truncated = body.content.slice(0, 100_000);
  const hintLine = body.hint ? `\nHint about the data: ${body.hint}\n` : "";
  const resp = await anthropic.messages.create({
    model: "claude-sonnet-4-20250514",
    max_tokens: 8192,
    messages: [{ role: "user", content: `You are a data extraction expert. Parse the following raw text content into structured tables.
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
${truncated}` }],
  });
  const textBlock = resp.content.find((b) => b.type === "text");
  const raw = textBlock?.text || "[]";
  let tables: StructuredTable[];
  try {
    const cleaned = raw.replace(/^```(?:json)?\s*\n?/i, "").replace(/\n?```\s*$/i, "").trim();
    const parsed = JSON.parse(cleaned);
    tables = Array.isArray(parsed) ? parsed : [parsed];
  } catch {
    return json({ error: "Failed to parse structured data from AI response", raw_response: raw.slice(0, 500) }, 422);
  }
  return json({ success: true, data: { tables, elapsed_ms: Date.now() - start } });
}

export default async (req: Request, context: Context) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders() });
  if (req.method !== "POST") return json({ error: "POST required" }, 405);
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
      case "structure": return await handleStructure(body as StructureRequest);
      default: return json({ error: "Unknown action. Use: scrape, crawl, extract, structure" }, 400);
    }
  } catch (e: any) {
    console.error("Scrape API error:", e);
    return json({ error: e.message || "Scrape failed" }, 500);
  }
};

function json(data: any, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeaders() } });
}

function corsHeaders(): Record<string, string> {
  return { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type, X-Admin-Token, Authorization" };
}

export const config: Config = { path: "/api/scrape" };

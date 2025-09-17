#!/usr/bin/env python3
import asyncio
import json
import os
import urllib.parse
from typing import Any, Dict, List, Optional

from apify import Actor, ProxyConfiguration
from apify.storages import KeyValueStore

from playwright.async_api import async_playwright, Browser, Page

# Labels
PRODUCT_LABEL = "PRODUCT"
LISTING_LABEL = "LISTING"
SELLER_LABEL = "SELLER"
CATEGORY_LABEL = "CATEGORY"
KEYWORD_LABEL = "KEYWORD"

# default navigation timeout (ms) if none provided
DEFAULT_NAVIGATION_TIMEOUT_MS = 30000


class LimitsTracker:
    def __init__(self):
        self.limits = {}


# ---------------------
# Helpers
# ---------------------
def build_search_url_for_keyword(keyword: str, template: Optional[str] = None) -> str:
    template = template or "https://www.tiktok.com/search?q={keyword}"
    return template.format(keyword=urllib.parse.quote_plus(keyword))


def normalize_start_items(raw_start_urls: Any) -> List[Dict[str, Any]]:
    out = []
    if not raw_start_urls:
        return out
    if isinstance(raw_start_urls, str):
        out.append({"url": raw_start_urls})
        return out
    if isinstance(raw_start_urls, list):
        for item in raw_start_urls:
            if isinstance(item, str):
                out.append({"url": item})
            elif isinstance(item, dict):
                url = item.get("url")
                if url:
                    ud = item.get("userData") or {}
                    if "label" in item:
                        ud["label"] = item["label"]
                    out.append({"url": url, "userData": ud})
    return out


def choose_label_for_url(url: str, explicit_userdata: Dict[str, Any]) -> Dict[str, Any]:
    if explicit_userdata and explicit_userdata.get("label"):
        return explicit_userdata
    lower = url.lower()
    if "search" in lower or "tag" in lower or "shop" in lower or "collections" in lower:
        return {"label": LISTING_LABEL}
    return {"label": PRODUCT_LABEL}


def ms_timeouts_from_input(raw_timeouts: Dict[str, Any]) -> Dict[str, int]:
    """
    Convert various timeout formats in input to ms map used by code.
    Accepts:
      - navigationTimeoutSecs (seconds)
      - requestTimeoutSecs (seconds)
      - navigation (ms)
    Returns dict with 'navigation' ms.
    """
    out = {}
    if not isinstance(raw_timeouts, dict):
        out["navigation"] = DEFAULT_NAVIGATION_TIMEOUT_MS
        return out
    if "navigation" in raw_timeouts:
        out["navigation"] = int(raw_timeouts["navigation"])
    elif "navigationTimeoutSecs" in raw_timeouts:
        out["navigation"] = int(raw_timeouts["navigationTimeoutSecs"]) * 1000
    else:
        out["navigation"] = DEFAULT_NAVIGATION_TIMEOUT_MS
    return out


# ---------------------
# Processing functions (improved)
# ---------------------
async def process_product_task(
    browser: Browser,
    req: Dict[str, Any],
    proxy_url: Optional[str],
    accept_language: str,
    region: str,
    timeouts: Dict[str, Any],
    include_creator_videos: bool,
    capture_screenshots: bool,
    notify_cfg: Dict[str, Any],
    kv_store: KeyValueStore,
    log,
):
    url = req.get("url")
    log.info(f"[PRODUCT] Processing {url}")
    context = await browser.new_context()
    page: Page = await context.new_page()
    try:
        await page.goto(url, timeout=timeouts.get("navigation", DEFAULT_NAVIGATION_TIMEOUT_MS))
        await page.wait_for_load_state("networkidle")
        # Basic extraction example: title + url
        try:
            title = await page.title()
        except Exception:
            title = None

        # Example: push basic scraped object to default dataset
        scraped = {"url": url, "title": title}
        await Actor.push_data(scraped)
        log.info(f"[PRODUCT] pushed data for {url}: {scraped}")
        # Optionally save screenshot if requested
        if capture_screenshots:
            try:
                screenshot = await page.screenshot()
                # store screenshot in KV store under a safe key
                key = f"screenshots/{urllib.parse.quote_plus(url)}.png"
                await kv_store.set_value(key, screenshot, content_type="image/png")
                log.info(f"[PRODUCT] saved screenshot for {url} as {key}")
            except Exception as e:
                log.warning(f"[PRODUCT] screenshot failed for {url}: {e}")
    except Exception as e:
        log.warning(f"[PRODUCT] navigation/extract error for {url}: {e}")
    finally:
        await page.close()
        await context.close()


async def process_listing_task(
    browser: Browser,
    req: Dict[str, Any],
    proxy_url: Optional[str],
    accept_language: str,
    region: str,
    timeouts: Dict[str, Any],
    request_queue,
    limits: LimitsTracker,
    log,
    kv_store: KeyValueStore,
    debug: bool = False,
):
    url = req.get("url")
    log.info(f"[LISTING] Processing {url}")
    context = await browser.new_context()
    page: Page = await context.new_page()
    discovered = 0
    try:
        await page.goto(url, timeout=timeouts.get("navigation", DEFAULT_NAVIGATION_TIMEOUT_MS))
        await page.wait_for_load_state("networkidle")

        # Heuristic: collect anchors and filter by patterns commonly seen in product links.
        anchors = await page.query_selector_all("a")
        hrefs = []
        for a in anchors:
            try:
                href = await a.get_attribute("href")
            except Exception:
                href = None
            if not href:
                continue
            href = href.strip()
            # Normalize to absolute URL
            if href.startswith("/"):
                href = urllib.parse.urljoin(page.url, href)
            # Skip JS anchors
            if href.startswith("javascript:") or href.startswith("#"):
                continue
            hrefs.append(href)

        # Basic filtering: product-like patterns — adjust for actual TikTok Shop structure
        product_candidates = []
        for h in hrefs:
            hl = h.lower()
            # common heuristics (you may refine)
            if "/product/" in hl or "/item/" in hl or "/shop/" in hl or "/video/" in hl or "tiktok.com/@" in hl:
                product_candidates.append(h)

        # remove duplicates and limit
        unique = list(dict.fromkeys(product_candidates))
        for candidate in unique:
            # Skip if already in queue: SDK may de-dupe but we attempt to add
            try:
                await request_queue.add_request({"url": candidate, "userData": {"label": PRODUCT_LABEL}})
                discovered += 1
                log.info(f"[LISTING] queued product: {candidate}")
            except Exception as e:
                log.debug(f"[LISTING] add_request failed for {candidate}: {e}")

        if discovered == 0:
            # Debug: save listing HTML so you can inspect what the page actually contains (selector tuning)
            if debug:
                try:
                    page_html = await page.content()
                    key = f"debug/listing-{urllib.parse.quote_plus(url)}.html"
                    await kv_store.set_value(key, page_html, content_type="text/html")
                    log.info(f"[LISTING] saved listing HTML to kv://{key} for inspection")
                except Exception as e:
                    log.warning(f"[LISTING] failed saving debug HTML for {url}: {e}")

            log.info(f"[LISTING] no product candidates discovered on {url}. Found {len(hrefs)} anchors, {len(product_candidates)} product-like candidates.")

    except Exception as e:
        log.warning(f"[LISTING] navigation/extract error for {url}: {e}")
    finally:
        await page.close()
        await context.close()


# ---------------------
# Worker loop
# ---------------------
async def worker_loop(
    worker_id: int,
    browser: Browser,
    request_queue,
    proxy_configuration,
    accept_language: str,
    region: str,
    timeouts: Dict[str, Any],
    include_creator_videos: bool,
    capture_screenshots: bool,
    notify_cfg: Dict[str, Any],
    kv_store: KeyValueStore,
    limits: LimitsTracker,
    log,
    debug: bool,
):
    log.info(f"Worker {worker_id} started")
    while True:
        req = await request_queue.fetch_next_request()
        if not req:
            log.info(f"Worker {worker_id}: no request fetched, exiting")
            break

        # support both dict-like request and SDK Request objects
        try:
            url = req.get("url") if isinstance(req, dict) else getattr(req, "url", None)
        except Exception:
            url = None

        label = (req.get("userData") or {}).get("label") if isinstance(req, dict) else None

        proxy_url = None
        try:
            proxy_url = await proxy_configuration.new_url() if proxy_configuration else None
        except Exception:
            proxy_url = None

        log.info(f"Worker {worker_id} fetched: {url} (label={label})")

        try:
            if label == PRODUCT_LABEL:
                await process_product_task(
                    browser,
                    req if isinstance(req, dict) else {"url": url},
                    proxy_url,
                    accept_language,
                    region,
                    timeouts,
                    include_creator_videos,
                    capture_screenshots,
                    notify_cfg,
                    kv_store,
                    log,
                )
            else:
                # treat everything else as LISTING for safety
                await process_listing_task(
                    browser,
                    req if isinstance(req, dict) else {"url": url},
                    proxy_url,
                    accept_language,
                    region,
                    timeouts,
                    request_queue,
                    limits,
                    log,
                    kv_store,
                    debug,
                )

            # Mark handled (SDK might accept dict or request object)
            try:
                await request_queue.mark_request_as_handled(req)
            except Exception:
                log.debug("mark_request_as_handled failed; request may already be handled or signature differs.")
        except Exception as e:
            log.warning(f"Worker {worker_id}: error processing {url} — {e}")


# ---------------------
# Actor entry
# ---------------------
async def main():
    async with Actor:
        log = Actor.log
        input_data = await Actor.get_input() or {}

        # debug dump of input
        try:
            log.info("Actor input: " + json.dumps(input_data))
        except Exception:
            log.info("Actor input (non-jsonable): " + str(input_data))

        # collect start URLs and keyword-based searches
        raw_start = input_data.get("startUrls", []) or input_data.get("start_urls", [])
        keywords = input_data.get("keywords") or input_data.get("keyword") or input_data.get("searchKeywords")
        search_template = input_data.get("searchUrlTemplate")

        start_items = normalize_start_items(raw_start)
        if keywords:
            if isinstance(keywords, str):
                keywords = [keywords]
            for kw in keywords:
                start_items.append({"url": build_search_url_for_keyword(kw, template=search_template), "userData": {"label": LISTING_LABEL}})

        # also allow explicit productUrls in input
        for p in input_data.get("productUrls", []) or []:
            start_items.append({"url": p, "userData": {"label": PRODUCT_LABEL}})

        if not start_items:
            log.warning("No start URLs, keywords or productUrls provided — nothing to queue.")
            return

        # timeouts conversion
        timeouts = ms_timeouts_from_input(input_data.get("timeouts", {}))

        accept_language = input_data.get("acceptLanguage", "en-US")
        region = input_data.get("region", "US")
        include_creator_videos = input_data.get("includeCreatorVideos", False)
        capture_screenshots = input_data.get("captureScreenshots", False)
        notify_cfg = input_data.get("notify", {}) or input_data.get("notifyCfg", {})
        debug = bool(input_data.get("debug", False))

        # proxy
        proxy_configuration = None
        if input_data.get("useProxy", False):
            proxy_configuration = await ProxyConfiguration.create({"useApifyProxy": True})

        kv_store = await KeyValueStore.open()
        request_queue = await Actor.open_request_queue()

        # enqueue start items
        added = 0
        for item in start_items:
            url = item.get("url")
            userData = item.get("userData", {}) or {}
            if not userData.get("label"):
                userData = choose_label_for_url(url, userData)
            await request_queue.add_request({"url": url, "userData": userData})
            log.info(f"Queued start URL: {url} (label={userData.get('label')})")
            added += 1

        log.info(f"Added {added} start requests to the queue.")

        limits = LimitsTracker()

        # launch Playwright
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=Actor.config.headless, args=["--no-sandbox", "--disable-dev-shm-usage"])
            worker_count = int(input_data.get("maxConcurrency", input_data.get("concurrency", 3)))
            workers = [
                worker_loop(
                    i,
                    browser,
                    request_queue,
                    proxy_configuration,
                    accept_language,
                    region,
                    timeouts,
                    include_creator_videos,
                    capture_screenshots,
                    notify_cfg,
                    kv_store,
                    limits,
                    log,
                    debug,
                )
                for i in range(worker_count)
            ]
            await asyncio.gather(*workers)
            await browser.close()

        log.info("Run finished.")


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
import asyncio
import json
import os
from typing import Any, Dict, List, Optional, Union

from apify import Actor, ProxyConfiguration
from apify.storages import KeyValueStore

# Playwright async API
from playwright.async_api import async_playwright, Browser, Page

# Labels
PRODUCT_LABEL = "PRODUCT"
LISTING_LABEL = "LISTING"
SELLER_LABEL = "SELLER"
CATEGORY_LABEL = "CATEGORY"
KEYWORD_LABEL = "KEYWORD"

DEFAULT_TIMEOUTS = {"navigation": 30000}


class LimitsTracker:
    def __init__(self):
        self.limits = {}


# ---------------------
# Helpers
# ---------------------
def build_search_url_for_keyword(keyword: str, template: Optional[str] = None) -> str:
    """
    Build a search/listing URL from a keyword.
    Default template points to a basic TikTok search; you may change this to the exact
    TikTok Shop search URL you'd like to use.
    """
    template = template or "https://www.tiktok.com/search?q={keyword}"
    return template.format(keyword=keyword)


def normalize_start_items(raw_start_urls: Any) -> List[Dict[str, Any]]:
    """
    Accepts multiple input formats and normalizes to a list of dicts:
    - ["https://..."] -> [{"url": "..."}]
    - [{"url": "...", "userData": {...}}] -> unchanged
    - "https://..." -> [{"url": "..."}]
    """
    out: List[Dict[str, Any]] = []
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
                # Already a dict, ensure it has url
                url = item.get("url")
                if url:
                    # preserve userData or label if provided
                    ud = item.get("userData") or {}
                    if "label" in item:
                        ud["label"] = item["label"]
                    out.append({"url": url, "userData": ud})
    return out


def choose_label_for_url(url: str, explicit_userdata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Heuristic: if URL looks like a search/listing URL, label it LISTING; otherwise PRODUCT.
    If userData already contains label, respect that.
    """
    if explicit_userdata and explicit_userdata.get("label"):
        return explicit_userdata

    lower = url.lower()
    if "search" in lower or "shop" in lower or "tag" in lower or "collections" in lower:
        return {"label": LISTING_LABEL}
    # default to PRODUCT for direct product links
    return {"label": PRODUCT_LABEL}


# ---------------------
# Processing Functions (skeletons)
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
    kv_store,
    log,
):
    url = req.get("url")
    log.info(f"[PRODUCT] {url}")
    # Example: open a page and take minimal action
    context = await browser.new_context()
    page: Page = await context.new_page()
    try:
        await page.goto(url, timeout=timeouts.get("navigation", 30000))
        # TODO: add your scraping for product page here
        await asyncio.sleep(0.1)
    except Exception as e:
        log.warning(f"product task error for {url}: {e}")
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
):
    url = req.get("url")
    log.info(f"[LISTING] {url}")
    context = await browser.new_context()
    page: Page = await context.new_page()
    try:
        await page.goto(url, timeout=timeouts.get("navigation", 30000))
        # TODO: replace the selector with the actual one that yields product links on listing/search pages.
        # Example pseudo-code showing how to enqueue discovered product links:
        #
        # anchors = await page.query_selector_all("a[href*='/product/'], a.product-link")
        # for a in anchors:
        #     href = await a.get_attribute("href")
        #     if href:
        #         full = page.url.rstrip("/") + href if href.startswith("/") else href
        #         await request_queue.add_request({"url": full, "userData": {"label": PRODUCT_LABEL}})
        #
        # For now we simulate discovering nothing; implement real selectors for TikTok Shop.
        await asyncio.sleep(0.1)
    except Exception as e:
        log.warning(f"listing task error for {url}: {e}")
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
    kv_store,
    limits: LimitsTracker,
    log,
):
    log.info(f"Worker {worker_id} started")
    while True:
        req = await request_queue.fetch_next_request()
        if not req:
            log.info(f"Worker {worker_id}: no request fetched, exiting")
            break

        # depending on SDK, req might be a Request-like object or dict
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
                    req,
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
            elif label in (LISTING_LABEL, SELLER_LABEL, CATEGORY_LABEL, KEYWORD_LABEL):
                await process_listing_task(
                    browser,
                    req,
                    proxy_url,
                    accept_language,
                    region,
                    timeouts,
                    request_queue,
                    limits,
                    log,
                )
            else:
                # default
                await process_product_task(
                    browser,
                    {**(req if isinstance(req, dict) else {}), "userData": {"label": PRODUCT_LABEL}},
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

            # mark handled: SDK may accept dict or request object
            try:
                await request_queue.mark_request_as_handled(req)
            except Exception:
                # fallback: if request_queue expects a request id or dict, adjust accordingly
                log.debug("mark_request_as_handled failed; request may already be handled or method signature differs.")
        except Exception as e:
            log.warning(f"Worker {worker_id}: error processing {url} — {e}")
            # Optionally re-add to queue for retry:
            # await request_queue.add_request(req)


# ---------------------
# Actor entry
# ---------------------
async def main():
    async with Actor:
        log = Actor.log
        input_data = await Actor.get_input() or {}
        # debug: log entire input (truncated for safety)
        try:
            log.info("Actor input: " + json.dumps(input_data))
        except Exception:
            log.info("Actor input (non-jsonable): " + str(input_data))

        # Accept either startUrls or keywords
        raw_start = input_data.get("startUrls", [])
        keywords = input_data.get("keywords") or input_data.get("searchKeywords") or input_data.get("keyword")

        # If keywords provided, build search/listing URLs
        search_template = input_data.get("searchUrlTemplate")  # optional override
        start_items = normalize_start_items(raw_start)

        if keywords:
            if isinstance(keywords, str):
                keywords = [keywords]
            for kw in keywords:
                url = build_search_url_for_keyword(kw, template=search_template)
                start_items.append({"url": url, "userData": {"label": LISTING_LABEL}})

        # If nothing to start with, warn and exit
        if not start_items:
            log.warning("No start URLs or keywords provided. Nothing to queue.")
            return

        accept_language = input_data.get("acceptLanguage", "en-US")
        region = input_data.get("region", "US")
        include_creator_videos = input_data.get("includeCreatorVideos", False)
        capture_screenshots = input_data.get("captureScreenshots", False)
        notify_cfg = input_data.get("notifyCfg", {})
        timeouts = input_data.get("timeouts", DEFAULT_TIMEOUTS)

        # Proxy setup
        proxy_configuration = None
        if input_data.get("useProxy", False):
            proxy_configuration = await ProxyConfiguration.create({"useApifyProxy": True})

        kv_store = await KeyValueStore.open()
        request_queue = await Actor.open_request_queue()

        # Add normalized start items into the queue (respect label if present)
        added = 0
        for item in start_items:
            url = item.get("url")
            userData = item.get("userData", {})
            # apply heuristic label if userData empty
            if not userData.get("label"):
                userData = choose_label_for_url(url, userData)
            await request_queue.add_request({"url": url, "userData": userData})
            added += 1
            log.info(f"Queued start URL: {url} (label={userData.get('label')})")

        log.info(f"Added {added} start requests to the queue.")

        limits = LimitsTracker()

        # Launch Playwright
        log.info("Launching Playwright...")
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=Actor.config.headless, args=["--disable-gpu"])

            worker_count = int(input_data.get("concurrency", 3))
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
                )
                for i in range(worker_count)
            ]

            await asyncio.gather(*workers)
            await browser.close()

        log.info("Run finished.")


if __name__ == "__main__":
    asyncio.run(main())

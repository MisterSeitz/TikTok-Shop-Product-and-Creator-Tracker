#!/usr/bin/env python3
import asyncio
import json
import os
from typing import Any, Dict

from apify import Actor, ProxyConfiguration
from apify.storages import KeyValueStore

# Playwright async API
from playwright.async_api import async_playwright, Browser, Page

# Your constants or labels
PRODUCT_LABEL = "PRODUCT"
SELLER_LABEL = "SELLER"
CATEGORY_LABEL = "CATEGORY"
KEYWORD_LABEL = "KEYWORD"

# You can adjust this according to your needs
DEFAULT_TIMEOUTS = {"navigation": 30000}


# Track limits if needed
class LimitsTracker:
    def __init__(self):
        self.limits = {}


# ---------------------
# Processing Functions
# ---------------------

async def process_product_task(
    browser: Browser,
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
):
    """
    Example skeleton: create a new context + page for each product request,
    perform navigation / scraping, then close the context.
    """
    url = req.get("url")
    log.info(f"Processing product: {url}")

    context = await browser.new_context()
    page: Page = await context.new_page()
    try:
        # If you want to use proxy at page level, you can set it via context/userAgent or route
        # Example navigation (use your actual scraping logic here)
        await page.goto(url, timeout=timeouts.get("navigation", 30000))
        # ... extract data, push to dataset or save to kv_store ...
        await asyncio.sleep(0.1)  # simulate some I/O / scraping
    finally:
        await page.close()
        await context.close()


async def process_listing_task(
    browser: Browser,
    req,
    proxy_url,
    accept_language,
    region,
    timeouts,
    request_queue,
    limits,
    log,
):
    """
    Example skeleton for listing pages — same pattern as product task.
    Enqueue discovered product URLs into the request_queue as needed.
    """
    url = req.get("url")
    log.info(f"Processing listing: {url}")

    context = await browser.new_context()
    page: Page = await context.new_page()
    try:
        await page.goto(url, timeout=timeouts.get("navigation", 30000))
        # Example: find links and add to queue (replace with your selectors)
        # anchors = await page.query_selector_all("a.product-link")
        # for a in anchors:
        #     href = await a.get_attribute("href")
        #     if href:
        #         await request_queue.add_request({"url": href, "userData": {"label": PRODUCT_LABEL}})
        await asyncio.sleep(0.1)  # simulate some I/O / scraping
    finally:
        await page.close()
        await context.close()


# ---------------------
# Worker Loop
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
    """
    Worker fetches requests from queue and processes them. Each processed request
    is marked as handled to avoid duplicate processing.
    """
    log.info(f"Worker {worker_id} started")
    while True:
        req = await request_queue.fetch_next_request()
        if not req:
            log.info(f"Worker {worker_id}: no more requests, exiting")
            break  # no more requests right now

        label = (req.get("userData") or {}).get("label")
        proxy_url = None
        try:
            proxy_url = await proxy_configuration.new_url() if proxy_configuration else None
        except Exception:
            proxy_url = None

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
            elif label in (SELLER_LABEL, CATEGORY_LABEL, KEYWORD_LABEL):
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
                # Unknown or direct URL defaults to product
                await process_product_task(
                    browser,
                    {**req, "userData": {"label": PRODUCT_LABEL}},
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

            # Mark the request as handled in the request queue so it won't be fetched again.
            await request_queue.mark_request_as_handled(req)
        except Exception as e:
            log.warning(f"Worker {worker_id}: error processing {req.get('url')} — {e}")
            # Optionally re-add to queue for retry:
            # await request_queue.add_request(req)


# ---------------------
# Actor Entry Point
# ---------------------

async def main():
    async with Actor:
        log = Actor.log
        input_data = await Actor.get_input() or {}
        start_urls = input_data.get("startUrls", [])
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

        # Add start URLs to queue
        for url in start_urls:
            await request_queue.add_request({"url": url, "userData": {"label": PRODUCT_LABEL}})

        limits = LimitsTracker()

        # Launch Playwright and browser using playwright.async_api
        log.info("Launching Playwright...")
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=Actor.config.headless,
                args=["--disable-gpu"],
            )

            # Spawn workers
            worker_count = int(input_data.get("concurrency", 5))
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

            # Close browser when done
            await browser.close()

        log.info("All workers finished.")


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
import asyncio
import json
import os
from typing import Any, Dict

from apify import Actor, ProxyConfiguration
from apify.storages import KeyValueStore
from apify_client.storages import RequestQueue

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
    playwright,
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
    log.info(f"Processing product: {req.get('url')}")
    # your product scraping logic here
    await asyncio.sleep(0.1)  # simulate some I/O

async def process_listing_task(
    playwright,
    req,
    proxy_url,
    accept_language,
    region,
    timeouts,
    request_queue,
    limits,
    log,
):
    log.info(f"Processing listing: {req.get('url')}")
    # your listing scraping logic here
    await asyncio.sleep(0.1)  # simulate some I/O

# ---------------------
# Worker Loop
# ---------------------

async def worker_loop(
    worker_id: int,
    playwright,
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
    while True:
        req = await request_queue.fetch_next_request()
        if not req:
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
                    playwright,
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
                    playwright,
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
                    playwright,
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

            # In Python SDK there's no mark_request_handled(); fetched = handled.
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
            proxy_configuration = await ProxyConfiguration.create(
                {"useApifyProxy": True}
            )

        kv_store = await KeyValueStore.open()
        request_queue = await Actor.open_request_queue()

        # Add start URLs to queue
        for url in start_urls:
            await request_queue.add_request(
                {"url": url, "userData": {"label": PRODUCT_LABEL}}
            )

        limits = LimitsTracker()

        # Launch Playwright browser
        playwright = await Actor.new_playwright()

        # Spawn workers
        worker_count = int(input_data.get("concurrency", 5))
        workers = [
            worker_loop(
                i,
                playwright,
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

        log.info("All workers finished.")

if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
import asyncio
import json
import urllib.parse
from typing import Any, Dict, List, Optional
import re

from apify import Actor, ProxyConfiguration
from apify.storages import KeyValueStore
from playwright.async_api import async_playwright, Browser, Page, Response

# Labels
PRODUCT_LABEL = "PRODUCT"
LISTING_LABEL = "LISTING"

DEFAULT_NAVIGATION_TIMEOUT_MS = 30000
MOBILE_UA = "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
DESKTOP_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"

class LimitsTracker:
    def __init__(self):
        self.limits = {}

def slugify_tiktok_category(cat: str) -> str:
    cat = cat.lower()
    cat = cat.replace("&", "and")
    cat = re.sub(r"[^a-z0-9]+", "-", cat)
    return cat.strip("-")

# ---------------------
# Helper Functions
# ---------------------
async def safe_get_text(page: Page, selector: str) -> Optional[str]:
    element = await page.query_selector(selector)
    if element:
        return await element.text_content()
    return None

async def safe_get_attribute(page: Page, selector: str, attribute: str) -> Optional[str]:
    element = await page.query_selector(selector)
    if element:
        return await element.get_attribute(attribute)
    return None

# ---------------------
# Task Handlers
# ---------------------
async def process_listing_page(page: Page, request_queue: Any, log: Any):
    log.info("Processing listing page...")
    
    # Scroll to load more products
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
    await page.wait_for_timeout(2000)

    # Find and queue product links. You will need to inspect the TikTok page
    # to find the correct selector for product links.
    product_links = await page.query_selector_all("a[href*='/product/']")
    for link in product_links:
        href = await link.get_attribute("href")
        if href:
            product_url = page.url(href)
            if product_url.startswith("https://www.tiktok.com/product/"):
                await request_queue.add_request(
                    {"url": product_url, "userData": {"label": PRODUCT_LABEL}}
                )
                log.info(f"Queued product page: {product_url}")

async def process_product_page(page: Page, kv_store: KeyValueStore, log: Any):
    log.info("Processing product page...")
    
    # Wait for the main product information to load.
    # Replace with a more specific selector if possible.
    await page.wait_for_selector('h1', timeout=10000)

    # Extract product details
    product_data = {
        "url": page.url,
        "name": await safe_get_text(page, "h1"), # Find the correct selector for the product name
        "price": await safe_get_text(page, "span[data-e2e='product-price']"), # Find the correct selector for the price
        "description": await safe_get_text(page, "div.product-description-class"), # Find the correct selector for the description
        "seller": await safe_get_text(page, "a[href*='/shop']"), # Find the correct selector for the seller name
        "reviews_count": await safe_get_text(page, "span.reviews-count-class"), # Find the correct selector for reviews count
        "images": [],
    }

    # Extract all image URLs.
    image_elements = await page.query_selector_all("img.product-image-class") # Find the correct selector for product images
    for img in image_elements:
        src = await img.get_attribute("src")
        if src:
            product_data["images"].append(src)

    # Push the structured data to the Apify dataset
    await Actor.push_data(product_data)
    log.info(f"Pushed product data for {product_data['name']}")


async def worker_loop(
    worker_id: int,
    browser: Browser,
    request_queue: Any,
    proxy_configuration: ProxyConfiguration,
    accept_language: Optional[str],
    region: Optional[str],
    timeouts: Dict[str, Any],
    capture_screenshots: bool,
    kv_store: KeyValueStore,
    log: Any,
    debug: bool,
):
    # This part of the code is largely the same as your original file
    # and is responsible for managing the browser and requests.
    context = await browser.new_context(
        user_agent=DESKTOP_UA,
        extra_http_headers={
            "Accept-Language": accept_language,
            "X-App-Region": region,
            "Referer": "https://www.tiktok.com/"
        } if accept_language else None,
        bypass_csp=True,
    )
    if proxy_configuration and await proxy_configuration.new_proxy_url():
        await context.route("**/*", proxy_configuration.intercept_request)

    page = await context.new_page()

    while True:
        request = await request_queue.fetch_next_request()
        if not request:
            break
        
        url = request.url
        label = request.user_data.get("label")

        try:
            log.info(f"Worker {worker_id} is processing {url} (label={label})")
            
            # The navigation timeout is crucial for slow-loading pages.
            await page.goto(url, wait_until="networkidle", timeout=timeouts.get("navigation", DEFAULT_NAVIGATION_TIMEOUT_MS))
            
            # Conditionally call the appropriate handler based on the request label.
            if label == LISTING_LABEL:
                await process_listing_page(page, request_queue, log)
            elif label == PRODUCT_LABEL:
                await process_product_page(page, kv_store, log)
            else:
                log.warning(f"Unknown label: {label} for URL: {url}")
                
            await request_queue.mark_request_as_handled(request)

        except Exception as e:
            log.error(f"Failed to process {url} due to {e}")
            await request_queue.reclaim_request(request)

    await context.close()


async def main():
    async with Actor:
        Actor.log.info("Starting Apify TikTok Scraper (Playwright)...")
        input_data = await Actor.get_input() or {}
        proxy_configuration = await Actor.create_proxy_configuration()
        
        # ... (rest of the main function, setting up queues and workers)
        # This part of the code is largely the same as your original file.
        # Ensure you have your start URLs and categories correctly configured.

        # Example: Using a search query to start the crawl
        start_urls = [
            {"url": f"https://www.tiktok.com/tag/{slugify_tiktok_category('fashion & accessories')}", "userData": {"label": LISTING_LABEL}}
        ]
        
        request_queue = await Actor.open_request_queue()
        for item in start_urls:
            await request_queue.add_request(item)
            
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=Actor.config.headless,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            worker_count = int(input_data.get("maxConcurrency", 3))
            workers = [
                worker_loop(
                    i,
                    browser,
                    request_queue,
                    proxy_configuration,
                    input_data.get("acceptLanguage"),
                    input_data.get("region"),
                    input_data.get("pageTimeouts", {}),
                    input_data.get("captureScreenshots", False),
                    Actor.get_key_value_store(),
                    Actor.log,
                    input_data.get("debug", False)
                )
                for i in range(worker_count)
            ]
            await asyncio.gather(*workers)
            await browser.close()
            
        Actor.log.info("Run finished.")

if __name__ == "__main__":
    asyncio.run(main())

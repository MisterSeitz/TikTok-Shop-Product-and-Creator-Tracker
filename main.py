
#!/usr/bin/env python3
import asyncio
import json
import re
import urllib.parse
from typing import Any, Dict, List, Optional

from apify import Actor, ProxyConfiguration
from apify.storages import KeyValueStore
from playwright.async_api import async_playwright, Browser, Page, Response

PRODUCT_LABEL = "PRODUCT"
LISTING_LABEL = "LISTING"

DEFAULT_NAVIGATION_TIMEOUT_MS = 30000

MOBILE_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/16.0 Mobile/15E148 Safari/604.1"
)

DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/116.0.0.0 Safari/537.36"
)


def slugify_tiktok_category(cat: str) -> str:
    cat = cat.lower().replace("&", "and")
    return re.sub(r"[^a-z0-9]+", "-", cat).strip("-")


def build_search_url_for_keyword(
    keyword: str,
    template: Optional[str] = None
) -> str:
    template = template or "https://www.tiktok.com/tag/{keyword}"
    return template.format(
        keyword=urllib.parse.quote_plus(keyword)
    )


def normalize_start_items(raw: Any) -> List[Dict[str, Any]]:
    result = []

    if not raw:
        return result

    if isinstance(raw, str):
        raw = [raw]

    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                result.append({"url": item})
            elif isinstance(item, dict) and item.get("url"):
                user_data = dict(item.get("userData") or {})
                if item.get("label"):
                    user_data["label"] = item["label"]
                result.append({
                    "url": item["url"],
                    "userData": user_data,
                })

    return result


def choose_label_for_url(
    url: str,
    explicit_userdata: Dict[str, Any]
) -> Dict[str, Any]:
    if explicit_userdata and explicit_userdata.get("label"):
        return explicit_userdata

    parsed = urllib.parse.urlparse(url)
    path = parsed.path.lower()

    # Individual TikTok Shop product pages
    if any(x in path for x in (
        "/pdp/",
        "/product/",
        "/item/",
        "/video/",
    )):
        return {"label": PRODUCT_LABEL}

    # Discovery and category pages
    if any(x in path for x in (
        "/tag/",
        "/collections/",
        "/category/",
        "/search",
    )):
        return {"label": LISTING_LABEL}

    return {"label": PRODUCT_LABEL}


def ms_timeouts_from_input(raw: Any) -> Dict[str, int]:
    if not isinstance(raw, dict):
        return {"navigation": DEFAULT_NAVIGATION_TIMEOUT_MS}

    if "navigation" in raw:
        return {"navigation": int(raw["navigation"])}

    if "navigationTimeoutSecs" in raw:
        return {
            "navigation": int(raw["navigationTimeoutSecs"]) * 1000
        }

    return {"navigation": DEFAULT_NAVIGATION_TIMEOUT_MS}


def normalize_locale(value: str) -> str:
    """
    Convert HTTP Accept-Language values into valid
    Playwright browser locales.
    """
    first = (value or "en-GB").split(",")[0].strip()
    first = first.split(";")[0].strip()
    first = first.replace("_", "-")

    if re.fullmatch(r"[a-zA-Z]{2,3}(-[a-zA-Z]{2,4})?", first):
        return first

    return "en-GB"


async def create_context_for_req(
    browser: Browser,
    accept_language: str,
    proxy_url: Optional[str] = None,
    use_mobile: bool = True,
):
    locale = normalize_locale(accept_language)

    kwargs = {
        "user_agent": MOBILE_UA if use_mobile else DESKTOP_UA,
        "locale": locale,
        "java_script_enabled": True,
    }

    if proxy_url:
        kwargs["proxy"] = {"server": proxy_url}

    return await browser.new_context(**kwargs)


async def fetch_and_save_response_for_debug(
    page: Page,
    resp: Optional[Response],
    kv_store: KeyValueStore,
    key_prefix: str,
    log,
):
    try:
        if resp is None:
            body = await page.content()
            key = f"{key_prefix}.html"
            await kv_store.set_value(
                key, body, content_type="text/html"
            )
            log.info(f"[DEBUG] Saved {key}")
            return

        body = await resp.text()
        content_type = resp.headers.get(
            "content-type", ""
        ).lower()

        if "html" in content_type or body.lstrip().startswith("<"):
            ext, mime = ".html", "text/html"
        elif "json" in content_type or body.lstrip().startswith(("{", "[")):
            ext, mime = ".json", "application/json"
        else:
            ext, mime = ".txt", "text/plain"

        key = f"{key_prefix}{ext}"
        await kv_store.set_value(
            key, body, content_type=mime
        )
        log.info(
            f"[DEBUG] Saved {key}, HTTP {resp.status}"
        )

    except Exception as exc:
        log.warning(f"[DEBUG] Save failed: {exc}")


async def auto_scroll_page(
    page: Page,
    scroll_step: int = 1200,
    max_scrolls: int = 8,
    wait_time_ms: int = 600,
    log=None,
):
    try:
        last_height = await page.evaluate(
            "() => document.body.scrollHeight"
        )

        for _ in range(max_scrolls):
            await page.evaluate(
                "(step) => window.scrollBy(0, step)",
                scroll_step,
            )
            await page.wait_for_timeout(wait_time_ms)

            height = await page.evaluate(
                "() => document.body.scrollHeight"
            )

            if height == last_height:
                break

            last_height = height

    except Exception as exc:
        if log:
            log.warning(f"[SCROLL] {exc}")


async def process_product_task(
    browser: Browser,
    req: Dict[str, Any],
    proxy_url: Optional[str],
    accept_language: str,
    region: str,
    timeouts: Dict[str, Any],
    capture_screenshots: bool,
    kv_store: KeyValueStore,
    log,
    debug: bool = False,
):
    url = req.get("url")
    log.info(f"[PRODUCT] Processing {url}")

    context = None

    try:
        context = await create_context_for_req(
            browser, accept_language, proxy_url
        )

        page = await context.new_page()

        response = await page.goto(
            url,
            timeout=timeouts["navigation"],
            wait_until="domcontentloaded",
        )

        await page.wait_for_timeout(1500)

        title = await page.title()
        final_url = page.url
        status = response.status if response else None

        log.info(
            f"[PRODUCT] HTTP={status}, title={title}"
        )

        if debug:
            await fetch_and_save_response_for_debug(
                page,
                response,
                kv_store,
                "debug/product-" + urllib.parse.quote_plus(url),
                log,
            )

        # Save basic page information.
        # This is not a full GMV or sales extractor.
        record = {
            "url": url,
            "finalUrl": final_url,
            "title": title,
            "httpStatus": status,
            "region": region,
        }

        await Actor.push_data(record)
        log.info("[PRODUCT] Dataset record saved")

        if capture_screenshots:
            try:
                png = await page.screenshot(full_page=True)
                key = (
                    "screenshots/"
                    + urllib.parse.quote_plus(url)
                    + ".png"
                )
                await kv_store.set_value(
                    key, png, content_type="image/png"
                )
            except Exception as exc:
                log.warning(
                    f"[SCREENSHOT] Failed: {exc}"
                )

    except Exception as exc:
        log.error(f"[PRODUCT] Failed {url}: {exc}")

    finally:
        if context:
            await context.close()


async def process_listing_task(
    browser: Browser,
    req: Dict[str, Any],
    proxy_url: Optional[str],
    accept_language: str,
    region: str,
    timeouts: Dict[str, Any],
    request_queue,
    log,
    kv_store: KeyValueStore,
    debug: bool = False,
):
    url = req.get("url")
    log.info(f"[LISTING] Processing {url}")

    context = None

    try:
        context = await create_context_for_req(
            browser, accept_language, proxy_url
        )

        page = await context.new_page()

        response = await page.goto(
            url,
            timeout=timeouts["navigation"],
            wait_until="domcontentloaded",
        )

        await auto_scroll_page(page, log=log)

        if debug:
            await fetch_and_save_response_for_debug(
                page,
                response,
                kv_store,
                "debug/listing-" + urllib.parse.quote_plus(url),
                log,
            )

        hrefs = await page.locator("a[href]").evaluate_all(
            "(els) => els.map(a => a.href)"
        )

        candidates = []

        for href in hrefs:
            path = urllib.parse.urlparse(href).path.lower()

            if any(x in path for x in (
                "/pdp/",
                "/product/",
                "/item/",
                "/video/",
            )):
                candidates.append(href)

        unique = list(dict.fromkeys(candidates))

        for candidate in unique:
            await request_queue.add_request({
                "url": candidate,
                "userData": {"label": PRODUCT_LABEL},
            })

        log.info(
            f"[LISTING] Found {len(unique)} candidate links"
        )

    except Exception as exc:
        log.error(f"[LISTING] Failed {url}: {exc}")

    finally:
        if context:
            await context.close()


async def worker_loop(
    worker_id: int,
    browser: Browser,
    request_queue,
    proxy_configuration,
    accept_language: str,
    region: str,
    timeouts: Dict[str, Any],
    capture_screenshots: bool,
    kv_store: KeyValueStore,
    log,
    debug: bool,
):
    log.info(f"Worker {worker_id} started")

    while True:
        req = await request_queue.fetch_next_request()

        if not req:
            break

        label = (req.get("userData") or {}).get("label")

        proxy_url = None

        if proxy_configuration:
            try:
                proxy_url = await proxy_configuration.new_url()
            except Exception as exc:
                log.warning(f"[PROXY] {exc}")

        try:
            if label == PRODUCT_LABEL:
                await process_product_task(
                    browser,
                    req,
                    proxy_url,
                    accept_language,
                    region,
                    timeouts,
                    capture_screenshots,
                    kv_store,
                    log,
                    debug,
                )
            else:
                await process_listing_task(
                    browser,
                    req,
                    proxy_url,
                    accept_language,
                    region,
                    timeouts,
                    request_queue,
                    log,
                    kv_store,
                    debug,
                )

        finally:
            await request_queue.mark_request_as_handled(req)


async def main():
    async with Actor:
        log = Actor.log
        input_data = await Actor.get_input() or {}

        log.info(
            "Actor input: " + json.dumps(input_data)
        )

        raw_start = (
            input_data.get("startUrls")
            or input_data.get("start_urls")
            or input_data.get("productUrls")
            or []
        )

        start_items = normalize_start_items(raw_start)

        keywords = (
            input_data.get("keywords")
            or input_data.get("keyword")
            or input_data.get("searchKeywords")
            or []
        )

        if isinstance(keywords, str):
            keywords = [keywords]

        search_template = input_data.get("searchUrlTemplate")

        for keyword in keywords:
            start_items.append({
                "url": build_search_url_for_keyword(
                    keyword, search_template
                ),
                "userData": {"label": LISTING_LABEL},
            })

        for category in input_data.get(
            "tiktokCategories", []
        ):
            slug = slugify_tiktok_category(category)
            start_items.append({
                "url": f"https://www.tiktok.com/tag/{slug}",
                "userData": {"label": LISTING_LABEL},
            })

        for url in input_data.get("categoryUrls", []):
            start_items.append({
                "url": url,
                "userData": {"label": LISTING_LABEL},
            })

        if not start_items:
            log.warning(
                "No product URLs, keywords or categories provided."
            )
            return

        timeouts = ms_timeouts_from_input(
            input_data.get("timeouts", {})
        )

        accept_language = input_data.get(
            "acceptLanguage", "en-GB"
        )

        region = input_data.get("region", "GB")

        capture_screenshots = bool(
            input_data.get("captureScreenshots", False)
        )

        debug = bool(input_data.get("debug", False))

        proxy_configuration = None

        if input_data.get("useProxy", False):
            proxy_configuration = await ProxyConfiguration.create(
                {"useApifyProxy": True}
            )

        kv_store = await KeyValueStore.open()
        request_queue = await Actor.open_request_queue()

        for item in start_items:
            url = item["url"]
            user_data = item.get("userData") or {}

            if not user_data.get("label"):
                user_data = choose_label_for_url(
                    url, user_data
                )

            await request_queue.add_request({
                "url": url,
                "userData": user_data,
            })

            log.info(
                f"Queued {url} as {user_data['label']}"
            )

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )

            worker_count = max(
                1,
                min(
                    int(input_data.get(
                        "maxConcurrency", 3
                    )),
                    5,
                ),
            )

            workers = [
                worker_loop(
                    i,
                    browser,
                    request_queue,
                    proxy_configuration,
                    accept_language,
                    region,
                    timeouts,
                    capture_screenshots,
                    kv_store,
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

import asyncio
import json
import re
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urlparse

import httpx
from apify import Actor
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, TimeoutError as PwTimeoutError

# ------------ Constants and helpers ------------

PRODUCT_LABEL = "PRODUCT"
SELLER_LABEL = "SELLER"
CATEGORY_LABEL = "CATEGORY"
KEYWORD_LABEL = "KEYWORD"

IN_STOCK = "IN_STOCK"
OUT_OF_STOCK = "OUT_OF_STOCK"

DEFAULT_ACCEPT_LANGUAGE = "en-US,en;q=0.9"

# TikTok Shop URL helpers (best-effort)
def seller_shop_url(handle: str) -> str:
    handle = handle.lstrip("@")
    return f"https://www.tiktok.com/@{handle}/shop"

def keyword_search_url(keyword: str, region: str) -> str:
    # Best-effort shop search URL; TikTok frequently changes. We'll fallback to generic search with hints.
    params = {"q": keyword}
    # Attempt to force shop tab; if not supported, still returns search.
    return f"https://www.tiktok.com/search?{urlencode(params)}&t=shop&region={region}"

def normalize_url(url: str) -> str:
    return url.split("#")[0].rstrip("/")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def extract_product_id_from_url(url: str) -> Optional[str]:
    # Common patterns: /product/123456789, or query param item_id/product_id
    parsed = urlparse(url)
    path = parsed.path
    m = re.search(r"/product/(\d+)", path)
    if m:
        return m.group(1)
    q = dict([kv.split("=", 1) if "=" in kv else (kv, "") for kv in parsed.query.split("&") if kv])
    for key in ("product_id", "item_id", "id"):
        if key in q and q[key].isdigit():
            return q[key]
    # TikTok sometimes uses /shop/product/123
    m2 = re.search(r"/shop/product/(\d+)", path)
    if m2:
        return m2.group(1)
    return None


async def try_accept_cookies(page: Page) -> None:
    try:
        # Try common consent buttons
        selectors = [
            'button:has-text("Accept all")',
            'button:has-text("Accept All")',
            'button:has-text("I agree")',
            'button:has-text("Agree")',
            'text=Accept >> xpath=..',  # generic
        ]
        for sel in selectors:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                await asyncio.sleep(0.5)
                break
        # Also dismiss region prompts if present
        close_btns = [
            'button[aria-label="Close"]',
            '[data-e2e="close-button"]',
        ]
        for sel in close_btns:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click(timeout=2000)
                await asyncio.sleep(0.2)
    except Exception:
        pass


async def gentle_scroll(page: Page, max_steps: int = 12, step_px: int = 1200, delay_ms: int = 250) -> None:
    try:
        for _ in range(max_steps):
            await page.evaluate("(y) => window.scrollBy(0, y)", step_px)
            await asyncio.sleep(delay_ms / 1000)
    except Exception:
        pass


def parse_json_safe(text: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(text)
    except Exception:
        return None


async def extract_window_state(page: Page) -> Dict[str, Any]:
    # Try to fetch data structures commonly used by TikTok
    try:
        return await page.evaluate(
            """() => {
                const out = {};
                try { out.SIGI_STATE = window.SIGI_STATE || null; } catch (e) {}
                try { out.__NEXT_DATA__ = window.__NEXT_DATA__ || null; } catch (e) {}
                try { out.APOLLO_STATE = window.__APOLLO_STATE__ || null; } catch (e) {}
                try { out.__INIT_PROPS__ = window.__INIT_PROPS__ || null; } catch (e) {}
                try { out.__UNIVERSAL_DATA__ = window.__UNIVERSAL_DATA__ || null; } catch (e) {}
                try {
                    const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
                        .map(s => { try { return JSON.parse(s.textContent); } catch (e) { return null; } })
                        .filter(Boolean);
                    out.LD_JSON = scripts;
                } catch (e) {}
                return out;
            }"""
        )
    except Exception:
        return {}


def pick_first_str(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        if v and isinstance(v, str):
            return v
    return None


def to_availability(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    v = value.lower()
    if "instock" in v or "in_stock" in v or v == "in stock":
        return IN_STOCK
    if "outofstock" in v or "out_of_stock" in v or v == "out of stock":
        return OUT_OF_STOCK
    return None


def coerce_currency(code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    return code.strip().upper()


def coerce_price(p: Any) -> Optional[float]:
    try:
        if p is None:
            return None
        if isinstance(p, (int, float)):
            return float(p)
        s = str(p).strip().replace(",", "")
        # Extract first float-looking pattern
        m = re.search(r"[-+]?\d*\.?\d+", s)
        if m:
            return float(m.group(0))
    except Exception:
        return None
    return None


async def extract_product_from_structured(state: Dict[str, Any], page: Page) -> Dict[str, Any]:
    # Try LD+JSON first
    item: Dict[str, Any] = {}
    try:
        ld_items = state.get("LD_JSON") or []
        # Look for Product type
        for ld in ld_items:
            if isinstance(ld, dict) and ld.get("@type") in ("Product", ["Product"]):
                title = ld.get("name")
                description = ld.get("description")
                images = []
                if isinstance(ld.get("image"), list):
                    images = [str(x) for x in ld.get("image") if isinstance(x, (str,))]
                elif isinstance(ld.get("image"), str):
                    images = [ld.get("image")]

                offers = ld.get("offers") or {}
                price = coerce_price(offers.get("price"))
                currency = coerce_currency(offers.get("priceCurrency"))
                availability = to_availability(offers.get("availability"))
                rating_value = None
                review_count = None
                if isinstance(ld.get("aggregateRating"), dict):
                    rating_value = coerce_price(ld["aggregateRating"].get("ratingValue"))
                    review_count = int(coerce_price(ld["aggregateRating"].get("reviewCount") or 0) or 0)
                seller_name = None
                if isinstance(ld.get("brand"), dict):
                    seller_name = ld["brand"].get("name")
                seller = {"handle": None, "name": seller_name, "url": None}

                item.update({
                    "title": title,
                    "description": description,
                    "images": images,
                    "price": {"current": price, "currency": currency},
                    "availability": availability,
                    "rating": rating_value,
                    "review_count": review_count,
                    "seller": seller,
                })
                break
    except Exception:
        pass

    # Try window states for more detail (seller, id, original price, etc.)
    try:
        sigi = state.get("SIGI_STATE") or {}
        # Heuristics: search for product entities with price and title
        # TikTok state structure can vary; attempt to find candidate objects
        def search_for_product(d: Any) -> List[Dict[str, Any]]:
            found = []
            if isinstance(d, dict):
                # a product-like object has title/name and price
                keys = set(d.keys())
                if any(k in keys for k in ("title", "name")) and any(k in keys for k in ("price", "salePrice", "originPrice")):
                    found.append(d)
                for v in d.values():
                    found.extend(search_for_product(v))
            elif isinstance(d, list):
                for v in d:
                    found.extend(search_for_product(v))
            return found

        candidates = search_for_product(sigi)
        if not item.get("title") and candidates:
            cand = candidates[0]
            item["title"] = cand.get("title") or cand.get("name") or item.get("title")
            p_current = cand.get("price") or cand.get("salePrice") or cand.get("priceNow")
            p_original = cand.get("originPrice") or cand.get("originalPrice")
            currency = cand.get("currency") or item.get("price", {}).get("currency")
            price = item.get("price") or {}
            price.update({
                "current": coerce_price(p_current),
                "original": coerce_price(p_original),
                "currency": coerce_currency(currency),
            })
            item["price"] = price

        # Seller info heuristics
        if not item.get("seller"):
            # Try to find shop/seller-like objects
            def search_for_seller(d: Any) -> Optional[Dict[str, Any]]:
                if isinstance(d, dict):
                    keys = set(d.keys())
                    if "shopName" in keys or "sellerName" in keys or "nickname" in keys:
                        return {
                            "handle": d.get("uniqueId") or d.get("sellerId") or d.get("sellerUserId"),
                            "name": d.get("shopName") or d.get("sellerName") or d.get("nickname"),
                            "url": d.get("shopUrl") or None,
                        }
                    for v in d.values():
                        r = search_for_seller(v)
                        if r:
                            return r
                elif isinstance(d, list):
                    for v in d:
                        r = search_for_seller(v)
                        if r:
                            return r
                return None

            seller = search_for_seller(sigi) or {"handle": None, "name": None, "url": None}
            item["seller"] = seller

        # Images
        if not item.get("images"):
            def search_images(d: Any) -> List[str]:
                imgs = []
                if isinstance(d, dict):
                    for k, v in d.items():
                        if "img" in k.lower() or "image" in k.lower() or "cover" in k.lower():
                            if isinstance(v, str) and v.startswith("http"):
                                imgs.append(v)
                            elif isinstance(v, list):
                                for x in v:
                                    if isinstance(x, str) and x.startswith("http"):
                                        imgs.append(x)
                                    elif isinstance(x, dict):
                                        for vv in x.values():
                                            if isinstance(vv, str) and vv.startswith("http"):
                                                imgs.append(vv)
                        if isinstance(v, (dict, list)):
                            imgs.extend(search_images(v))
                elif isinstance(d, list):
                    for v in d:
                        imgs.extend(search_images(v))
                return imgs

            imgs = search_images(sigi)
            if imgs:
                item["images"] = list(dict.fromkeys(imgs))[:10]
    except Exception:
        pass

    # Fallback DOM scraping for missing fields
    try:
        if not item.get("title"):
            title_el = page.locator("h1, h2").first
            if await title_el.count() > 0:
                item["title"] = (await title_el.inner_text()).strip()
    except Exception:
        pass

    # Attempt to detect availability via text cues
    try:
        if not item.get("availability"):
            body_text = (await page.content()).lower()
            avail = None
            if "out of stock" in body_text or "sold out" in body_text:
                avail = OUT_OF_STOCK
            elif "in stock" in body_text or "available" in body_text:
                avail = IN_STOCK
            item["availability"] = avail
    except Exception:
        pass

    # Ensure price structure exists
    item.setdefault("price", {"current": None, "original": None, "currency": None})

    return item


async def extract_creator_videos(page: Page, limit: int = 6) -> List[Dict[str, Any]]:
    creators: List[Dict[str, Any]] = []
    try:
        # Find video links nearby, collect engagement if visible
        anchors = page.locator('a[href*="/video/"]')
        cnt = await anchors.count()
        seen = set()
        for i in range(min(cnt, limit * 2)):  # gather a bit extra and de-dup
            href = await anchors.nth(i).get_attribute("href")
            if not href or "/video/" not in href:
                continue
            url = href if href.startswith("http") else f"https://www.tiktok.com{href}"
            if url in seen:
                continue
            seen.add(url)
            # Try to fetch engagement text around the link
            parent = anchors.nth(i).locator("xpath=..")
            txt = ""
            try:
                txt = await parent.inner_text()
            except Exception:
                pass
            likes = None
            comments = None
            m1 = re.search(r"([\d,.]+)\s*[Ll]ike", txt)
            m2 = re.search(r"([\d,.]+)\s*[Cc]omment", txt)
            if m1:
                likes = coerce_price(m1.group(1))
            if m2:
                comments = coerce_price(m2.group(1))
            creators.append({
                "video_url": url,
                "likes": likes,
                "comments": comments,
            })
            if len(creators) >= limit:
                break
    except Exception:
        pass
    return creators


async def send_notifications(notify_cfg: Dict[str, Any], payload: Dict[str, Any], log) -> None:
    if not notify_cfg or not notify_cfg.get("enabled", True):
        return
    tasks = []
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            if notify_cfg.get("slackWebhookUrl"):
                # Slack expects {"text": "..."} or blocks; we'll send concise text summary + JSON fallback
                text = f"TikTok Shop update: {payload.get('title') or payload.get('url')} — changes: {json.dumps(payload.get('detected_changes', {}))}"
                tasks.append(client.post(notify_cfg["slackWebhookUrl"], json={"text": text}))
            if notify_cfg.get("webhookUrl"):
                tasks.append(client.post(notify_cfg["webhookUrl"], json=payload))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        log.warning(f"Notification failed: {e}")


# ------------ Playwright context management ------------

async def launch_browser_with_proxy(playwright, proxy_url: Optional[str], headless: bool = True):
    # Proxy is set on browser launch level in Playwright
    launch_args = {
        "headless": headless,
        "args": [
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ],
    }
    if proxy_url:
        launch_args["proxy"] = {"server": proxy_url}
    browser = await playwright.chromium.launch(**launch_args)
    return browser


async def new_context(browser: Browser, accept_language: str, region: str, navigation_timeout_ms: int):
    context = await browser.new_context(
        locale=accept_language.split(",")[0].strip(),
        user_agent=None,  # let Playwright pick a sane default chromium UA
        viewport={"width": 1280, "height": 1800},
    )
    # Set default timeout
    context.set_default_timeout(navigation_timeout_ms)

    # Seed localStorage, region hints
    await context.add_init_script(
        f"""
        (() => {{
            try {{
                window.localStorage.setItem('tiktok_region', '{region}');
                window.localStorage.setItem('preferred_region', '{region}');
            }} catch (e) {{}}
            Object.defineProperty(navigator, 'language', {{ get: () => '{accept_language.split(",")[0].strip()}' }});
            Object.defineProperty(navigator, 'languages', {{ get: () => {json.dumps([accept_language.split(",")[0].strip(), "en"])} }});
        }})();
        """
    )
    return context


# ------------ Request processing workers ------------

class LimitsTracker:
    def __init__(self, global_max: Optional[int], per_seller: Optional[int], per_category: Optional[int]) -> None:
        self.global_max = global_max
        self.per_seller = per_seller
        self.per_category = per_category
        self.total_products = 0
        self.seller_counts: Dict[str, int] = {}
        self.category_counts: Dict[str, int] = {}

    def can_add_product(self, source_label: str, source_key: Optional[str]) -> bool:
        if self.global_max is not None and self.total_products >= self.global_max:
            return False
        if source_label == SELLER_LABEL and self.per_seller is not None and source_key:
            if self.seller_counts.get(source_key, 0) >= self.per_seller:
                return False
        if source_label == CATEGORY_LABEL and self.per_category is not None and source_key:
            if self.category_counts.get(source_key, 0) >= self.per_category:
                return False
        return True

    def mark_product_added(self, source_label: str, source_key: Optional[str]):
        self.total_products += 1
        if source_label == SELLER_LABEL and source_key:
            self.seller_counts[source_key] = self.seller_counts.get(source_key, 0) + 1
        if source_label == CATEGORY_LABEL and source_key:
            self.category_counts[source_key] = self.category_counts.get(source_key, 0) + 1


async def discover_products_from_listing(page: Page, log, max_to_collect: Optional[int] = None) -> List[str]:
    # Try to collect product detail URLs from a seller/category/search listing
    await gentle_scroll(page, max_steps=14)
    urls: List[str] = []
    try:
        # Find product anchors that likely go to /product/
        anchors = page.locator('a[href*="/product/"], a[href*="/shop/product/"]')
        count = await anchors.count()
        for i in range(count):
            href = await anchors.nth(i).get_attribute("href")
            if not href:
                continue
            url = href if href.startswith("http") else f"https://www.tiktok.com{href}"
            url = normalize_url(url)
            if "/product/" in url or "/shop/product/" in url:
                urls.append(url)
            if max_to_collect and len(urls) >= max_to_collect:
                break
    except Exception as e:
        log.warning(f"Listing product discovery failed: {e}")
    # De-dup
    deduped = list(dict.fromkeys(urls))
    return deduped


async def process_listing_task(
    playwright,
    request: Dict[str, Any],
    proxy_url: Optional[str],
    accept_language: str,
    region: str,
    timeouts: Dict[str, Any],
    request_queue,
    limits: LimitsTracker,
    log,
):
    url = request["url"]
    label = request.get("userData", {}).get("label")
    source_key = request.get("userData", {}).get("sourceKey")  # seller handle or category URL
    navigation_timeout_ms = int((timeouts.get("navigationTimeoutSecs") or 25) * 1000)

    browser: Optional[Browser] = None
    context: Optional[BrowserContext] = None
    page: Optional[Page] = None

    try:
        browser = await launch_browser_with_proxy(playwright, proxy_url)
        context = await new_context(browser, accept_language, region, navigation_timeout_ms)
        page = await context.new_page()
        # Accept-Language header via locale; add header explicitly as well
        await page.set_extra_http_headers({"Accept-Language": accept_language})
        await page.goto(url, wait_until="domcontentloaded")
        await try_accept_cookies(page)
        await page.wait_for_load_state("networkidle", timeout=navigation_timeout_ms)
        await gentle_scroll(page, max_steps=12)

        product_urls = await discover_products_from_listing(page, log)
        if not product_urls:
            log.info(f"No product URLs discovered on listing: {url}")

        for p_url in product_urls:
            if not limits.can_add_product(label, source_key):
                break
            # Seed product request
            await request_queue.add_request(
                {"url": p_url, "uniqueKey": p_url, "userData": {"label": PRODUCT_LABEL, "region": region}}
            )
            limits.mark_product_added(label, source_key)

        log.info(f"Discovered {len(product_urls)} product URLs from {label or 'LISTING'}: {url}")
    except PwTimeoutError:
        log.warning(f"Timeout during listing processing: {url}")
    except Exception as e:
        log.warning(f"Error processing listing {url}: {e}")
    finally:
        try:
            if page:
                await page.close()
        except Exception:
            pass
        try:
            if context:
                await context.close()
        except Exception:
            pass
        try:
            if browser:
                await browser.close()
        except Exception:
            pass


async def process_product_task(
    playwright,
    request: Dict[str, Any],
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
    url = request["url"]
    navigation_timeout_ms = int((timeouts.get("navigationTimeoutSecs") or 25) * 1000)
    product_id = extract_product_id_from_url(url)

    browser: Optional[Browser] = None
    context: Optional[BrowserContext] = None
    page: Optional[Page] = None
    screenshot_key = None

    try:
        browser = await launch_browser_with_proxy(playwright, proxy_url)
        context = await new_context(browser, accept_language, region, navigation_timeout_ms)
        page = await context.new_page()
        await page.set_extra_http_headers({"Accept-Language": accept_language})
        await page.goto(url, wait_until="domcontentloaded")
        await try_accept_cookies(page)
        await page.wait_for_load_state("networkidle", timeout=navigation_timeout_ms)
        await gentle_scroll(page, max_steps=10)

        state = await extract_window_state(page)
        product_data = await extract_product_from_structured(state, page)

        if include_creator_videos:
            creators = await extract_creator_videos(page)
        else:
            creators = []

        if capture_screenshots:
            # Save screenshot to KV and keep the key for dataset
            try:
                b = await page.screenshot(full_page=True, type="png")
                # fallback product id
                ss_pid = product_id or re.sub(r"[^a-zA-Z0-9]+", "_", urlparse(url).path.strip("/"))[:80] or "unknown"
                screenshot_key = f"screenshot_{ss_pid}.png"
                await Actor.set_value(screenshot_key, b, content_type="image/png")
            except Exception as e:
                log.warning(f"Screenshot failed for {url}: {e}")

        # Build result record
        pid = product_id
        # If still None, try to locate from structured state id
        if not pid:
            # Heuristic: find numeric id in state
            def find_id(d: Any) -> Optional[str]:
                if isinstance(d, dict):
                    for k, v in d.items():
                        if k in ("productId", "itemId", "id") and isinstance(v, (str, int)):
                            s = str(v)
                            if s.isdigit():
                                return s
                        r = find_id(v)
                        if r:
                            return r
                elif isinstance(d, list):
                    for v in d:
                        r = find_id(v)
                        if r:
                            return r
                return None

            pid = find_id(state) or None

        # Final fallback: hash-like slug from URL path
        if not pid:
            pid = re.sub(r"[^a-zA-Z0-9]+", "_", urlparse(url).path.strip("/"))[:80] or "unknown"

        result: Dict[str, Any] = {
            "product_id": pid,
            "url": normalize_url(url),
            "region": region,
            "captured_at": now_iso(),
            "title": product_data.get("title"),
            "description": product_data.get("description"),
            "seller": {
                "handle": product_data.get("seller", {}).get("handle"),
                "name": product_data.get("seller", {}).get("name"),
                "url": product_data.get("seller", {}).get("url"),
            },
            "price": {
                "current": product_data.get("price", {}).get("current"),
                "original": product_data.get("price", {}).get("original"),
                "currency": product_data.get("price", {}).get("currency"),
            },
            "availability": product_data.get("availability"),
            "rating": product_data.get("rating"),
            "review_count": product_data.get("review_count"),
            "images": product_data.get("images") or [],
            "creators": creators,
            "screenshot_key": screenshot_key,
            "detected_changes": {},
        }

        # Change detection
        snapshot_key = f"product_{pid}"
        previous = await kv_store.get_value(snapshot_key)  # returns Python type when using SDK
        detected_changes: Dict[str, Any] = {}
        if previous is None:
            detected_changes["first_seen"] = True
        else:
            prev_price = None
            prev_avail = None
            try:
                prev_price = previous.get("price", {}).get("current")
                prev_avail = previous.get("availability")
            except Exception:
                pass

            cur_price = result["price"]["current"]
            cur_avail = result["availability"]
            if cur_price is not None and prev_price is not None and cur_price != prev_price:
                detected_changes["price"] = {"from": prev_price, "to": cur_price}
            elif prev_price is None and cur_price is not None:
                detected_changes["price"] = {"from": None, "to": cur_price}

            if cur_avail != prev_avail:
                detected_changes["availability"] = {"from": prev_avail, "to": cur_avail}

        result["detected_changes"] = detected_changes

        # Store snapshot for next runs (store a compact snapshot)
        snapshot = {
            "product_id": pid,
            "url": result["url"],
            "title": result["title"],
            "price": {"current": result["price"]["current"], "currency": result["price"]["currency"]},
            "availability": result["availability"],
            "last_seen_at": result["captured_at"],
        }
        await Actor.set_value(snapshot_key, snapshot)

        # Push to dataset
        await Actor.push_data(result)

        # Notifications
        if notify_cfg and notify_cfg.get("enabled", True):
            should_notify = True
            only_on_change = notify_cfg.get("onlyOnChange", True)
            if only_on_change and not any(k in detected_changes for k in ("price", "availability", "first_seen")):
                should_notify = False
            if should_notify:
                await send_notifications(notify_cfg, result, log)

        log.info(f"Processed product: {result['title'] or result['url']}")
    except PwTimeoutError:
        log.warning(f"Timeout while processing product: {url}")
    except Exception as e:
        log.exception(f"Error processing product {url}: {e}")
    finally:
        try:
            if page:
                await page.close()
        except Exception:
            pass
        try:
            if context:
                await context.close()
        except Exception:
            pass
        try:
            if browser:
                await browser.close()
        except Exception:
            pass


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
            # No more requests available right now; break to let others finish
            break

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


async def main() -> None:
    async with Actor:
        log = Actor.log

        # Input parsing and defaults
        inp = await Actor.get_input() or {}

        product_urls: List[str] = [normalize_url(u) for u in (inp.get("productUrls") or []) if isinstance(u, str) and u.strip()]
        seller_handles: List[str] = [s for s in (inp.get("sellerHandles") or []) if isinstance(s, str) and s.strip()]
        keywords: List[str] = [k for k in (inp.get("keywords") or []) if isinstance(k, str) and k.strip()]
        category_urls: List[str] = [normalize_url(u) for u in (inp.get("categoryUrls") or []) if isinstance(u, str) and u.strip()]

        region: str = inp.get("region") or "US"
        accept_language: str = inp.get("acceptLanguage") or DEFAULT_ACCEPT_LANGUAGE

        limits_cfg = inp.get("limits") or {}
        limits = LimitsTracker(
            global_max=limits_cfg.get("maxProducts"),
            per_seller=limits_cfg.get("maxProductsPerSeller"),
            per_category=limits_cfg.get("maxProductsPerCategory"),
        )

        max_concurrency = int(inp.get("maxConcurrency") or 5)
        timeouts = inp.get("timeouts") or {"navigationTimeoutSecs": 25, "requestTimeoutSecs": 30}
        include_creator_videos = bool(inp.get("includeCreatorVideos", True))
        capture_screenshots = bool(inp.get("captureScreenshots", False))
        notify_cfg = inp.get("notify") or {"enabled": True, "onlyOnChange": True}
        debug = bool(inp.get("debug", False))

        if debug:
            log.set_level("DEBUG")

        # Storages and proxy
        request_queue = await Actor.open_request_queue()
        kv_store = await Actor.open_key_value_store()  # default
        proxy_configuration = None
        if inp.get("proxyConfiguration"):
            proxy_configuration = await Actor.create_proxy_configuration(inp["proxyConfiguration"])

        # Seed tasks with labels
        seeded = 0

        for url in product_urls:
            await request_queue.add_request({"url": url, "uniqueKey": url, "userData": {"label": PRODUCT_LABEL, "region": region}})
            seeded += 1

        for handle in seller_handles:
            url = seller_shop_url(handle)
            await request_queue.add_request({"url": url, "uniqueKey": url, "userData": {"label": SELLER_LABEL, "sourceKey": handle, "region": region}})
            seeded += 1

        for kw in keywords:
            url = keyword_search_url(kw, region)
            await request_queue.add_request({"url": url, "uniqueKey": url, "userData": {"label": KEYWORD_LABEL, "sourceKey": kw, "region": region}})
            seeded += 1

        for url in category_urls:
            await request_queue.add_request({"url": url, "uniqueKey": url, "userData": {"label": CATEGORY_LABEL, "sourceKey": url, "region": region}})
            seeded += 1

        if seeded == 0:
            log.warning("No seeds provided (productUrls, sellerHandles, keywords, categoryUrls). Exiting.")
            return

        log.info(f"Seeded {seeded} start requests")

        # Start Playwright and workers
        async with async_playwright() as playwright:
            # Run worker tasks
            sem = asyncio.Semaphore(max_concurrency)

            async def run_worker(idx: int):
                async with sem:
                    await worker_loop(
                        worker_id=idx,
                        playwright=playwright,
                        request_queue=request_queue,
                        proxy_configuration=proxy_configuration,
                        accept_language=accept_language,
                        region=region,
                        timeouts=timeouts,
                        include_creator_videos=include_creator_videos,
                        capture_screenshots=capture_screenshots,
                        notify_cfg=notify_cfg,
                        kv_store=kv_store,
                        limits=limits,
                        log=log,
                    )

            workers = [asyncio.create_task(run_worker(i)) for i in range(max_concurrency)]
            await asyncio.gather(*workers)

        log.info("Actor run finished.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        print("Fatal error in actor:", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

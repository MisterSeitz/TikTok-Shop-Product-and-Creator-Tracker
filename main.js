'use strict';

const { Actor } = require('apify');
const { PuppeteerCrawler, log, sleep } = require('crawlee');
const { gotScraping } = require('got-scraping');
const crypto = require('crypto');

const DEFAULT_MAX_ITEMS = 1000;
const DEFAULT_CONCURRENCY = 5;

// Helpers
const nowIso = () => new Date().toISOString();

const sha1 = (s) => crypto.createHash('sha1').update(String(s)).digest('hex');

const waitFor = (ms) => new Promise((res) => setTimeout(res, ms));

/**
 * Attempt to click cookie/consent prompts and region popups.
 */
async function handleConsent(page) {
    try {
        // A few common selectors/labels
        const buttons = [
            'button:has-text("Accept")',
            'button:has-text("I agree")',
            'button:has-text("Agree")',
            'button:has-text("Allow")',
            '[data-e2e="cookie-banner-accept"]',
            'button[data-cookie="accept"]',
        ];
        for (const sel of buttons) {
            const btn = await page.$(sel);
            if (btn) {
                await btn.click({ delay: 50 });
                await page.waitForTimeout(400);
            }
        }
    } catch {
        // noop
    }
}

/**
 * Light auto scroll to load lazy content.
 */
async function lightScroll(page, steps = 6, stepPx = 800, delayMs = 350) {
    for (let i = 0; i < steps; i++) {
        try {
            await page.evaluate((px) => window.scrollBy(0, px), stepPx);
            await page.waitForTimeout(delayMs);
        } catch {
            break;
        }
    }
}

/**
 * Extract product data using structured data first and DOM fallbacks.
 */
async function extractProduct(page, region, requestUrl) {
    const data = await page.evaluate(() => {
        const out = {
            product_id: null,
            title: null,
            description: null,
            seller: { handle: null, name: null, url: null },
            price: { current: null, original: null, currency: null },
            availability: null,
            rating: null,
            review_count: null,
            images: [],
            creators: [],
        };

        const safeJsonParse = (s) => {
            try {
                return JSON.parse(s);
            } catch {
                return null;
            }
        };

        // 1) JSON-LD Product
        const ldNodes = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
        let productLD = null;
        for (const s of ldNodes) {
            const j = safeJsonParse(s.textContent || '');
            if (!j) continue;
            const candidates = Array.isArray(j) ? j : [j];
            for (const item of candidates) {
                if (!item || typeof item !== 'object') continue;
                if ((item['@type'] === 'Product') || (Array.isArray(item['@type']) && item['@type'].includes('Product'))) {
                    productLD = item;
                    break;
                }
            }
            if (productLD) break;
        }

        if (productLD) {
            out.title = out.title || productLD.name || null;
            out.description = out.description || productLD.description || null;

            const offers = productLD.offers || productLD.aggregateOffer || null;
            if (offers) {
                if (Array.isArray(offers)) {
                    const primary = offers[0];
                    out.price.current = Number(primary.price) || null;
                    out.price.currency = primary.priceCurrency || null;
                    if (primary.priceSpecification && primary.priceSpecification.price) {
                        out.price.original = Number(primary.priceSpecification.price) || null;
                    } else if (primary.highPrice && primary.lowPrice && Number(primary.highPrice) !== Number(primary.lowPrice)) {
                        out.price.original = Number(primary.highPrice);
                    }
                    const avail = primary.availability || primary.availabilityStarts || null;
                    if (typeof avail === 'string') {
                        if (avail.toLowerCase().includes('instock')) out.availability = 'IN_STOCK';
                        else if (avail.toLowerCase().includes('outofstock')) out.availability = 'OUT_OF_STOCK';
                    }
                } else {
                    out.price.current = Number(offers.price) || null;
                    out.price.currency = offers.priceCurrency || null;
                    if (offers.priceSpecification && offers.priceSpecification.price) {
                        out.price.original = Number(offers.priceSpecification.price) || null;
                    } else if (offers.highPrice && offers.lowPrice && Number(offers.highPrice) !== Number(offers.lowPrice)) {
                        out.price.original = Number(offers.highPrice);
                    }
                    const avail = offers.availability || offers.availabilityStarts || null;
                    if (typeof avail === 'string') {
                        if (avail.toLowerCase().includes('instock')) out.availability = 'IN_STOCK';
                        else if (avail.toLowerCase().includes('outofstock')) out.availability = 'OUT_OF_STOCK';
                    }
                }
            }

            if (productLD.image) {
                if (Array.isArray(productLD.image)) out.images = productLD.image;
                else if (typeof productLD.image === 'string') out.images = [productLD.image];
            }
            if (productLD.brand) {
                if (typeof productLD.brand === 'string') {
                    out.seller.name = productLD.brand;
                } else if (productLD.brand && productLD.brand.name) {
                    out.seller.name = productLD.brand.name;
                }
            }
            if (productLD.seller) {
                if (typeof productLD.seller === 'string') {
                    out.seller.name = out.seller.name || productLD.seller;
                } else if (productLD.seller && productLD.seller.name) {
                    out.seller.name = out.seller.name || productLD.seller.name;
                }
            }
            if (productLD.aggregateRating) {
                out.rating = Number(productLD.aggregateRating.ratingValue) || null;
                out.review_count = Number(productLD.aggregateRating.reviewCount || productLD.aggregateRating.ratingCount) || null;
            }
            if (productLD.sku) out.product_id = String(productLD.sku);
            if (!out.product_id && productLD.productID) out.product_id = String(productLD.productID);
        }

        // 2) NEXT_DATA (Next.js)
        const nextScript = document.querySelector('#__NEXT_DATA__');
        const nextData = nextScript ? safeJsonParse(nextScript.textContent || '') : (window.__NEXT_DATA__ || null);
        if (nextData && nextData.props) {
            const nd = JSON.stringify(nextData);
            // Try finding productId patterns in serialized data
            const re = /"productId"\s*:\s*"([^"]+)"/i;
            const m = nd.match(re);
            if (m && m[1]) {
                out.product_id = out.product_id || m[1];
            }
            // Try basic fields
            const titleMatch = nd.match(/"title"\s*:\s*"([^"]{3,200})"/i);
            if (titleMatch && titleMatch[1] && !out.title) out.title = titleMatch[1];
        }

        // 3) Apollo or SIGI_STATE fallback
        const apollo = window.__APOLLO_STATE__ || null;
        if (apollo && typeof apollo === 'object') {
            // Heuristic: search for product-like entries
            for (const [k, v] of Object.entries(apollo)) {
                if (v && typeof v === 'object') {
                    if (v.__typename && /Product/i.test(v.__typename)) {
                        out.title = out.title || v.title || v.name || null;
                        out.description = out.description || v.description || null;
                        out.product_id = out.product_id || v.id || v.productId || null;
                        if (v.price) {
                            out.price.current = out.price.current || Number(v.price.current || v.price) || out.price.current;
                            out.price.original = out.price.original || Number(v.price.original) || out.price.original;
                            out.price.currency = out.price.currency || v.price.currency || out.price.currency;
                        }
                        if (v.seller || v.shop) {
                            const sellerObj = v.seller || v.shop;
                            out.seller.name = out.seller.name || sellerObj.name || null;
                            out.seller.handle = out.seller.handle || sellerObj.uniqueId || sellerObj.handle || null;
                            out.seller.url = out.seller.url || sellerObj.url || null;
                        }
                        if (Array.isArray(v.images) && v.images.length) {
                            out.images = out.images.length ? out.images : v.images.map((i) => (i.url || i.src || i));
                        }
                        if (v.stockStatus) {
                            if (/out/i.test(v.stockStatus)) out.availability = 'OUT_OF_STOCK';
                            else if (/in/i.test(v.stockStatus)) out.availability = 'IN_STOCK';
                        }
                    }
                }
            }
        }

        const sigi = window.SIGI_STATE || null;
        if (sigi && typeof sigi === 'object') {
            // Attempt to pull seller and product if present
            // Often used for video pages; product info may be nested
            try {
                const s = JSON.stringify(sigi);
                const m = s.match(/"productId"\s*:\s*"([^"]+)"/i);
                if (m && m[1]) out.product_id = out.product_id || m[1];
            } catch {
                // ignore
            }
        }

        // 4) DOM fallbacks
        if (!out.title) {
            const h1 = document.querySelector('h1');
            out.title = h1 ? h1.textContent.trim() : null;
        }
        if (!out.description) {
            const desc = document.querySelector('[data-e2e="product-desc"], .product-description, [itemprop="description"]');
            if (desc) out.description = desc.textContent.trim();
        }
        if (!out.images.length) {
            const imgs = Array.from(document.querySelectorAll('img[src]'))
                .map((el) => el.getAttribute('src'))
                .filter((u) => u && !u.startsWith('data:'));
            out.images = Array.from(new Set(imgs)).slice(0, 10);
        }
        if (!out.availability) {
            const bodyText = document.body.innerText || '';
            if (/out of stock/i.test(bodyText)) out.availability = 'OUT_OF_STOCK';
            else if (/in stock|available/i.test(bodyText)) out.availability = 'IN_STOCK';
        }
        // Attempt to detect seller handle from profile link
        if (!out.seller.handle) {
            const sellerLink = document.querySelector('a[href*="/@"]');
            if (sellerLink) {
                const href = sellerLink.getAttribute('href') || '';
                const m = href.match(/\/@([^\/\?\#]+)/);
                if (m && m[1]) {
                    out.seller.handle = m[1];
                    out.seller.url = out.seller.url || (href.startsWith('http') ? href : (location.origin + href));
                }
            }
        }

        // 5) Creators linked videos on product page
        try {
            const videoAnchors = Array.from(document.querySelectorAll('a[href*="/video/"]'));
            const creators = [];
            for (const a of videoAnchors) {
                const href = a.getAttribute('href') || '';
                const abs = href.startsWith('http') ? href : (location.origin + href);
                const m = abs.match(/\/@([^\/]+)\/video\/(\d+)/);
                if (!m) continue;
                const creator = '@' + m[1];
                creators.push({
                    creator,
                    video_url: abs,
                    likes: null,
                    comments: null,
                    shares: null,
                });
            }
            // Dedupe by video URL
            const seen = new Set();
            out.creators = creators.filter((c) => {
                if (seen.has(c.video_url)) return false;
                seen.add(c.video_url);
                return true;
            }).slice(0, 10);
        } catch {
            // ignore
        }

        return out;
    });

    // Normalize product_id
    let productId = data.product_id;
    if (!productId) {
        // Fallback: derive from URL
        const m = String(requestUrl).match(/product\/([^\/\?\#]+)/i);
        productId = m && m[1] ? m[1] : sha1(requestUrl).slice(0, 16);
    }

    // Final shape
    return {
        product_id: String(productId),
        url: requestUrl,
        region: region || null,
        captured_at: nowIso(),
        title: data.title || null,
        description: data.description || null,
        seller: {
            handle: data.seller?.handle || null,
            name: data.seller?.name || null,
            url: data.seller?.url || null,
        },
        price: {
            current: data.price?.current != null ? Number(data.price.current) : null,
            original: data.price?.original != null ? Number(data.price.original) : null,
            currency: data.price?.currency || null,
        },
        availability: data.availability || null,
        rating: data.rating != null ? Number(data.rating) : null,
        review_count: data.review_count != null ? Number(data.review_count) : null,
        images: Array.isArray(data.images) ? data.images.slice(0, 20) : [],
        creators: Array.isArray(data.creators) ? data.creators : [],
        screenshot_key: null,
        detected_changes: {},
    };
}

/**
 * Diffing function to detect price and availability changes.
 */
function detectChanges(prev, curr) {
    const changes = {};
    if (!prev) {
        changes.first_seen = true;
        return changes;
    }

    // Price change
    const prevPrice = prev.price?.current ?? null;
    const currPrice = curr.price?.current ?? null;
    if (prevPrice !== currPrice && currPrice != null) {
        changes.price = { from: prevPrice, to: currPrice };
    }

    // Availability change
    const prevAvail = prev.availability ?? null;
    const currAvail = curr.availability ?? null;
    if (prevAvail !== currAvail && currAvail != null) {
        changes.availability = { from: prevAvail, to: currAvail };
    }

    return changes;
}

/**
 * Send notification to Slack/webhook if configured.
 */
async function sendNotifications(notifyCfg, item) {
    if (!notifyCfg) return;
    const { webhookUrl, slackWebhookUrl } = notifyCfg || {};
    const hasChange = item.detected_changes && (item.detected_changes.price || item.detected_changes.availability || item.detected_changes.first_seen);
    if (!hasChange) return;

    const summary = [
        `TikTok Shop product update`,
        `Title: ${item.title || 'N/A'}`,
        `ID: ${item.product_id}`,
        `URL: ${item.url}`,
        `Region: ${item.region || 'N/A'}`,
        item.price?.currency ? `Price: ${item.price.current} ${item.price.currency}` : `Price: ${item.price.current ?? 'N/A'}`,
        `Availability: ${item.availability || 'N/A'}`,
        item.detected_changes.price ? `Price changed: ${item.detected_changes.price.from} -> ${item.detected_changes.price.to}` : null,
        item.detected_changes.availability ? `Availability changed: ${item.detected_changes.availability.from} -> ${item.detected_changes.availability.to}` : null,
        item.detected_changes.first_seen ? `First seen: ${item.captured_at}` : null,
    ].filter(Boolean).join('\n');

    try {
        if (webhookUrl) {
            await gotScraping({
                url: webhookUrl,
                method: 'POST',
                timeout: { request: 15000 },
                headers: { 'content-type': 'application/json' },
                json: { event: 'tiktok_shop_product_update', payload: item },
            });
        }
    } catch (e) {
        log.warning(`Webhook failed: ${e.message}`);
    }

    try {
        if (slackWebhookUrl) {
            await gotScraping({
                url: slackWebhookUrl,
                method: 'POST',
                timeout: { request: 15000 },
                headers: { 'content-type': 'application/json' },
                json: { text: summary },
            });
        }
    } catch (e) {
        log.warning(`Slack webhook failed: ${e.message}`);
    }
}

/**
 * Extract product links from listing/search/seller pages.
 */
async function collectProductLinks(page, maxLinks = 200) {
    // Try to scroll a bit to load more products
    await lightScroll(page, 8, 900, 400);

    const links = await page.evaluate((limit) => {
        const hrefs = Array.from(document.querySelectorAll('a[href]'))
            .map((a) => a.getAttribute('href'))
            .filter(Boolean);

        const productLike = [];
        for (const href of hrefs) {
            const abs = href.startsWith('http') ? href : (location.origin + href);
            // Heuristic for TikTok Shop product URLs:
            // - shop.tiktok.com/product/...
            // - tiktok.com/@seller/...?productId=... or /product/...
            if (/shop\.tiktok\.com\/product\//i.test(abs) || /\/product\/[A-Za-z0-9\-\_]+/i.test(abs)) {
                productLike.push(abs.split('?')[0]);
            }
        }
        return Array.from(new Set(productLike)).slice(0, limit);
    }, maxLinks);

    return links;
}

/**
 * Seed queue with URLs by label.
 */
async function enqueueMany(queue, urls, label, extra = {}) {
    if (!Array.isArray(urls)) return 0;
    let count = 0;
    for (const url of urls) {
        if (!url) continue;
        await queue.addRequest({
            url,
            userData: { label, ...extra },
        });
        count += 1;
    }
    return count;
}

Actor.main(async () => {
    const input = await Actor.getInput() || {};

    const {
        productUrls = [],
        sellerHandles = [],
        keywords = [],
        categoryUrls = [],
        region = 'US',
        maxItems = DEFAULT_MAX_ITEMS,
        captureScreenshots = false,
        notify = null,
        proxyConfiguration: proxyConfigInput = null,
        maxConcurrency = DEFAULT_CONCURRENCY,
        navigationTimeoutSecs = 45,
        requestHandlerTimeoutSecs = 90,
        debug = false,
    } = input;

    if (debug) log.setLevel(log.LEVELS.DEBUG);

    const proxyConfiguration = await Actor.createProxyConfiguration(proxyConfigInput || {});
    const requestQueue = await Actor.openRequestQueue();
    const store = await Actor.openKeyValueStore('tiktok-shop-tracker');

    // Seed product URLs directly
    await enqueueMany(requestQueue, productUrls, 'PRODUCT');

    // Seed seller handles -> try seller shop pages
    if (Array.isArray(sellerHandles) && sellerHandles.length) {
        const sellerUrls = sellerHandles.map((h) => `https://www.tiktok.com/@${h}/shop`);
        await enqueueMany(requestQueue, sellerUrls, 'SELLER_SHOP', { handle: null });
    }

    // Seed category URLs as listing pages
    await enqueueMany(requestQueue, categoryUrls, 'CATEGORY');

    // Seed keyword search pages (best-effort)
    if (Array.isArray(keywords) && keywords.length) {
        const searchUrls = keywords.map((k) => `https://shop.tiktok.com/search?q=${encodeURIComponent(k)}`);
        await enqueueMany(requestQueue, searchUrls, 'SEARCH');
    }

    let totalPushed = 0;
    const seenProductIds = new Set();

    const crawler = new PuppeteerCrawler({
        requestQueue,
        proxyConfiguration,
        maxConcurrency,
        requestHandlerTimeoutSecs,
        // Sessions help keep cookies/consent across requests
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: Math.max(10, maxConcurrency * 2),
        },
        launchContext: {
            launchOptions: {
                headless: true,
                args: [
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                ],
            },
        },
        preNavigationHooks: [
            async ({ page, request, session }) => {
                // Region and language hints
                const acceptLanguage = region ? `${region.toLowerCase()}-${region.toUpperCase()},en;q=0.9` : 'en-US,en;q=0.9';
                await page.setExtraHTTPHeaders({
                    'Accept-Language': acceptLanguage,
                });

                await page.evaluateOnNewDocument((r) => {
                    try {
                        localStorage.setItem('tt-shop-region', r);
                        localStorage.setItem('preferred_region', r);
                    } catch {}
                }, region);

                // Some pages need a slightly longer default nav timeout
                page.setDefaultNavigationTimeout(navigationTimeoutSecs * 1000);
                page.setDefaultTimeout(Math.max(navigationTimeoutSecs, requestHandlerTimeoutSecs) * 1000);
            },
        ],
        postNavigationHooks: [
            async ({ page }) => {
                await handleConsent(page);
            },
        ],
        async requestHandler({ request, page, enqueueLinks, crawler }) {
            const { label } = request.userData;
            log.debug(`Handling ${label} -> ${request.url}`);

            if (label === 'PRODUCT') {
                await handleConsent(page);
                await lightScroll(page, 6, 800, 300);

                // Extract product data
                const product = await extractProduct(page, region, request.url);

                // Optional screenshot
                if (captureScreenshots) {
                    try {
                        const key = `screenshot_${product.product_id}_${Date.now()}`;
                        const buf = await page.screenshot({ fullPage: true });
                        await store.setValue(key, buf, { contentType: 'image/png' });
                        product.screenshot_key = key;
                    } catch (e) {
                        log.debug(`Screenshot failed for ${product.product_id}: ${e.message}`);
                    }
                }

                // Change detection
                const snapshotKey = `product_${product.product_id}`;
                const prev = await store.getValue(snapshotKey);
                const detected_changes = detectChanges(prev, product);
                product.detected_changes = detected_changes;

                // Save snapshot
                await store.setValue(snapshotKey, product);

                // Push dataset item
                await Actor.pushData(product);
                totalPushed += 1;

                // Send notifications if configured and changed
                await sendNotifications(notify, product);

                if (totalPushed >= maxItems) {
                    log.info(`Reached maxItems=${maxItems}, shutting down gracefully.`);
                    await crawler.teardown();
                }
                return;
            }

            // Listing-like pages: seller shop, category, search
            if (['SELLER_SHOP', 'CATEGORY', 'SEARCH'].includes(label)) {
                await handleConsent(page);
                await lightScroll(page, 10, 1000, 350);

                const links = await collectProductLinks(page, 200);
                let enqueued = 0;
                for (const url of links) {
                    // Avoid refetching if already seen (by id heuristic)
                    const pidGuess = (url.match(/product\/([^\/\?\#]+)/i) || [])[1];
                    const key = pidGuess ? pidGuess : sha1(url).slice(0, 16);
                    if (seenProductIds.has(key)) continue;
                    seenProductIds.add(key);

                    await requestQueue.addRequest({
                        url,
                        userData: { label: 'PRODUCT' },
                    });
                    enqueued += 1;
                }
                log.info(`Discovered ${enqueued} product links on ${label} page`);

                return;
            }

            // Fallback: try to treat as listing
            await handleConsent(page);
            const links = await collectProductLinks(page, 50);
            for (const url of links) {
                await requestQueue.addRequest({ url, userData: { label: 'PRODUCT' } });
            }
        },
        async failedRequestHandler({ request }) {
            log.warning(`Request failed ${request.url}`);
        },
    });

    // Start
    await crawler.run();

    log.info(`Done. Items pushed: ${totalPushed}`);
});


File: package.json
--------------------------------
{
  "name": "tiktok-shop-product-tracker",
  "version": "1.0.0",
  "description": "Apify actor that tracks TikTok Shop products with price/stock/rating changes and optional notifications.",
  "type": "commonjs",
  "main": "main.js",
  "scripts": {
    "start": "node main.js",
    "test-run": "APIFY_LOCAL_STORAGE_DIR=./apify_storage node main.js"
  },
  "author": "Your Name",
  "license": "MIT",
  "engines": {
    "node": ">=20"
  },
  "dependencies": {
    "apify": "^3.1.9",
    "crawlee": "^3.9.2",
    "got-scraping": "^4.0.3"
  }
}

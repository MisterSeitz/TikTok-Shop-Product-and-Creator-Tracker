const Apify = require('apify');
const crypto = require('crypto');

const { log, requestAsBrowser, sleep } = Apify.utils;

function sha1(str) {
    return crypto.createHash('sha1').update(String(str)).digest('hex');
}

function toNumberSafe(val) {
    if (val == null) return null;
    const cleaned = String(val).replace(/[^\d.,-]/g, '');
    const hasComma = cleaned.includes(',');
    const hasDot = cleaned.includes('.');
    let normalized = cleaned;

    if (hasComma && hasDot) {
        // Assume comma is thousands separator: remove commas
        normalized = cleaned.replace(/,/g, '');
    } else if (hasComma && !hasDot) {
        // Assume comma is decimal separator
        normalized = cleaned.replace(',', '.');
    }
    const n = Number(normalized);
    return Number.isFinite(n) ? n : null;
}

function pickCurrency(text) {
    if (!text) return null;
    if (/[€]/.test(text)) return 'EUR';
    if (/[£]/.test(text)) return 'GBP';
    if (/[$]/.test(text)) return 'USD';
    if (/\bIDR|\bRp/.test(text)) return 'IDR';
    if (/\bTHB/.test(text)) return 'THB';
    if (/\bPHP/.test(text)) return 'PHP';
    if (/\bVND/.test(text)) return 'VND';
    if (/\bSGD/.test(text)) return 'SGD';
    if (/\bMYR/.test(text)) return 'MYR';
    return null;
}

function normalizeUrl(url) {
    try {
        const u = new URL(url);
        u.search = '';
        u.hash = '';
        return u.toString();
    } catch {
        return url;
    }
}

async function notifyAll({ slackWebhookUrl, webhookUrl }, payload) {
    try {
        if (slackWebhookUrl) {
            const text = `TikTok Shop alert
• ${payload.event} for ${payload.product?.title || payload.product?.product_id || 'product'}
• Price: ${payload.product?.price?.current ?? 'n/a'} ${payload.product?.price?.currency ?? ''}
• Stock: ${payload.product?.availability ?? 'n/a'}
• URL: ${payload.product?.url}`;
            await requestAsBrowser({
                url: slackWebhookUrl,
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                payload: JSON.stringify({ text }),
            });
        }
        if (webhookUrl) {
            await requestAsBrowser({
                url: webhookUrl,
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                payload: JSON.stringify(payload),
            });
        }
    } catch (err) {
        log.warning(`Notification failed: ${err.message}`);
    }
}

async function acceptConsent(page) {
    try {
        await page.evaluate(() => {
            const phrases = ['accept', 'agree', 'allow all', 'allow', 'ok', 'got it', 'consent'];
            const buttons = Array.from(document.querySelectorAll('button, [role="button"], input[type="button"], input[type="submit"]'));
            for (const b of buttons) {
                const txt = (b.innerText || b.value || '').toLowerCase().trim();
                if (!txt) continue;
                if (phrases.some((p) => txt.includes(p))) {
                    b.click();
                }
            }
        });
        await sleep(500);
    } catch {}
}

async function setRegion(page, region) {
    try {
        await page.setExtraHTTPHeaders({
            'Accept-Language': region === 'US' ? 'en-US,en;q=0.9' : 'en;q=0.9',
        });
        await page.evaluateOnNewDocument((regionCode) => {
            try {
                localStorage.setItem('tt_region_hint', regionCode);
                localStorage.setItem('tt_lang_hint', 'en');
            } catch {}
        }, region);
    } catch {}
}

async function autoScroll(page, maxMs = 8000) {
    const start = Date.now();
    try {
        while (Date.now() - start < maxMs) {
            await page.evaluate(() => window.scrollBy(0, window.innerHeight));
            await sleep(250);
        }
    } catch {}
}

async function extractStateFromWindow(page) {
    try {
        return await page.evaluate(() => {
            const safe = (obj) => {
                try { return JSON.parse(JSON.stringify(obj)); } catch { return null; }
            };
            const scrs = Array.from(document.querySelectorAll('script[type="application/ld+json"]')).map(s => {
                try { return JSON.parse(s.textContent || '{}'); } catch { return null; }
            }).filter(Boolean);
            const anyJsonScript = Array.from(document.querySelectorAll('script')).map(s => s.textContent || '').find(t => /product/i.test(t) && /price/i.test(t));
            return {
                initProps: safe(window.__INIT_PROPS__),
                sigi: safe(window.SIGI_STATE),
                next: safe(window.__NEXT_DATA__),
                ld: scrs,
                anyJsonScript,
                location: window.location.href,
            };
        });
    } catch (e) {
        log.debug(`extractStateFromWindow failed: ${e.message}`);
        return {};
    }
}

function findProductInState(state) {
    let product = null;

    // LD+JSON Product
    if (Array.isArray(state.ld)) {
        for (const obj of state.ld) {
            if (obj && (obj['@type'] === 'Product' || obj.name || obj.offers)) {
                const offers = obj.offers || {};
                product = {
                    product_id: obj.sku || obj.productID || obj.identifier || null,
                    title: obj.name || null,
                    description: obj.description || null,
                    images: Array.isArray(obj.image) ? obj.image : obj.image ? [obj.image] : [],
                    price: {
                        current: toNumberSafe(offers.price),
                        original: toNumberSafe(offers.highPrice || offers.listPrice),
                        currency: offers.priceCurrency || pickCurrency(JSON.stringify(obj)),
                    },
                    availability: offers.availability || null,
                    rating: obj.aggregateRating?.ratingValue ? Number(obj.aggregateRating.ratingValue) : null,
                    review_count: obj.aggregateRating?.reviewCount ? Number(obj.aggregateRating.reviewCount) : null,
                };
                break;
            }
        }
    }

    // Next.js state
    if (!product && state.next) {
        const text = JSON.stringify(state.next);
        const idMatch = text.match(/"productId"\s*:\s*"(\d+)"/) || text.match(/"product_id"\s*:\s*"(\d+)"/);
        const titleMatch = text.match(/"title"\s*:\s*"([^"]{3,200})"/);
        const priceMatch = text.match(/"price[^"]*"\s*:\s*"?([\d.,]+)"?/);
        const currency = pickCurrency(text);
        product = {
            product_id: idMatch?.[1] || null,
            title: titleMatch?.[1] || null,
            price: { current: toNumberSafe(priceMatch?.[1]), currency },
        };
    }

    // SIGI or INIT_PROPS blobs
    if (!product && (state.sigi || state.initProps)) {
        const text = JSON.stringify(state.sigi || state.initProps);
        const idMatch = text.match(/"product(?:Id|ID|_id)"\s*:\s*"(\d+)"/);
        const titleMatch = text.match(/"name"\s*:\s*"([^"]{3,200})"/) || text.match(/"title"\s*:\s*"([^"]{3,200})"/);
        const priceMatch = text.match(/"price[^"]*"\s*:\s*"?([\d.,]+)"?/);
        const currency = pickCurrency(text);
        product = {
            product_id: idMatch?.[1] || null,
            title: titleMatch?.[1] || null,
            price: { current: toNumberSafe(priceMatch?.[1]), currency },
        };
    }

    return product;
}

async function extractCreatorsFromDom(page, maxCreators = 10) {
    const creators = await page.evaluate((limit) => {
        const out = [];
        const anchors = Array.from(document.querySelectorAll('a')).filter(a => /\/video\/\d+/.test(a.href));
        for (const a of anchors.slice(0, limit)) {
            const card = a.closest('div');
            const profileA = card ? card.querySelector('a[href*="/@"]') : null;
            const handle = profileA ? (profileA.href.split('/@')[1] || '').split(/[/?]/)[0] : null;
            const name = profileA ? profileA.textContent?.trim() : null;
            const likesEl = card ? card.querySelector('[aria-label*="like"], [data-e2e*="like-count"], [class*="like"]') : null;
            const commentsEl = card ? card.querySelector('[aria-label*="comment"], [data-e2e*="comment-count"], [class*="comment"]') : null;
            const sharesEl = card ? card.querySelector('[aria-label*="share"], [data-e2e*="share-count"], [class*="share"]') : null;

            out.push({
                video_url: a.href,
                creator: handle ? { handle, name, profile_url: profileA?.href || null } : null,
                stats: {
                    likes: likesEl ? likesEl.textContent : null,
                    comments: commentsEl ? commentsEl.textContent : null,
                    shares: sharesEl ? sharesEl.textContent : null,
                },
            });
        }
        return out;
    }, maxCreators);
    return creators || [];
}

async function extractProductFromDom(page) {
    return await page.evaluate(() => {
        const textContent = (sel) => document.querySelector(sel)?.textContent?.trim() || null;
        const title = textContent('h1') || textContent('[data-e2e*="product-title"]') || textContent('[class*="title"]');
        const priceEl = Array.from(document.querySelectorAll('span, div'))
            .find(el => /\$|€|£|SGD|MYR|IDR|THB|PHP|VND/.test(el.textContent || ''));
        const priceText = priceEl ? priceEl.textContent : null;

        let images = Array.from(document.querySelectorAll('img'))
            .map(i => i.src).filter(Boolean);
        images = Array.from(new Set(images));

        const ratingEl = Array.from(document.querySelectorAll('[aria-label*="rating"], [class*="rating"]')).find(Boolean);
        const ratingText = ratingEl ? ratingEl.textContent : null;

        const sellerA = Array.from(document.querySelectorAll('a')).find(a => /\/@/.test(a.href) && /shop|store|seller/i.test(a.textContent || ''));
        const seller = sellerA ? {
            url: sellerA.href,
            handle: (sellerA.href.split('/@')[1] || '').split(/[/?]/)[0],
            name: sellerA.textContent?.trim() || null,
        } : null;

        return {
            title,
            priceText,
            images,
            ratingText,
            seller,
        };
    });
}

async function diffAndNotify({ kv, notify, productObj }) {
    const key = `PRODUCT::${productObj.product_id || sha1(productObj.url)}`;
    const prev = await kv.getValue(key);
    let hasChange = false;
    const changes = {};

    if (prev) {
        if (prev?.price?.current !== productObj?.price?.current || prev?.price?.currency !== productObj?.price?.currency) {
            hasChange = true;
            changes.price = { from: prev.price, to: productObj.price };
        }
        if (prev?.availability !== productObj?.availability) {
            hasChange = true;
            changes.availability = { from: prev.availability, to: productObj.availability };
        }
    } else {
        changes.first_seen = true;
    }

    productObj.detected_changes = changes;
    await kv.setValue(key, productObj);

    if (hasChange) {
        await notifyAll(notify || {}, {
            event: 'product_change',
            product: {
                product_id: productObj.product_id,
                title: productObj.title,
                price: productObj.price,
                availability: productObj.availability,
                url: productObj.url,
            },
            changes,
            ts: new Date().toISOString(),
        });
    }
}

Apify.main(async () => {
    const input = await Apify.getInput() || {};
    const {
        productUrls = [],
        sellerHandles = [],
        keywords = [],
        categoryUrls = [],
        region = 'US',
        maxProductsPerSeller = 50,
        maxCreatorsPerProduct = 10,
        maxResultsPerKeyword = 20,
        captureScreenshots = true,
        notify = {},
        proxyConfiguration,
        maxConcurrency = 10,
        requestHandlerTimeoutSecs = 120,
        maxRequestsPerCrawl = 1000,
        debug = false,
    } = input;

    log.setLevel(debug ? log.LEVELS.DEBUG : log.LEVELS.INFO);

    const proxy = proxyConfiguration ? await Apify.createProxyConfiguration(proxyConfiguration) : null;
    const kv = await Apify.openKeyValueStore('STATE');
    const queue = await Apify.openRequestQueue();

    // Seed requests
    for (const url of productUrls) {
        await queue.addRequest({ url: normalizeUrl(url), userData: { label: 'PRODUCT' } });
    }
    for (const handle of sellerHandles) {
        const url = `https://www.tiktok.com/@${handle}`;
        await queue.addRequest({ url, userData: { label: 'SELLER', handle } });
    }
    for (const kw of keywords) {
        const url = `https://www.tiktok.com/search?q=${encodeURIComponent(kw)}`;
        await queue.addRequest({ url, userData: { label: 'KEYWORD', kw } });
    }
    for (const url of categoryUrls) {
        await queue.addRequest({ url, userData: { label: 'CATEGORY' } });
    }

    const crawler = new Apify.PuppeteerCrawler({
        requestQueue: queue,
        proxyConfiguration: proxy,
        maxConcurrency,
        useSessionPool: true,
        persistCookiesPerSession: true,
        launchContext: {
            launchOptions: {
                headless: true,
                args: [
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--lang=en-US,en',
                ],
            },
        },
        requestHandlerTimeoutSecs,
        maxRequestsPerCrawl,
        preNavigationHooks: [
            async ({ page, request }) => {
                await setRegion(page, region);
                const ua = `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36`;
                await page.setUserAgent(ua);
                await page.setViewport({ width: 1366, height: 768 });
                if (debug) log.debug(`Navigating: ${request.url}`);
            },
        ],
        navigationTimeoutSecs: Math.max(60, requestHandlerTimeoutSecs),
        requestHandler: async ({ request, page, response }) => {
            const { label } = request.userData;
            if (response && response.status() >= 400) {
                log.warning(`HTTP ${response.status()} for ${request.url}`);
            }

            await acceptConsent(page);
            try {
                await page.waitForTimeout(1500);
            } catch {}

            if (label === 'PRODUCT') {
                const url = page.url();
                const state = await extractStateFromWindow(page);
                let product = findProductInState(state);

                if (!product) {
                    const fallback = await extractProductFromDom(page);
                    product = {
                        product_id: null,
                        title: fallback.title || null,
                        price: {
                            current: toNumberSafe(fallback.priceText),
                            currency: pickCurrency(fallback.priceText),
                        },
                        images: fallback.images || [],
                        rating: fallback.ratingText ? toNumberSafe(fallback.ratingText) : null,
                        seller: fallback.seller || null,
                    };
                }

                const idFromUrl = (url.match(/\/product\/(\d+)/) || url.match(/productId=(\d+)/) || [])[1];
                product.product_id = product.product_id || idFromUrl || (product.title ? sha1(product.title) : sha1(url));

                const availabilityText = await page.evaluate(() => {
                    const n = Array.from(document.querySelectorAll('*')).find(el => /sold out|out of stock|in stock|available/i.test(el.textContent || ''));
                    return n ? n.textContent : null;
                });
                const availability = availabilityText ? (/(sold out|out of stock)/i.test(availabilityText) ? 'OUT_OF_STOCK' : 'IN_STOCK') : null;

                if (!product.seller) {
                    product.seller = await page.evaluate(() => {
                        const sellerLink = Array.from(document.querySelectorAll('a')).find(a => /\/@/.test(a.href) && (/shop|store|seller/i.test(a.textContent || '') || a.href.includes('/shop')));
                        if (!sellerLink) return null;
                        return {
                            url: sellerLink.href,
                            handle: (sellerLink.href.split('/@')[1] || '').split(/[/?]/)[0],
                            name: sellerLink.textContent?.trim() || null,
                        };
                    });
                }

                const creators = await extractCreatorsFromDom(page, maxCreatorsPerProduct);

                let screenshotKey = null;
                if (captureScreenshots) {
                    try {
                        screenshotKey = `SCREENSHOT-${product.product_id}`;
                        const buffer = await page.screenshot({ fullPage: true });
                        if (buffer) await Apify.setValue(screenshotKey, buffer, { contentType: 'image/png' });
                    } catch (e) {
                        log.debug(`Screenshot failed: ${e.message}`);
                    }
                }

                const item = {
                    type: 'product',
                    region,
                    url,
                    product_id: product.product_id,
                    title: product.title || null,
                    description: product.description || null,
                    price: {
                        current: product.price?.current ?? null,
                        original: product.price?.original ?? null,
                        currency: product.price?.currency || null,
                    },
                    availability,
                    rating: product.rating ?? null,
                    review_count: product.review_count ?? null,
                    seller: product.seller || null,
                    images: product.images || [],
                    creators,
                    screenshot_key: screenshotKey,
                    captured_at: new Date().toISOString(),
                };

                await diffAndNotify({ kv, notify, productObj: item });
                await Apify.pushData(item);
                log.info(`Saved product ${item.product_id} | ${item.title || ''}`);

            } else if (label === 'SELLER') {
                await autoScroll(page, 6000);
                const productLinks = await page.evaluate(() => {
                    const links = Array.from(document.querySelectorAll('a')).map(a => a.href);
                    return links.filter(href => /\/shop\/product\/\d+|\/product\/\d+/.test(href));
                });
                const unique = Array.from(new Set(productLinks)).slice(0, maxProductsPerSeller);
                log.info(`Discovered ${unique.length} product links from seller ${request.userData.handle}`);
                for (const u of unique) {
                    await queue.addRequest({ url: normalizeUrl(u), userData: { label: 'PRODUCT' } });
                }

            } else if (label === 'KEYWORD') {
                await autoScroll(page, 6000);
                const links = await page.evaluate(() => {
                    const all = Array.from(document.querySelectorAll('a')).map(a => a.href);
                    return all.filter(href => /\/shop\/product\/\d+|\/product\/\d+/.test(href));
                });
                const unique = Array.from(new Set(links)).slice(0, maxResultsPerKeyword);
                log.info(`Keyword "${request.userData.kw}" -> ${unique.length} product links`);
                for (const u of unique) {
                    await queue.addRequest({ url: normalizeUrl(u), userData: { label: 'PRODUCT' } });
                }

            } else if (label === 'CATEGORY') {
                await autoScroll(page, 8000);
                const links = await page.evaluate(() => {
                    const all = Array.from(document.querySelectorAll('a')).map(a => a.href);
                    return all.filter(href => /\/shop\/product\/\d+|\/product\/\d+/.test(href));
                });
                const unique = Array.from(new Set(links));
                log.info(`Category listing -> ${unique.length} product links`);
                for (const u of unique) {
                    await queue.addRequest({ url: normalizeUrl(u), userData: { label: 'PRODUCT' } });
                }
            }
        },
        failedRequestHandler: async ({ request, error }) => {
            log.error(`Request failed ${request.url}: ${error.message}`);
            await Apify.pushData({
                type: 'error',
                url: request.url,
                label: request.userData?.label,
                error: error.message,
                ts: new Date().toISOString(),
            });
        },
    });

    await crawler.run();

    log.info('Run finished.');
});
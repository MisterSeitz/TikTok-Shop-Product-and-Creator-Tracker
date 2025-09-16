# TikTok Shop Product and Creator Tracker (Apify Actor)

Tracks TikTok Shop products by URL, seller profiles, keywords, or category pages. Extracts product metadata, price, currency, availability, ratings, images, seller info, and linked creator videos. Stores KV snapshots to detect changes and sends Slack/webhook alerts.

What’s included
- PuppeteerCrawler with session pool and proxies.
- Multi-source extraction: JSON-LD/Next/SIGI state + DOM fallbacks.
- Change detection (price and availability) with notifications.
- Screenshots per product.
- Discovery from seller profiles, keywords, and category pages.

Inputs (see INPUT_SCHEMA.json)
- productUrls: Array of direct product URLs.
- sellerHandles: Array of seller or creator handles (without @).
- keywords: Array of search terms to discover products.
- categoryUrls: Array of TikTok Shop category/listing URLs.
- region: Region hint (affects language/availability).
- maxProductsPerSeller, maxCreatorsPerProduct, maxResultsPerKeyword.
- captureScreenshots: Store PNGs to key-value store.
- notify: { slackWebhookUrl?, webhookUrl? } for alerts.
- proxyConfiguration: Use Apify Proxy with region-matching country group (recommended).
- maxConcurrency, requestHandlerTimeoutSecs, maxRequestsPerCrawl, debug.

Output
- Dataset items:
  - type: "product"
  - product_id, url, region, captured_at
  - title, description
  - price: { current, original, currency }
  - availability: IN_STOCK | OUT_OF_STOCK | null
  - rating, review_count
  - seller: { handle, name, url }
  - images: []
  - creators: [{ video_url, creator: { handle, name, profile_url }, stats: { likes, comments, shares } }]
  - screenshot_key: key in default KV store
  - detected_changes: { price?, availability?, first_seen? }

Quick start (local)
1) Install
   npm ci

2) Run with sample input
   APIFY_LOCAL_STORAGE_DIR=./apify_storage node main.js
   // Then paste input JSON in the run on the Apify platform, or create INPUT.json under apify_storage/key_value_stores/default/INPUT.json.

3) Recommended: Run on Apify with Apify Proxy set to target region (e.g., US).

Example input
{
  "productUrls": [
    "https://www.tiktok.com/shop/product/1234567890123456789"
  ],
  "sellerHandles": ["brand_official"],
  "keywords": ["wireless earbuds"],
  "region": "US",
  "maxCreatorsPerProduct": 5,
  "notify": {
    "slackWebhookUrl": "https://hooks.slack.com/services/XXX/YYY/ZZZ"
  },
  "proxyConfiguration": { "useApifyProxy": true, "apifyProxyGroups": ["RESIDENTIAL"] },
  "debug": true
}

Notes and limitations
- TikTok changes DOM/UI frequently; this actor uses multiple extraction strategies but may require selector updates over time.
- Some data is region-gated; use region-appropriate proxies for best results.
- Respect TikTok’s Terms of Service and local laws. Use responsibly.
- Engagement stats parsed from product-linked video cards are best-effort; for precise counts, add a follow-up video detail step.

Roadmap ideas
- Deeper creator linkage: paginate product “Videos/Affiliates” tabs and enrich stats with timestamps.
- Historical price chart export; currency normalization to USD.
- More robust availability signals from hidden state or APIs.
- BigQuery/S3 exporters and daily scheduled monitoring template.

Development tips
- Enable debug=true to increase logging and validate screenshots and extracted DOM.
- Check the default key-value store for screenshots and state per product.
- To add custom alerts or diffs (e.g., rating changes), update diffAndNotify in main.js.
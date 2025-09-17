# TikTok Shop Product Tracker (Apify Python + Playwright)

Track TikTok Shop products for metadata, price and currency, availability/stock signals, seller info, images, ratings, and creator-linked videos. Detect changes across runs and optionally send Slack or webhook notifications.

Key differentiators
- Change detection across runs (price and availability diffs saved per product in KV store)
- Optional alerts via Slack and/or generic webhook on relevant changes
- Regional handling via Accept-Language and localStorage seeding
- Creator-linked videos discovery on product pages
- Playwright-based scraping for better resilience and scrolling/lazy-load handling

What it collects (per product)
- product_id, url, region, captured_at
- title, description
- seller { handle, name, url }
- price { current, original?, currency }
- availability IN_STOCK | OUT_OF_STOCK | null
- rating, review_count
- images[]
- creators[] with video_url and best-effort engagement stats
- screenshot_key (optional)
- detected_changes { price?, availability?, first_seen? }

Project structure
- apify.json
- Dockerfile
- INPUT_SCHEMA.json
- main.py
- requirements.txt
- README.md
- LICENSE
- .gitignore
- .dockerignore

Requirements
- Runs on Apify using base image apify/actor-python-playwright:3.11
- Apify Python SDK v1.x
- Playwright (bundled in the base image)
- httpx for notifications

Quick start (local)
1) Install dependencies:
   - Ensure Docker is installed if you want to run as a container.
   - Or for local Python run, set up a venv and install requirements.txt.

2) Validate input schema (optional but recommended):
   - npm i -g apify
   - npx apify@latest validate-schema INPUT_SCHEMA.json

3) Run locally with an example input:
   - Create apify_storage directory for local run:
     - mkdir -p apify_storage
   - Save an input to apify_storage/key_value_stores/default/INPUT.json:
     {
       "productUrls": [
         "https://www.tiktok.com/shop/product/1234567890"
       ],
       "region": "US",
       "captureScreenshots": true,
       "notify": {
         "enabled": true,
         "onlyOnChange": true,
         "slackWebhookUrl": "https://hooks.slack.com/services/XXX/YYY/ZZZ"
       },
       "proxyConfiguration": {
         "useApifyProxy": true,
         "apifyProxyGroups": ["RESIDENTIAL"], 
         "apifyProxyCountry": "US"
       },
       "maxConcurrency": 3,
       "timeouts": {
         "navigationTimeoutSecs": 30,
         "requestTimeoutSecs": 30
       },
       "debug": false
     }

   - Run:
     APIFY_LOCAL_STORAGE_DIR=./apify_storage python main.py

4) Inspect results:
   - Dataset: apify_storage/datasets/default
   - Key-Value Store snapshots: apify_storage/key_value_stores/default
   - Screenshots (if enabled): in default key-value store with keys like screenshot_<product_id>.png

Deploy on Apify
- Create a new actor, connect this GitHub repo or upload a .zip containing the files listed above.
- Select Clean build.
- Run with your desired input. Results go to the actor's default dataset and key-value store.

Input fields (summary)
- productUrls[]: Product detail URLs
- sellerHandles[]: Shop handles (seeds discovery)
- keywords[]: Search terms (best-effort discovery)
- categoryUrls[]: Category/listing URLs (best-effort discovery)
- region: US, GB, SG, MY, TH, VN, PH, ID
- acceptLanguage: Accept-Language header (default en-US,en;q=0.9)
- limits: { maxProducts, maxProductsPerSeller, maxProductsPerCategory }
- maxConcurrency: default 5
- timeouts: { navigationTimeoutSecs, requestTimeoutSecs }
- includeCreatorVideos: default true
- captureScreenshots: default false
- notify: { enabled, onlyOnChange, slackWebhookUrl, webhookUrl }
- proxyConfiguration: Apify proxy or custom proxies (recommended to match region)
- debug: verbose logging

Example inputs
1) Track specific products with alerts
{
  "productUrls": [
    "https://www.tiktok.com/shop/product/1234567890",
    "https://www.tiktok.com/shop/product/9876543210"
  ],
  "region": "US",
  "captureScreenshots": true,
  "notify": {
    "enabled": true,
    "onlyOnChange": true,
    "slackWebhookUrl": "https://hooks.slack.com/services/XXX/YYY/ZZZ"
  },
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"],
    "apifyProxyCountry": "US"
  }
}

2) Discover from sellers and categories with limits
{
  "sellerHandles": ["bestshop", "another_seller"],
  "categoryUrls": ["https://www.tiktok.com/@bestshop/shop"],
  "region": "GB",
  "limits": {
    "maxProducts": 50,
    "maxProductsPerSeller": 20,
    "maxProductsPerCategory": 30
  },
  "maxConcurrency": 10,
  "notify": {
    "enabled": true,
    "onlyOnChange": true
  },
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"],
    "apifyProxyCountry": "GB"
  }
}

Output example (dataset item)
{
  "product_id": "1234567890",
  "url": "https://www.tiktok.com/shop/product/1234567890",
  "region": "US",
  "captured_at": "2025-01-01T12:34:56.000Z",
  "title": "Sample Product",
  "description": "Great product description",
  "seller": {
    "handle": "bestshop",
    "name": "Best Shop",
    "url": "https://www.tiktok.com/@bestshop/shop"
  },
  "price": {
    "current": 19.99,
    "original": 24.99,
    "currency": "USD"
  },
  "availability": "IN_STOCK",
  "rating": 4.7,
  "review_count": 153,
  "images": ["https://.../image1.jpg", "https://.../image2.jpg"],
  "creators": [
    { "video_url": "https://www.tiktok.com/@creator/video/123", "likes": 1200, "comments": 45 }
  ],
  "screenshot_key": "screenshot_1234567890.png",
  "detected_changes": {
    "price": { "from": 21.99, "to": 19.99 }
  }
}

Change detection and snapshots
- Per-product snapshot stored at key product_<product_id> in the default key-value store.
- Diffs are computed for price.current and availability.
- detected_changes includes fields:
  - price: { from, to } if price changed or first observed
  - availability: { from, to } when it differs from previous run
  - first_seen: true on the first time we see the product
- Notifications are only sent when relevant changes occur if notify.onlyOnChange is true.

Notifications
- Slack: Uses incoming webhook URL. Sends a concise text with changes and a link/title.
- Generic webhook: HTTP POST with the full product JSON payload to notify.webhookUrl.
- Both use async httpx with timeouts, errors are logged (won’t fail the run).

How it works
- Launches headless Chromium via Playwright with optional per-context proxies.
- Sets Accept-Language and seeds localStorage hints for region.
- Handles cookie/consent prompts best-effort.
- Uses structured data first: JSON-LD, window globals (SIGI_STATE, __NEXT_DATA__, APOLLO_STATE), and DOM fallbacks.
- Light scrolling to trigger lazy loads on listings and product pages.
- Concurrency controlled via asyncio worker tasks.

Best practices and recommendations
- Use region-appropriate proxies. For US market, use Apify proxy with country US or residential pool.
- Prefer productUrls for precise tracking; seller/category/keyword discovery is best-effort and can change as TikTok updates UI.
- If you see frequent 429/403, lower maxConcurrency and increase timeouts, and ensure correct regional proxy usage.
- For stable alerts, run on a schedule and keep notify.onlyOnChange: true.

Troubleshooting
- Input schema fails to validate:
  - Run npx apify@latest validate-schema INPUT_SCHEMA.json and ensure arrays/objects have editors and no stray quotes.
- Docker build failures:
  - Ensure base image is apify/actor-python-playwright:3.11 and internet access is available during pip install.
- Playwright timeouts:
  - Increase timeouts.navigationTimeoutSecs and lower maxConcurrency.
  - Verify proxies are working and region matches the selected market.
- Empty fields:
  - TikTok frequently changes its frontend. This actor prefers structured data; DOM fallbacks help, but some markets/pages may vary. Open an issue with a sample URL and region.

Development and local testing
- Run with local storage:
  APIFY_LOCAL_STORAGE_DIR=./apify_storage python main.py
- Inspect logs and outputs in ./apify_storage.
- Adjust debug flag to true for verbose logs.

Compliance and responsible use
- Ensure you comply with TikTok’s Terms of Service and local laws.
- Use region-appropriate proxies and avoid excessive load.
- Do not collect or share personally identifiable information beyond the scope of product tracking.
- This project is for research/monitoring. You are responsible for how you use it.

License
- MIT License. See LICENSE.

Changelog
- 0.1.0 Initial release: Product metadata, price/availability tracking with diffs and notifications, creator-linked videos, screenshots, regional handling, Playwright scraping.

FROM apify/actor-node:20 AS build
WORKDIR /usr/src/app
Copy and install deps

COPY package*.json ./
RUN if [ -f package-lock.json ]; then npm --quiet ci; else npm --quiet install --no-audit --no-fund; fi
Copy app sources

COPY . ./
# Optional: prune dev deps for smaller image (best-effort)

RUN npm prune --omit=dev --no-audit --no-fund || true
# Provide a CMD so targeting this stage still runs

CMD ["node", "main.js"]
# Final stage (used if no build stage targeted)

FROM apify/actor-node:20 AS final
WORKDIR /usr/src/app
COPY --from=build /usr/src/app /usr/src/app
CMD ["node", "main.js"]

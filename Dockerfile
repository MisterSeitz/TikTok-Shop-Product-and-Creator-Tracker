FROM apify/actor-node:20 AS build
WORKDIR /usr/src/app
Install deps (ci if lockfile exists, otherwise install)

COPY package*.json ./
RUN if [ -f package-lock.json ]; then npm --quiet ci; else npm --quiet install --no-audit --no-fund; fi
Copy the rest of the project

COPY . ./
Optional: prune dev deps for smaller image (tolerate missing lockfile)

RUN npm prune --omit=dev --no-audit --no-fund || true
Ensure this stage can be used as the final image if Apify targets it

CMD ["node", "main.js"]
Final runtime stage (used if no specific build stage is targeted)

FROM apify/actor-node:20 AS final
WORKDIR /usr/src/app
COPY --from=build /usr/src/app /usr/src/app
CMD ["node", "main.js"]

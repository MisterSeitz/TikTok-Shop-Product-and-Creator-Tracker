FROM apify/actor-node-puppeteer-chrome:20

WORKDIR /usr/src/app
Copy only package files first (better layer caching) and give ownership to myuser

COPY --chown=myuser:myuser package*.json ./
# Switch to myuser (default in this image) and install deps

USER myuser
RUN if [ -f package-lock.json ]; then npm --quiet ci; else npm --quiet install --no-audit --no-fund; fi
Copy the rest of the project, still owned by myuser

COPY --chown=myuser:myuser . ./
Run the actor

CMD ["node", "main.js"]

# Option B — install as root, then drop privileges
FROM apify/actor-node-puppeteer-chrome:20

WORKDIR /usr/src/app
# Install deps as root (no permissions issues)

USER root
COPY package*.json ./
RUN if [ -f package-lock.json ]; then npm --quiet ci; else npm --quiet install --no-audit --no-fund; fi
Copy sources and give everything to myuser

COPY . ./
RUN chown -R myuser:myuser /usr/src/app
# Drop to non-root for runtime

USER myuser
CMD ["node", "main.js"]

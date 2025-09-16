# Use Apify base image with Node 20, Chrome, and fonts
FROM apify/actor-node-puppeteer-chrome:20

# Copy package files first for better layer caching
# Using --chown to ensure proper ownership for myuser
COPY --chown=myuser:myuser package.json ./
COPY --chown=myuser:myuser package-lock.json* ./

# Install dependencies as root, then drop to myuser
# Use npm ci if lockfile exists, otherwise npm install
RUN --mount=type=cache,target=/root/.npm \
    if [ -f package-lock.json ]; then npm ci; else npm install --no-audit --no-fund; fi

# Copy the rest of the actor source files
COPY --chown=myuser:myuser . ./

# Drop privileges to non-root user provided by the base image
USER myuser

# Run the actor
CMD ["node", "main.js"]

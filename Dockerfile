# Use Apify base image with Node 20, Chrome, and fonts

FROM apify/actor-node-puppeteer-chrome:20
# Ensure working directory (already set in base image, but explicit is fine)

WORKDIR /usr/src/app
# Copy package files (works with or without a lockfile)

COPY package*.json ./
# Install dependencies as root (no BuildKit flags)

RUN if [ -f package-lock.json ]; then npm ci; else npm install --no-audit --no-fund; fi
# Copy the rest of the actor source files

COPY . ./
# Fix ownership and drop privileges to the non-root user

RUN chown -R myuser:myuser /usr/src/app
USER myuser
# Run the actor

CMD ["node", "main.js"]

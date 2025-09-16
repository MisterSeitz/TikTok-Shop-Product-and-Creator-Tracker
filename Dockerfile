# Use Apify base image with Node 20, Chrome, and fonts

FROM apify/actor-node-puppeteer-chrome:20
# Switch to root for dependency installation to avoid EACCES

USER root
# Work in app dir

WORKDIR /usr/src/app
# Copy package files (supports with/without lockfile)

COPY package*.json ./
# Install dependencies as root

RUN npm config set fund false && npm config set audit false && 
if [ -f package-lock.json ]; then npm ci; else npm install --no-audit --no-fund; fi
# Copy the rest of the source

COPY . ./
# Fix ownership and drop to non-root for runtime

RUN chown -R myuser:myuser /usr/src/app
USER myuser
# Run the actor

CMD ["node", "main.js"]

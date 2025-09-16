# Use Apify base image with Node 20, Chrome, and fonts

FROM apify/actor-node-puppeteer-chrome:20
Copy package files (works with or without a lockfile)

COPY package*.json ./
# Install dependencies as root, using cache to speed up builds

RUN --mount=type=cache,target=/root/.npm 
if [ -f package-lock.json ]; then npm ci; else npm install --no-audit --no-fund; fi
Copy the rest of the actor source files

COPY . ./
# Fix ownership and drop privileges to the non-root user

RUN chown -R myuser:myuser /usr/src/app
USER myuser
Run the actor

CMD ["node", "main.js"]

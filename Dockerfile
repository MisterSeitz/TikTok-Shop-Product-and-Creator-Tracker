Use Apify base image with Node 20, Chrome, and fonts

FROM apify/actor-node-puppeteer-chrome:20
Install as root to avoid EACCES during npm install

USER root

# Workdir
WORKDIR /usr/src/app

# Use system Chrome from the image and skip Chromium download for Puppeteer
ENV PUPPETEER_SKIP_DOWNLOAD=true 
PUPPETEER_EXECUTABLE_PATH=/usr/bin/google-chrome

# Copy package files and install deps (works with or without lockfile)

COPY package*.json ./
RUN npm config set fund false && npm config set audit false && (test -f package-lock.json && npm ci || npm install --no-audit --no-fund)
# Copy the rest of the source

COPY . ./
# Fix ownership and drop to non-root for runtime

RUN chown -R myuser:myuser /usr/src/app
USER myuser
# Run the actor

CMD npm start
CMD ["node", "main.js"]

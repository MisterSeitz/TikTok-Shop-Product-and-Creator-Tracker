FROM apify/actor-node:20
WORKDIR /usr/src/app
COPY package*.json ./
RUN if [ -f package-lock.json ]; then npm --quiet ci; else npm --quiet install --no-audit --no-fund; fi
COPY . ./
CMD ["node", "main.js"]

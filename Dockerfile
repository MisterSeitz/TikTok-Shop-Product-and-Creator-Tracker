Copy all files

COPY . ./
Install dependencies
If package-lock.json exists, use reproducible 'ci'; otherwise fall back to 'install'

RUN if [ -f package-lock.json ]; then npm --quiet ci; else npm --quiet install --no-audit --no-fund; fi
Set working dir

WORKDIR /usr/src/app
Run the actor

CMD ["node", "main.js"]

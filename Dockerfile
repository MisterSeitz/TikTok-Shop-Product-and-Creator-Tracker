# Apify recommended Node base image
FROM apify/actor-node:20

# Copy all files
COPY . ./

# Install dependencies
RUN npm --quiet ci

# Set up default working dir
WORKDIR /usr/src/app

# Run the actor
CMD ["node", "main.js"]
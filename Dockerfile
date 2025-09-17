# Use the Playwright-enabled Python image from Apify
FROM apify/actor-python-playwright:3.11

# Work as root for setup
USER root
WORKDIR /app

# Install Python dependencies and fix permissions in one layer to reduce size
COPY requirements.txt ./
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir -r requirements.txt

# Copy the rest of your source code
COPY . ./

# Set ownership and switch to the default 'apify' non-root user
RUN chown -R apify:apify /app
USER apify

# Start the Apify Python runtime
CMD ["python", "-m", "apify"]

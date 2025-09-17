# Use the Playwright-enabled Python image
FROM apify/actor-python-playwright:3.11

# Install as root
USER root
WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -U pip && pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . ./

# Fix permissions and drop privileges
RUN chown -R myuser:myuser /app
USER myuser

# Start the Apify Python runtime (don’t use any xvfb entrypoint here)
CMD ["python", "-m", "apify"]

# Use Apify Python + Playwright base image with Chromium preinstalled
FROM apify/actor-python-playwright:3.11

# Environment for reliable runtime and smaller image size
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Create app directory
WORKDIR /app

# Install Python dependencies first (leverages Docker layer caching)
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy the rest of the project files
COPY . /app

# Create non-root user and fix permissions
RUN adduser --disabled-password --gecos "" myuser && chown -R myuser:myuser /app

# Drop to non-root for runtime
USER myuser

# Start the Apify actor (runs main.py via the Apify Python SDK)
CMD ["python", "-m", "apify"]

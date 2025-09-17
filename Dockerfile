# Base image with Python + Playwright for Apify actors
FROM apify/actor-python-playwright:3.11

# Use root to install dependencies and set up user
USER root
WORKDIR /app

# Create a non-root user to run the actor
RUN useradd -m appuser

# Install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir -r requirements.txt

# Copy your source code into the container
COPY . ./

# Fix file permissions and switch to non-root user
RUN chown -R appuser:appuser /app
USER appuser

# Start the Apify Python runtime
CMD ["python", "-m", "apify"]

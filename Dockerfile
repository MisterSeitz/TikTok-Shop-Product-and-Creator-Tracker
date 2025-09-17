FROM apify/actor-python-playwright:3.11

USER root
WORKDIR /app

# Create a non-root user
RUN useradd -m appuser

# Install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir -r requirements.txt

# Copy your code
COPY . ./

# Give ownership to the non-root user
RUN chown -R appuser:appuser /app

# Drop privileges
USER appuser

# Start the Apify runtime
CMD ["python", "-m", "apify"]

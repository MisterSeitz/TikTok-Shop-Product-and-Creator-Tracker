FROM apify/actor-python-playwright:3.11

USER root

# Create non-root user
RUN useradd -m appuser

# Install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir -r requirements.txt

# Copy your source code
COPY . ./

RUN chown -R appuser:appuser ./
USER appuser

CMD ["python", "-m", "apify"]

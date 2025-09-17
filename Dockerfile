FROM apify/actor-python-playwright:3.11

USER root
WORKDIR /app

RUN useradd -m appuser

# Install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir -r requirements.txt

COPY . ./

RUN chown -R appuser:appuser /app
USER appuser

# Run your actor entrypoint
CMD ["python", "main.py"]

FROM apify/actor-python-playwright:3.11

USER root
WORKDIR /usr/src/app

# Install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir -r requirements.txt

# Copy your source code into the container
COPY . ./

# Switch back to non-root user (the base image already defines one)
USER apify

# Run your actor script
CMD ["python", "main.py"]

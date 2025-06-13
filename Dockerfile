FROM ghcr.io/astral-sh/uv:0.7.4-python3.11-bookworm

# Install system dependencies and Nginx
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        make \
        curl \
        gnupg \
        git \
        nginx && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the entire repository content first
COPY . .
RUN rm -f setup.py

# Env variables
ENV UV_NO_CACHE 1
ENV UV_SYSTEM_PYTHON=true

# Install the package with docs dependencies, build HTML docs, and set up Nginx
RUN uv pip install --no-cache-dir -e '.[docs]' && \
    python docs/make.py --html && \
    mkdir -p /usr/share/nginx/html && \
    cp -r docs/build/html/* /usr/share/nginx/html/ && \
    cp default.conf /etc/nginx/conf.d/default.conf

# Expose port 8080 for Cloud Run
EXPOSE 8080

# Start Nginx in the foreground
CMD ["nginx", "-g", "daemon off;"]

# For local development:
# docker build -t dataeng-container-tools-doc .
# docker run --rm -p 8080:8080 dataeng-container-tools-doc

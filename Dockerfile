FROM ghcr.io/astral-sh/uv:0.8.15-python3.13-bookworm

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        nginx && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

ENV UV_NO_CACHE 1

RUN uv run python docs/make.py --html && \
    mkdir -p /usr/share/nginx/html && \
    cp -r docs/src/build/html/* /usr/share/nginx/html/ && \
    cp default.conf /etc/nginx/conf.d/default.conf

EXPOSE 8080
CMD ["nginx", "-g", "daemon off;"]

# For local development:
# docker build -t dataeng-container-tools-doc .
# docker run --rm -p 8080:8080 dataeng-container-tools-doc

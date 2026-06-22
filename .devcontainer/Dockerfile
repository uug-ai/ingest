FROM mcr.microsoft.com/devcontainers/go:1.25-bookworm

# Install node environment
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    nodejs graphviz npm \
    && rm -rf /var/lib/apt/lists/*

USER vscode

# Install go swagger
RUN go install github.com/swaggo/swag/cmd/swag@latest

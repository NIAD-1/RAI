# --- Build Stage (Rust) ---
FROM rust:1.77-slim AS rust-builder
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
# Create a dummy src/main.rs to build dependencies first (for better caching)
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
# Now copy real source and rebuild
COPY src ./src
# Update main.rs timestamp to trigger full rebuild
RUN touch src/main.rs && cargo build --release

# --- Final Stage ---
FROM debian:bookworm-slim

# Install system dependencies
# 1. ca-certificates & openssl (for https/rust)
# 2. curl (for node setup)
# 3. nodejs & npm (for whatsapp bridge)
# 4. python3 & reportlab (for pdf generation)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl-dev \
    curl \
    gnupg \
    python3 \
    python3-reportlab \
    && rm -rf /var/lib/apt/lists/*

# Install Move Node.js
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy Rust backend
COPY --from=rust-builder /app/target/release/research-bot /usr/local/bin/

# Copy Bridge
COPY bridge ./bridge
RUN cd bridge && npm install

# Copy Python Scripts
COPY scripts ./scripts

# Create required data dirs
RUN mkdir -p feedback papers

# Create start script
COPY scripts/start.sh /usr/local/bin/start.sh
RUN chmod +x /usr/local/bin/start.sh

# Render default port is 10000, we'll map PORT env to this
EXPOSE 3000
EXPOSE 8002

CMD ["/usr/local/bin/start.sh"]

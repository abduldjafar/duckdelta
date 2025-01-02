# Stage 1: Build the Rust binary
FROM rust:latest as builder

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the Cargo.toml and Cargo.lock to cache dependencies
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to fetch dependencies
RUN mkdir -p src && echo "fn main() {}" > src/main.rs

# Fetch dependencies
RUN cargo fetch && cargo build --release

# Copy the actual source code
COPY . .

# Build the project
RUN cargo build --release

# Stage 2: Create a minimal runtime image
FROM rust:latest 

# Install necessary runtime dependencies
RUN apt-get update && apt-get install -y vim ca-certificates && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the runtime container
WORKDIR /usr/src/app
RUN mkdir src
# Copy the binary from the builder stage
COPY --from=builder /usr/src/app/target/release/duckdelta /usr/local/bin/duckdelta
COPY --from=builder /usr/src/app/src/raw_data /usr/src/app/src/raw_data
RUN mkdir -p /root/.aws
COPY .aws /root/.aws




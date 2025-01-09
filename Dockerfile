FROM rust:slim-bookworm AS builder

WORKDIR /compile

COPY . .

RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev && \
    cargo build --release

FROM debian:bookworm-slim

WORKDIR /mgxhub

COPY --from=builder /compile/target/release/mgxhub .

RUN apt-get update && \
    apt-get install -y libssl3 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

CMD ["./mgxhub"]
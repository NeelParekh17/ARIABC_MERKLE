FROM ubuntu:22.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential python3 flex bison perl cmake pkg-config libssl-dev zlib1g-dev \
    libreadline-dev libkrb5-dev liblz4-dev libzstd-dev ca-certificates curl git \
    binutils rsync && rm -rf /var/lib/apt/lists/*

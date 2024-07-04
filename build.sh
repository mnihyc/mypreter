#!/bin/sh

rustup toolchain install nightly
rustup component add rust-src --toolchain nightly

build() {
    rustup +nightly target add $1
    RUSTFLAGS="-Zlocation-detail=none" cargo +nightly build --target $1 --release \
        -Z build-std=std,panic_abort \
        -Z build-std-features="optimize_for_size" \
        -Z build-std-features=panic_immediate_abort
}

build_norm() {
    rustup +nightly target add $1
    RUSTFLAGS="-Zlocation-detail=none" cargo +nightly build --target $1 --release
}

build x86_64-unknown-linux-musl
build x86_64-pc-windows-gnu
build_norm aarch64-unknown-linux-musl


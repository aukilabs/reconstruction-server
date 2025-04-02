#!/bin/bash

cd server && cargo build --release
cp target/release/server ../reconstruction

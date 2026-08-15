#!/bin/bash

# Running tests in parallel causes io_uring buffer registrations errors
cargo test -- --test-threads=1
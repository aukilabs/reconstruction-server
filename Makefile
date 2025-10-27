SHELL := /bin/bash

.PHONY: test fmt clippy ci

test:
	$(MAKE) -C server/rust test

fmt:
	$(MAKE) -C server/rust fmt

clippy:
	$(MAKE) -C server/rust clippy

ci: fmt clippy test

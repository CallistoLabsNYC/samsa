KAFKA_BROKERS ?= "127.0.0.1:9092"
KAFKA_TOPIC ?= "tester"

.PHONY: build
build:
	cargo build --all --examples


.PHONY: check
check:
	cargo clippy -- --no-deps
	KAFKA_BROKERS=$(KAFKA_BROKERS) KAFKA_TOPIC=$(KAFKA_TOPIC) cargo test --tests -- --show-output --test-threads=1


.PHONY: bench
bench:
	cargo bench


.PHONY: fmt
fmt:
	cargo fmt --all


.PHONY: clean
clean:
	cargo clean


.PHONY: outdated
outdated:
	cargo install --locked cargo-outdated
	cargo outdated -R

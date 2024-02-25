.PHONY: build
build:
	cargo build


.PHONY: check
check:
	cargo clippy -- --no-deps
	cargo test --tests -- --show-output --test-threads=1


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

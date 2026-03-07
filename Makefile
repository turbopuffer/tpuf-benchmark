BENCHMARKS := $(shell find benchmarks -name '*.toml')
ifdef DURATION
DURATION_FLAG := --duration $(DURATION)
else
DURATION_FLAG :=
endif

ifdef API_ENDPOINT
API_ENDPOINT_FLAG := --endpoint $(API_ENDPOINT)
else
API_ENDPOINT_FLAG :=
endif

.PHONY: build bench publish web-build

build:
	go build -o tpufbench ./cmd/tpufbench

bench: build
	@rm -rf results/*
	@mkdir -p results
	@for f in $(BENCHMARKS); do \
		name=$${f#benchmarks/}; \
		name=$${name%.toml}; \
		echo "Running benchmark: $$name"; \
		./tpufbench run $(DURATION_FLAG) $(API_ENDPOINT_FLAG) \
		  --namespace-prefix tpufbench-nightly-$$(basename $$name) \
		  --namespace-setup-concurrency=16 \
		  --if-nonempty=clear \
		  --output-dir \
		  results/$$name $$f; \
	done

publish:
	$(eval DATE := $(shell date +%Y-%m-%d))
	@for f in $(BENCHMARKS); do \
		name=$${f#benchmarks/}; \
		name=$${name%.toml}; \
		mkdir -p build/$(DATE)/$$(dirname $$name); \
		cp results/$$name/report.json build/$(DATE)/$$name.json; \
		echo "Published: build/$(DATE)/$$name.json"; \
	done
	@git fetch origin gh-pages
	@git checkout gh-pages
	make publish-web

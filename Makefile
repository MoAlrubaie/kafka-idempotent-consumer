# One-command entry points. `make demo` works from a fresh clone with only Java 21+ installed.

JAVA ?= $(if $(JAVA_HOME),$(JAVA_HOME)/bin/java,java)
MVNW := ./mvnw -q -B
CLASSPATH := target/classpath.txt
DEMO_MAIN := io.github.moalrubaie.refunds.demo.Demo
DEMO_ARGS ?=
# Java 24+ warns when the Kafka client uses sun.misc.Unsafe. Allow it there, quietly.
JAVA_MAJOR := $(shell $(JAVA) -XshowSettings:properties -version 2>&1 | awk -F' = ' '/java.specification.version/ {print $$2}')
JAVA_FLAGS := $(shell [ "$(JAVA_MAJOR)" -ge 24 ] 2>/dev/null && echo --sun-misc-unsafe-memory-access=allow)
# Matches docker-compose.yml.
REDPANDA := localhost:19092
POSTGRES := jdbc:postgresql://localhost:15432/refunds

.PHONY: help demo build up demo-redpanda down test lint format clean

help: ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  make %-15s %s\n", $$1, $$2}'

build: ## Compile, and write the classpath the demo runs with
	$(MVNW) -DskipTests test-compile dependency:build-classpath -Dmdep.outputFile=$(CLASSPATH) -Dmdep.includeScope=test

demo: build ## Run all three sinks against an in-process Kafka broker and H2 (no Docker)
	@$(JAVA) $(JAVA_FLAGS) -cp target/classes:target/test-classes:$$(cat $(CLASSPATH)) $(DEMO_MAIN) $(DEMO_ARGS)

up: ## Start Redpanda, Redpanda Console and Postgres with docker compose
	docker compose up -d --wait

demo-redpanda: build ## Run the demo against the docker compose stack (run `make up` first)
	@$(JAVA) $(JAVA_FLAGS) -cp target/classes:target/test-classes:$$(cat $(CLASSPATH)) $(DEMO_MAIN) \
		--bootstrap $(REDPANDA) --jdbc-url $(POSTGRES) --jdbc-user refunds --jdbc-password refunds $(DEMO_ARGS)

down: ## Stop the docker compose stack and delete its data
	docker compose down -v

test: ## Unit and end-to-end tests (the Redpanda and Postgres ones need Docker, and skip without it)
	./mvnw -B test

lint: ## Check formatting
	$(MVNW) spotless:check

format: ## Auto-format the code
	$(MVNW) spotless:apply

clean: ## Remove build output
	$(MVNW) clean

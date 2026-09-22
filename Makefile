.PHONY: setup deps ingest build test run ui clean

# dbt and Dagster both resolve the landing zone through CITIBIKE_ROOT.
export CITIBIKE_ROOT := $(shell pwd)
export DAGSTER_HOME  := $(shell pwd)/.dagster
export DBT_PROFILES_DIR := $(shell pwd)/dbt

VENV := .venv/bin
MONTH ?= 2024-01

setup:
	uv venv --python 3.12
	uv pip install -r requirements.txt
	mkdir -p .dagster && touch .dagster/dagster.yaml
	$(MAKE) deps

deps:
	cd dbt && $(CURDIR)/$(VENV)/dbt deps

ingest:
	$(VENV)/python -m pipeline.ingest --month $(MONTH)

build: deps
	cd dbt && $(CURDIR)/$(VENV)/dbt build

test: deps
	cd dbt && $(CURDIR)/$(VENV)/dbt test

# Full pipeline for one month, orchestrated by Dagster.
run: deps
	$(VENV)/dagster asset materialize --select '*' \
		--partition $(MONTH)-01 -m pipeline.definitions

ui: deps
	$(VENV)/dagster dev -m pipeline.definitions

# Leaves dbt_packages in place; `deps` restores it, and removing it would break
# `dbt parse` on the next run before any target gets a chance to reinstall.
clean:
	rm -rf data/citibike.duckdb dbt/target .dagster/storage

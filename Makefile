.PHONY: setup fetch transform test full clean

setup:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	mkdir -p data

fetch:
	.venv/bin/python orchestrate.py fetch

transform:
	.venv/bin/python orchestrate.py transform

test:
	.venv/bin/python orchestrate.py test

full:
	.venv/bin/python orchestrate.py full

clean:
	rm -rf data/*.duckdb data/*.wal target dbt_packages logs

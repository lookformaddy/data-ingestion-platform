.PHONY: install test demo lint serve clean

PYTHON ?= python3
VENV ?= /tmp/ingest-venv

install:
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements.txt

test:
	PYTHONPATH=src $(PYTHON) -m pytest -q

demo:
	PYTHONPATH=src $(PYTHON) -m ingest demo

serve:
	PYTHONPATH=src $(PYTHON) -m ingest serve --host 0.0.0.0 --port 8000

lint:
	$(PYTHON) -m compileall -q src tests

clean:
	rm -rf .ingest-state .ingest-demo .pytest_cache
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true

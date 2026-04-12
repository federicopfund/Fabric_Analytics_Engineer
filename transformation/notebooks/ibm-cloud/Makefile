.PHONY: all bronze silver gold dry-run status clean

PYTHON := /workspaces/data-engineer/.venv/bin/python
ORCH   := transformation/notebooks/ibm-cloud/orchestrator.py

## Run full pipeline (Bronze → Silver → Gold)
all:
	$(PYTHON) $(ORCH) --layer all

## Run only Bronze layer (RAW CSV → Parquet)
bronze:
	$(PYTHON) $(ORCH) --layer bronze

## Run only Silver layer (Business logic transforms)
silver:
	$(PYTHON) $(ORCH) --layer silver

## Run only Gold layer (Star schema + Db2 export)
gold:
	$(PYTHON) $(ORCH) --layer gold

## Validate pipeline without executing
dry-run:
	$(PYTHON) $(ORCH) --dry-run

## Show last pipeline run status
status:
	$(PYTHON) $(ORCH) --status

## Install Python dependencies
install:
	$(PYTHON) -m pip install pyspark ibm-db ibm_db_sa pandas ibm-cos-sdk jupyter nbconvert

## Upload raw CSV to IBM COS
upload-raw:
	@echo "Uploading CSV files to COS RAW bucket..."
	@for f in transformation/spark-jobs/pipelines/batch-etl-scala/src/main/resources/csv/*.csv; do \
		fname=$$(basename "$$f"); \
		echo "  $$fname"; \
		ibmcloud cos object-put --bucket datalake-raw-us-south --key "retail/$$fname" --body "$$f" --content-type "text/csv"; \
	done

## List COS bucket contents
list-buckets:
	@echo "=== RAW ===" && ibmcloud cos objects --bucket datalake-raw-us-south
	@echo "=== BRONZE ===" && ibmcloud cos objects --bucket datalake-bronze-us-south
	@echo "=== SILVER ===" && ibmcloud cos objects --bucket datalake-silver-us-south
	@echo "=== GOLD ===" && ibmcloud cos objects --bucket datalake-gold-us-south

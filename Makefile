.PHONY: help setup infra-plan infra-apply produce stream dbt-run dbt-test quality lint test clean

help:
	@echo "fraud-detection-pipeline — available commands"
	@echo ""
	@echo "  setup          Install Python dependencies"
	@echo "  infra-plan     Terraform plan (preview infra changes)"
	@echo "  infra-apply    Terraform apply (provision Azure resources)"
	@echo "  produce        Start event producer (default: 500 tps, 5 min)"
	@echo "  stream         Start PySpark streaming job"
	@echo "  dbt-run        Run all dbt models"
	@echo "  dbt-test       Run all dbt tests"
	@echo "  quality        Run Great Expectations suite"
	@echo "  lint           Run ruff + mypy"
	@echo "  test           Run pytest unit tests"
	@echo "  clean          Remove build artifacts"

setup:
	pip install -r requirements.txt
	cd dbt_project && dbt deps

infra-plan:
	cd infra && terraform init && terraform plan

infra-apply:
	cd infra && terraform apply

produce:
	python producer/transaction_producer.py --tps 500 --duration 300

stream:
	spark-submit \
		--master local[4] \
		--packages com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22 \
		streaming/fraud_detector.py

dbt-run:
	cd dbt_project && dbt run --target dev --select staging+

dbt-test:
	cd dbt_project && dbt test --target dev --select staging+ --store-failures

quality:
	python quality/expectations_suite.py --run-id local-$(shell date +%Y%m%d%H%M%S)

lint:
	ruff check streaming/ producer/ quality/ airflow/
	mypy streaming/ producer/ --ignore-missing-imports

test:
	pytest tests/ -v --tb=short

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -name "*.pyc" -delete
	rm -rf dbt_project/target dbt_project/logs

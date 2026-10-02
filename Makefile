SHELL := /bin/bash
.PHONY: host-setup oracle-e2e-bootstrap local-up ui spark-ui test core-build spark-job-build ingestion-build dlt-package-build artifacts-publish runtime-build dlt-build airflow-build seed oracle-up oracle-seed oracle-e2e status benchmark-report metrics-replay local-stop
host-setup:
	bash tools/setup-host.sh
oracle-e2e-bootstrap:
	bash tools/bootstrap-oracle-e2e.sh

local-up:
	bash tools/bootstrap-local.sh
ui:
	python3 tools/port-forwards.py
spark-ui:
	python3 tools/spark-ui.py $(if $(APPLICATION),--application "$(APPLICATION)",) --wait 30
test:
	PYTHONPATH=libraries/ingestion-core/src:engines/dlt/src:engines/spark/job/src:orchestration/airflow/provider/src .venv/bin/pytest -q libraries/ingestion-core/tests engines/dlt/tests engines/spark/job/tests orchestration/airflow/provider/tests
core-build:
	.venv/bin/python -m build --wheel --outdir dist libraries/ingestion-core
spark-job-build:
	.venv/bin/python -m build --wheel --outdir dist engines/spark/job
ingestion-build: spark-job-build
dlt-package-build:
	.venv/bin/python -m build --wheel --outdir dist engines/dlt
artifacts-publish: spark-job-build
	python3 tools/publish-artifacts.py
runtime-build: core-build
	python3 tools/fetch-jars.py
	docker build -f engines/spark/Dockerfile -t company-spark-runtime:0.4.0 .
	docker save company-spark-runtime:0.4.0 | docker exec -i company-spark ctr -n k8s.io images import -
dlt-build:
	docker build -f engines/dlt/Dockerfile -t company-dlt-ingestion:0.6.0 .
	docker save company-dlt-ingestion:0.6.0 | docker exec -i company-spark ctr -n k8s.io images import -
airflow-build:
	docker build -f orchestration/airflow/Dockerfile -t company-airflow:0.1.0 .
	docker save company-airflow:0.1.0 | docker exec -i company-spark ctr -n k8s.io images import -
	.tools/bin/kubectl --context company-spark -n spark-lab rollout restart deployment/airflow
seed:
	python3 tools/generate-seed.py
	python3 tools/seed-sqlserver.py
oracle-up:
	python3 tools/configure-secrets.py
	.venv/bin/python tools/render-platform.py
	.tools/bin/kubectl --context company-spark apply -f infrastructure/local/kubernetes/platform.yaml
	.tools/bin/kubectl --context company-spark -n spark-lab rollout status statefulset/oracle --timeout=900s
oracle-seed:
	python3 tools/seed-oracle.py --rows $${ROWS:-1000000}
oracle-e2e:
	python3 tools/run-oracle-e2e.py --workers $${WORKERS:-4}
status:
	.tools/bin/kubectl --context company-spark get pods -A
	.tools/bin/kubectl --context company-spark -n spark-lab get sparkapplications,jobs
benchmark-report:
	PYTHONPATH=libraries/ingestion-core/src:engines/spark/job/src .venv/bin/python tools/benchmark-report.py
metrics-replay:
	PYTHONPATH=libraries/ingestion-core/src:engines/spark/job/src .venv/bin/python tools/replay-spark-metrics.py
local-stop:
	.tools/bin/minikube stop -p company-spark

# Scalable ETL and MLOps lab

This project processes reproducible synthetic customer records from CSV, SQLite, and a paginated JSON API (or offline API snapshot), trains a churn classifier, tracks experiments with MLflow, and serves predictions with Flask and Waitress. The report has five body pages plus title and references. The data are instructional; model scores do not establish business usefulness.

## Run locally

Use Python 3.11 or 3.12. On Windows, use `py` if `python` is unavailable.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pipeline.seed
python -m pipeline.etl
python -m pipeline.train
python -m pytest -q
waitress-serve --host=127.0.0.1 --port=8000 --call pipeline.api:create_app
```

On Linux/macOS activate with `source .venv/bin/activate`. Run commands at the repository root. The seed step overwrites demo sources; omit it when using your own data. The API loads the model at startup, so restart the service after promotion. Never load joblib artifacts from untrusted sources.

```powershell
Invoke-RestMethod http://localhost:8000/predict -Method Post -ContentType application/json -Body '{"tenure":12,"monthly_charge":80,"complaint_words":4}'
```

`GET /health` returns the MLflow run ID. `GET /metrics` exposes Prometheus metrics. Inputs must contain exactly the three finite, nonnegative numeric features. Missing values in training are imputed using only the training folds; serving rejects incomplete requests.

## Data and processing contract

All sources contain `customer_id`, `tenure`, `monthly_charge`, `review`, and binary `churn`. IDs must be globally unique across sources. CSV and SQL extraction use 10,000-row batches. Set `SOURCE_API_URL` to an endpoint accepting `page` and `limit`, returning `{"records":[...],"next_page":2}` and finally `next_page: null`. Network failures and repeated page tokens fail the task. Retries occur at Airflow task level. When unset, extraction reads `data/api_snapshot.json`; no external API is contacted.

Dask reads staged CSV partitions and computes at most four concurrently. pandas cleans each partition, removes invalid targets, converts malformed/negative numeric features to missing values, and derives review word count. SQLAlchemy writes a staging table inside a transaction. A primary key rejects duplicate IDs. The published table is replaced only after validation. Reruns replace a snapshot rather than append duplicate rows. A failed load preserves the previous published table. Source snapshots must remain immutable during retries; the lab staging directory is shared and permits one writer only.

SQLite is the intentional single-host sink. This demonstrates bounded-memory ETL, not a distributed database or a measured throughput claim. For larger deployments use immutable interval-specific object paths, a distributed Dask scheduler, and PostgreSQL staging/upsert operations. Do not point concurrent DAG runs at the same files. The offline snapshot is small and read into memory; live API ingestion is paginated.

## Model tracking and promotion

Training selects at most 100,000 rows in deterministic ID order. This explicit memory cap can bias a production sample; replace it with representative sampling or incremental training at scale. An 80/20 stratified split isolates holdout evaluation. Three-fold CV tunes logistic regression C over 0.1, 1, and 10. Median imputation and standardization are inside the scikit-learn pipeline to prevent leakage. MLflow records candidate CV scores, best parameters, holdout AUC/accuracy, data fingerprint, code SHA (in CI), signature, and model. AUC below 0.65 blocks local artifact publication. This lab threshold needs business validation before real use.

`artifacts/model.joblib` includes preprocessing; `metadata.json` links it to its run. These files should travel together inside an immutable container. Run `mlflow ui --backend-store-uri sqlite:///mlflow.db --host 127.0.0.1` to inspect experiments. Set `MLFLOW_TRACKING_URI` for a shared tracking service. The lab uses run IDs and image commit tags for versions; it does not configure a centralized Model Registry or production approval policy.

## Airflow scheduling

Airflow is a separate Linux/WSL2 service, not installed into the Windows lab venv. Use Python 3.11 and Airflow 2.10.5 with its official constraints:

```bash
python3.11 -m venv .airflow-venv
source .airflow-venv/bin/activate
pip install 'apache-airflow==2.10.5' --constraint 'https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt'
```

Run the core pipeline in a separate Linux venv to avoid Airflow dependency conflicts. Install `requirements.txt` there. Export `LAB_PROJECT_ROOT` as the absolute repository path and `LAB_PYTHON` as the absolute path to that venv's `bin/python` before starting Airflow. Copy `dags/churn_pipeline.py` into `$AIRFLOW_HOME/dags`, run `airflow standalone`, and enable `churn_pipeline`. Seed sources once using the core venv before enabling the DAG. The Bash tasks execute the core environment from the repository root. Both environments need access to `data/` and `artifacts/`. The DAG runs daily with two retries, no backfill, and one active run. ETL precedes training; failed ETL prevents training. This is a single-host shared-filesystem DAG. Training publishes an eligible artifact; CI builds and publishes a container on code pushes. Scheduled training alone does not restart a running API.

## CI and deployment

`.github/workflows/ci.yml` tests pull requests, then on main builds a model and image, smoke-tests `/health`, and publishes `ghcr.io/OWNER/REPOSITORY:COMMIT_SHA`. GitHub package permissions must permit publication. Pinned top-level dependencies are a compatibility baseline, not a fully locked supply chain. `requirements-lock.txt` captures the locally tested resolution; use platform-specific lock generation for Linux CI.

After local training, run `docker compose up --build -d` for API, Prometheus, and Grafana. Grafana at port 3000 provisions a dashboard and datasource; change its initial admin password at first login. Prometheus is at port 9090. Ports bind to localhost. The API container uses a single Waitress process so Prometheus counters are consistent across threads.

For automatic updates on your own Linux Docker host, configure a GitHub self-hosted runner with label `lab-deploy`, clone this repository there, and enable `.github/workflows/deploy.yml` through manual dispatch using a tested image tag. The deployment workflow pulls the immutable image, starts a candidate on port 8001, verifies health, then replaces the service on port 8000. A failed replacement restores the previous image. It is a lab deployment with brief downtime, not blue/green traffic routing. Private packages require a preauthenticated host. Only trusted maintainers should run deployment jobs on that runner.

## Monitoring and validation

Provisioned panels show request rate, invalid/error request rate, p95 latency, and the prediction probability distribution. Pipeline logs include loaded row counts and elapsed seconds; Airflow captures task failures and retry history. Probability shifts can prompt investigation but do not measure accuracy. Evaluate drift and performance with delayed ground-truth labels before retraining decisions. No alert routing or automated drift-based promotion is configured.

The tests cover full extraction/cleaning/loading, idempotence, training and serving, input rejection, metrics, and preservation of the published data after duplicate-ID failure. `VALIDATION.md` records actual execution status. Docker, GitHub Actions, and Airflow need separate runtime verification.

## Submit to GitHub

Create an empty GitHub repository, then run:

```bash
git add .
git commit -m "Implement ETL and MLOps lab"
git branch -M main
git remote add origin https://github.com/YOUR_ACCOUNT/BigDataWeek05.git
git push -u origin main
```

No remote is preconfigured. Replace placeholders; publishing is a separate step. The verified submission report is `report/DDS8530_Perez_Assignment05.pdf`. Edit the title fields in `report/build_pdf.py` and rebuild with ReportLab to personalize it. `report/Lab_Report.md` is the editable text source.

To rebuild the report, install report/requirements.txt in a separate authoring environment, edit the canonical paragraphs in report/report_content.py, then run python report/report_content.py and python report/build_pdf.py. The latter checks the seven-page total and renders page images for review. Markdown edits alone do not update the PDF.

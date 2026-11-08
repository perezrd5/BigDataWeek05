# Validation record

Executed locally on November 2, 2026, using Windows and Python 3.12.14.

## Passed

- `python -m pytest -vv -s -o faulthandler_timeout=60`: **3 passed**, 19 dependency warnings, 33.12 seconds. Tests cover API pagination and timeout propagation, cleaning invalid records, offline ETL, repeatable loading, model quality gate, serving, input rejection, metrics, and preservation of published data after duplicate-ID failure.
- `python -m pip check`: no broken requirements.
- Python compilation of `pipeline`, `dags`, and `tests`: passed. Compilation does not validate Airflow imports or scheduling.
- `python -m tools.run_lab`: completed extraction, transformation, loading, training, MLflow logging, artifact publication, and a Flask test-client prediction.

| Measurement | Observed value |
| --- | --- |
| Synthetic seed | 42 |
| Records loaded | 2,400 |
| Holdout records | 480 |
| Holdout ROC AUC | 0.777165 |
| Holdout accuracy | 0.752083 |
| Sample churn probability | 0.652372 |
| Pipeline time after imports | 22.130 seconds |
| MLflow parent run | `03f33ad5991e4f0cb568a11d0387130a` |
| Dataset SHA256 | `e1d3c4f02ae14f051a37dc3b994eef7f206443a422da3700bbdb2884221d3f60` |

Timing is a single local observation, not a benchmark. The source data are synthetic. The test-client prediction does not verify network serving. Run-specific evidence and the trained model remain locally in the ignored `artifacts/` folder; regenerate them with the README commands after cloning.

The report PDF has seven pages: one title page, five body pages, and one references page. Every page was rendered and visually inspected. The report uses 12-point Times text, 1.5 line spacing, and explicit body-page breaks. Markdown content and PDF generation scripts are included. Personal title-page fields remain blank for the student to complete.

## Docker and live monitoring verification

- Docker Compose built the API image and started the API, Prometheus, and Grafana. All three services were running, and the containerized API returned `"status":"ready"` from `/health` with MLflow run ID `c7acc4692a074b969aa7bd5c5b29bce2`.
- Prometheus reported the `churn-api` target at `http://api:8000/metrics` as **UP**. [Prometheus target screenshot](evidence/prometheus-target-up.png)
- After sending test predictions to the containerized API, Grafana displayed the prediction request rate, estimated p95 latency of approximately 35–46 ms, and prediction probability buckets. [Grafana dashboard screenshot](evidence/grafana-churn-dashboard.png)

## Airflow verification

- In Ubuntu on WSL 2, Airflow 2.10.5 successfully ran the `churn_pipeline` DAG after the demo inputs were generated. The manual run completed both `etl` and `train`. [Airflow manual run screenshot](evidence/airflow-manual-run.png)

## Not executed here

- A live external data API. HTTP pagination and timeout behavior were tested using mocked responses.
- GitHub Actions, GHCR publication, self-hosted deployment, or rollback on a host.

Top-level dependency versions are pinned. `requirements-lock.txt` captures the tested Windows resolution. Third-party warnings include Dask/NumPy and MLflow/Pydantic deprecations and a scikit-learn/SciPy solver-option warning. They did not fail the tests, but a maintained production dependency set should resolve them and be tested on the deployment platform.



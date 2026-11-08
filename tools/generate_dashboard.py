"""Generate the provisioned Grafana dashboard."""
import json
from pathlib import Path

panels = []
for index, (title, query) in enumerate([
    ('Prediction request rate', 'sum(rate(prediction_requests_total[5m]))'),
    ('Invalid and failed request rate', 'sum(rate(prediction_requests_total{status!="ok"}[5m]))'),
    ('Prediction latency p95', 'histogram_quantile(0.95, sum by (le) (rate(prediction_latency_seconds_bucket[5m])))'),
    ('Prediction probability distribution', 'sum by (le) (rate(prediction_probability_bucket[5m]))')
]):
    panels.append({'id': index+1, 'type': 'timeseries', 'title': title, 'datasource': {'type': 'prometheus', 'uid': 'prometheus'}, 'gridPos': {'x': index % 2*12, 'y': index//2*8, 'w': 12, 'h': 8}, 'targets': [{'expr': query, 'refId': 'A'}]})
Path('monitoring/dashboard.json').write_text(json.dumps({'uid': 'churn-lab', 'title': 'Churn API Monitoring', 'schemaVersion': 39, 'version': 1, 'refresh': '15s', 'time': {'from': 'now-30m', 'to': 'now'}, 'panels': panels}, indent=2))

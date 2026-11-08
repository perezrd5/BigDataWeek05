import json
import logging
import math
from pathlib import Path
from time import perf_counter
import joblib
import pandas as pd
from flask import Flask, Response, jsonify, request
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pipeline.etl import FEATURES

REQUESTS = Counter('prediction_requests_total', 'Prediction requests', ['status'])
LATENCY = Histogram('prediction_latency_seconds', 'Prediction latency')
PROBABILITY = Histogram('prediction_probability', 'Predicted probability', buckets=[.1,.25,.5,.75,.9,1])

def create_app(directory=Path('artifacts')):
    app = Flask(__name__)
    app.config['MAX_CONTENT_LENGTH'] = 65536
    model = joblib.load(directory/'model.joblib')
    metadata = json.loads((directory/'metadata.json').read_text())

    @app.get('/health')
    def health():
        return jsonify(status='ready', run_id=metadata['run_id'])

    @app.get('/metrics')
    def metrics():
        return Response(generate_latest(), content_type=CONTENT_TYPE_LATEST)

    @app.post('/predict')
    def predict():
        start = perf_counter()
        try:
            body = request.get_json(silent=True)
            if not isinstance(body, dict) or set(body) != set(FEATURES):
                raise ValueError('Supply exactly tenure, monthly_charge, complaint_words')
            for value in body.values():
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                    raise ValueError('Features must be finite nonnegative numbers')
            probability = float(model.predict_proba(pd.DataFrame([body], columns=FEATURES))[0, 1])
            REQUESTS.labels('ok').inc()
            PROBABILITY.observe(probability)
            return jsonify(churn_probability=probability, run_id=metadata['run_id'])
        except ValueError as error:
            REQUESTS.labels('invalid').inc()
            return jsonify(error=str(error)), 400
        except Exception:
            REQUESTS.labels('error').inc()
            logging.exception('Prediction failed')
            return jsonify(error='Prediction failed'), 500
        finally:
            LATENCY.observe(perf_counter()-start)
    return app

if __name__ == '__main__':
    create_app().run(host='0.0.0.0', port=8000)

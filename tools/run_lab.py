"""Run the offline example and save measured validation evidence."""
import json
import logging
from pathlib import Path
from time import perf_counter
from pipeline import seed, etl, train
from pipeline.api import create_app

logging.basicConfig(level=logging.INFO)
start = perf_counter()
seed.run()
rows = etl.run()
result = train.run()
client = create_app().test_client()
prediction = client.post('/predict', json={'tenure': 12, 'monthly_charge': 80, 'complaint_words': 4})
assert prediction.status_code == 200
evidence = {'rows_loaded': rows, **result, 'sample_prediction': prediction.json, 'elapsed_seconds': round(perf_counter()-start, 3), 'dataset': 'synthetic seed 42'}
Path('artifacts/validation.json').write_text(json.dumps(evidence, indent=2))
print(json.dumps(evidence, indent=2))

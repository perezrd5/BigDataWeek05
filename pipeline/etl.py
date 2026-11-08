"""Partitioned extraction and transactional SQLite snapshot publication."""
import logging
import os
from pathlib import Path
from time import perf_counter
import dask.dataframe as dd
from dask import compute
import pandas as pd
import requests
from sqlalchemy import create_engine, text

LOG = logging.getLogger(__name__)
FEATURES = ['tenure', 'monthly_charge', 'complaint_words']

def clean(frame):
    frame = frame.copy()
    for col in ['tenure', 'monthly_charge', 'churn']:
        frame[col] = pd.to_numeric(frame[col], errors='coerce')
    frame = frame[frame.churn.isin([0, 1]) & frame.customer_id.notna()]
    for col in ['tenure', 'monthly_charge']:
        frame.loc[frame[col] < 0, col] = float('nan')
    frame['complaint_words'] = frame['review'].fillna('').astype(str).str.split().str.len().astype(float)
    return frame[['customer_id', *FEATURES, 'churn']]

def extract(root):
    """CSV and SQL stream in batches; API uses an explicit page contract."""
    raw = root / 'raw'
    raw.mkdir(parents=True, exist_ok=True)
    for old in raw.glob('part-*.csv'):
        old.unlink()
    index = 0
    def save(frame):
        nonlocal index
        frame.to_csv(raw / f'part-{index:06d}.csv', index=False)
        index += 1
    for frame in pd.read_csv(root / 'customers.csv', chunksize=10000):
        save(frame)
    with create_engine(f'sqlite:///{root / "source.db"}').connect() as conn:
        for frame in pd.read_sql(text('SELECT * FROM customers'), conn, chunksize=10000):
            save(frame)
    url = os.getenv('SOURCE_API_URL')
    if url:
        # Contract: {"records": [...], "next_page": int | null}.
        page, seen = 1, set()
        while page is not None:
            if page in seen:
                raise ValueError('Repeated API pagination token')
            seen.add(page)
            response = requests.get(url, params={'page': page, 'limit': 10000}, timeout=30)
            response.raise_for_status()
            payload = response.json()
            if payload['records']:
                save(pd.DataFrame(payload['records']))
            page = payload['next_page']
    else:
        # Offline snapshot has identical schema, including unstructured review text.
        save(pd.read_json(root / 'api_snapshot.json'))
    return raw

def run(root=Path('data')):
    started = perf_counter()
    raw = extract(root)
    frame = dd.read_csv(str(raw / 'part-*.csv'), blocksize='16MiB', dtype={'customer_id': 'string', 'review': 'string', 'tenure': 'object', 'monthly_charge': 'object', 'churn': 'object'})
    meta = {'customer_id': 'string', 'tenure': 'float64', 'monthly_charge': 'float64', 'complaint_words': 'float64', 'churn': 'float64'}
    cleaned = frame.map_partitions(clean, meta=meta)
    engine = create_engine(f'sqlite:///{root / "warehouse.db"}')
    rows = 0
    # Sequential sink bounds memory; Dask partitions parallelize upstream work.
    with engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS staging'))
        conn.execute(text('CREATE TABLE staging (customer_id TEXT PRIMARY KEY, tenure REAL, monthly_charge REAL, complaint_words REAL, churn INTEGER)'))
        partitions = cleaned.to_delayed()
        for offset in range(0, len(partitions), 4):
            for batch in compute(*partitions[offset:offset+4], scheduler='threads'):
                if not batch.empty:
                    batch.to_sql('staging', conn, if_exists='append', index=False)
                    rows += len(batch)
        if rows < 100:
            raise ValueError('Insufficient valid rows')
        conn.execute(text('DROP TABLE IF EXISTS processed'))
        conn.execute(text('ALTER TABLE staging RENAME TO processed'))
    LOG.info('etl_complete rows=%d seconds=%.3f', rows, perf_counter()-started)
    return rows

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()

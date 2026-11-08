"""Deterministic synthetic sources; no claim of real customer performance."""
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

def run(root=Path('data')):
    root.mkdir(exist_ok=True, parents=True)
    rng = np.random.default_rng(42)
    n = 2400
    tenure = rng.integers(1, 73, n)
    charge = rng.uniform(20, 130, n)
    complaints = rng.integers(0, 2, n)
    probability = 1 / (1 + np.exp(-(-1 + charge/65 - tenure/22 + complaints)))
    frame = pd.DataFrame({'customer_id': [f'C{i}' for i in range(n)], 'tenure': tenure.astype(float), 'monthly_charge': charge, 'review': np.where(complaints, 'service failed please help', 'service works'), 'churn': rng.binomial(1, probability)})
    frame.loc[::31, 'tenure'] = np.nan
    frame.iloc[:1200].to_csv(root/'customers.csv', index=False)
    frame.iloc[1200:2000].to_sql('customers', create_engine(f'sqlite:///{root / "source.db"}'), if_exists='replace', index=False)
    frame.iloc[2000:].to_json(root/'api_snapshot.json', orient='records')

if __name__ == '__main__':
    run()

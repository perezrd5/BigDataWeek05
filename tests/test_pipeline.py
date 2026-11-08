from pathlib import Path
import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from pipeline import seed, etl, train
from pipeline.api import create_app

def test_api_pagination(tmp_path, monkeypatch):
    seed.run(tmp_path)
    monkeypatch.setenv('SOURCE_API_URL', 'https://example.invalid/customers')
    calls = []
    class Response:
        def raise_for_status(self):
            pass
        def json(self):
            page = calls[-1]
            return {'records': [{'customer_id': f'api-{page}', 'tenure': 1, 'monthly_charge': 40, 'review': 'help', 'churn': 0}], 'next_page': 2 if page == 1 else None}
    def get(url, params, timeout):
        calls.append(params['page'])
        assert timeout == 30
        return Response()
    monkeypatch.setattr(etl.requests, 'get', get)
    etl.extract(tmp_path)
    assert calls == [1, 2]
    def fail(*args, **kwargs):
        raise etl.requests.Timeout('test timeout')
    monkeypatch.setattr(etl.requests, 'get', fail)
    with pytest.raises(etl.requests.Timeout):
        etl.extract(tmp_path)

def test_clean_invalid_records():
    frame = pd.DataFrame({'customer_id': ['a','b','c'], 'tenure': [-1, 2, 3], 'monthly_charge': ['bad', 4, 5], 'review': [None, 'two words', 'x'], 'churn': [1, 0, 2]})
    result = etl.clean(frame)
    assert len(result) == 2
    assert pd.isna(result.iloc[0].tenure)
    assert pd.isna(result.iloc[0].monthly_charge)
    assert result.complaint_words.tolist() == [0, 2]

def test_full_pipeline(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    seed.run()
    assert etl.run() == 2400
    assert etl.run() == 2400
    metadata = train.run()
    assert metadata['test_auc'] >= .65
    app = create_app().test_client()
    assert app.get('/health').status_code == 200
    prediction = app.post('/predict', json={'tenure': 12, 'monthly_charge': 80, 'complaint_words': 4})
    assert 0 <= prediction.json['churn_probability'] <= 1
    assert app.post('/predict', json={'tenure': -1}).status_code == 400
    assert b'prediction_requests_total' in app.get('/metrics').data
    # Duplicate source identifiers abort loading and preserve published data.
    frame = pd.read_csv('data/customers.csv')
    pd.concat([frame, frame.iloc[:1]]).to_csv('data/customers.csv', index=False)
    with pytest.raises(Exception):
        etl.run()
    with create_engine('sqlite:///data/warehouse.db').connect() as conn:
        assert conn.execute(text('SELECT COUNT(*) FROM processed')).scalar() == 2400

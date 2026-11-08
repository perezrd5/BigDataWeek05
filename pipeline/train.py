import hashlib
import json
import os
from pathlib import Path
import joblib
import mlflow
import mlflow.sklearn
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import GridSearchCV, train_test_split, StratifiedKFold
from sklearn.metrics import roc_auc_score, accuracy_score
from pipeline.etl import FEATURES

def run(root=Path('data'), output=Path('artifacts')):
    with create_engine(f'sqlite:///{root / "warehouse.db"}').connect() as conn:
        # Intentional bounded training subset; ETL remains partitioned.
        frame = pd.read_sql(text('SELECT * FROM processed ORDER BY customer_id LIMIT 100000'), conn)
    X, y = frame[FEATURES], frame.churn.astype(int)
    Xtrain, Xtest, ytrain, ytest = train_test_split(X, y, stratify=y, test_size=.2, random_state=42)
    search = GridSearchCV(make_pipeline(SimpleImputer(strategy='median'), StandardScaler(), LogisticRegression(max_iter=1000)), {'logisticregression__C': [.1, 1, 10]}, scoring='roc_auc', cv=StratifiedKFold(3, shuffle=True, random_state=42))
    mlflow.set_tracking_uri(os.getenv('MLFLOW_TRACKING_URI', 'sqlite:///mlflow.db'))
    mlflow.set_experiment('customer-churn')
    output.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(pd.util.hash_pandas_object(frame, index=True).values.tobytes()).hexdigest()
    with mlflow.start_run() as parent:
        search.fit(Xtrain, ytrain)
        for params, score in zip(search.cv_results_['params'], search.cv_results_['mean_test_score']):
            with mlflow.start_run(nested=True):
                mlflow.log_params(params)
                mlflow.log_metric('cv_auc', score)
        model = search.best_estimator_
        metrics = {'test_auc': roc_auc_score(ytest, model.predict_proba(Xtest)[:, 1]), 'test_accuracy': accuracy_score(ytest, model.predict(Xtest))}
        mlflow.log_metrics(metrics)
        mlflow.log_params(search.best_params_)
        mlflow.set_tags({'data_sha256': digest, 'git_sha': os.getenv('GITHUB_SHA', 'local')})
        mlflow.sklearn.log_model(model, 'model', signature=mlflow.models.infer_signature(Xtrain, model.predict(Xtrain)))
        if metrics['test_auc'] < .65:
            raise ValueError(f'Model quality gate failed: {metrics}')
        metadata = {**metrics, 'run_id': parent.info.run_id, 'data_sha256': digest, 'features': FEATURES}
        joblib.dump(model, output/'model.tmp')
        os.replace(output/'model.tmp', output/'model.joblib')
        (output/'metadata.json').write_text(json.dumps(metadata, indent=2))
    return metadata

if __name__ == '__main__':
    print(json.dumps(run(), indent=2))

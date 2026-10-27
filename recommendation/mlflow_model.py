"""MLflow model flavor for both sale/rent recommendation indices."""
import json
import mlflow.pyfunc
import pandas as pd
from recommendation.engine import RecommendationEngine


class RecommendationModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        self.engine = RecommendationEngine(context.artifacts['bundle'])

    def predict(self, context, model_input, params=None):
        output = []
        for row in model_input.to_dict('records'):
            method = row['method']
            if method not in ('similar', 'preferences'):
                raise ValueError('method must be similar or preferences')
            results = getattr(self.engine, method)(**json.loads(row['request_json']))
            output.append({'result_json': json.dumps(results, ensure_ascii=False)})
        return pd.DataFrame(output)

"""Price-per-m2 building blocks of `price_model`.

Used by scripts/train_price_model.py and logged next to the model (MLflow
`code_paths`), so the API can unpickle the model without importing this repo.
Chosen in notebooks/price_model_improvements.ipynb: MAPE 20.21% -> 19.73%.
"""

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, RegressorMixin, TransformerMixin, clone
from sklearn.model_selection import KFold

MISSING = "__missing__"

# Neighbourhood groups whose average price/m2 becomes a feature. A street name
# is only meaningful inside its district, hence district_id in that key.
AREA_GROUPS = {
    "ward_type": ["ward_id", "property_type"],
    "street_type": ["district_id", "street", "property_type"],
    "project": ["project_name"],
    "district_subtype": ["district_id", "property_subtype"],
}
AREA_COLUMNS = sorted({c for cols in AREA_GROUPS.values() for c in cols})


class AreaPriceEncoder(BaseEstimator, TransformerMixin):
    """Smoothed mean of y (log price/m2) per neighbourhood group, plus group size.

    mean = (sum(y) + smoothing * prior) / (n + smoothing), where prior is the
    mean of the listing's property_type, so small groups lean on the prior.
    fit_transform is out-of-fold (like sklearn's TargetEncoder): a training
    row never sees its own price. transform uses every fitted row.
    """

    def __init__(self, smoothing=5.0, folds=5, random_state=42):
        self.smoothing = smoothing
        self.folds = folds
        self.random_state = random_state

    def fit(self, X, y):
        self.stats_ = self._stats(X, y)
        return self

    def fit_transform(self, X, y):
        self.fit(X, y)
        y = np.asarray(y, dtype=float)
        out = np.empty((len(X), 2 * len(AREA_GROUPS)))
        folds = KFold(self.folds, shuffle=True, random_state=self.random_state)
        for fit_rows, apply_rows in folds.split(X):
            stats = self._stats(X.iloc[fit_rows], y[fit_rows])
            out[apply_rows] = self._apply(X.iloc[apply_rows], stats).to_numpy()
        return pd.DataFrame(out, index=X.index, columns=self.get_feature_names_out())

    def transform(self, X):
        return self._apply(X, self.stats_)

    def get_feature_names_out(self, input_features=None):
        return np.array([f"{kind}_{name}" for name in AREA_GROUPS for kind in ("ppm2", "n")])

    @staticmethod
    def _clean(X):
        return X[AREA_COLUMNS].replace(MISSING, np.nan)

    def _stats(self, X, y):
        frame = self._clean(X)
        y = pd.Series(np.asarray(y, dtype=float), index=X.index)
        groups = {
            name: y.groupby([frame[c] for c in cols]).agg(["sum", "count"])
            for name, cols in AREA_GROUPS.items()
        }
        return groups, y.groupby(frame["property_type"]).mean(), float(y.mean())

    def _apply(self, X, stats):
        groups, prior_by_type, overall = stats
        frame = self._clean(X)
        prior = frame["property_type"].map(prior_by_type).fillna(overall).to_numpy(dtype=float)
        out = {}
        for name, cols in AREA_GROUPS.items():
            keys = (pd.MultiIndex.from_frame(frame[cols]) if len(cols) > 1
                    else pd.Index(frame[cols[0]]))
            matched = groups[name].reindex(keys)
            n = matched["count"].fillna(0).to_numpy(dtype=float)
            total = matched["sum"].fillna(0).to_numpy(dtype=float)
            out[f"ppm2_{name}"] = np.where(
                n > 0, (total + self.smoothing * prior) / (n + self.smoothing), np.nan)
            out[f"n_{name}"] = n
        return pd.DataFrame(out, index=X.index)


class PricePerM2Regressor(BaseEstimator, RegressorMixin):
    """Fits `pipeline` on log(price / area_m2) and predicts log(price).

    Callers (gate, API, prediction table) keep using exp(predict(X)) as the
    price in VND, so they work with old and new model versions alike.
    """

    def __init__(self, pipeline):
        self.pipeline = pipeline

    def fit(self, X, y):
        log_area = np.log(X["area_m2"].to_numpy(dtype=float))
        self.pipeline_ = clone(self.pipeline).fit(X, np.asarray(y, dtype=float) - log_area)
        return self

    def predict(self, X):
        return self.pipeline_.predict(X) + np.log(X["area_m2"].to_numpy(dtype=float))

    @property
    def named_steps(self):
        # The fitted steps once fitted, so logging code reads them like a Pipeline.
        return getattr(self, "pipeline_", self.pipeline).named_steps

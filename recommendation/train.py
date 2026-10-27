"""Fit independent TF-IDF vocabularies/indices for sale and rent, atomically."""
import argparse
import gc
import json
import os
import platform
import shutil
import tempfile
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from scipy.sparse import save_npz
from sklearn.feature_extraction.text import TfidfVectorizer

from .contract import (SCHEMA_VERSION, DEFAULT_WEIGHTS, prepare, eligible, utc,
                       file_hash, key_hash)


def train(input_path, output, *, dataset_version, as_of=None, max_age_days=90,
          max_features=30000, min_df=2):
    input_path, output = Path(input_path), Path(output)
    if not dataset_version.strip() or max_features < 100 or min_df < 1:
        raise ValueError('Specify a dataset version, max_features >= 100 and min_df >= 1')
    if output.exists():
        raise FileExistsError('Use a new immutable model version directory')
    df = prepare(pd.read_parquet(input_path))
    now = utc(as_of)
    selected = df.loc[eligible(df, now, max_age_days)].copy()
    if set(selected.transaction_type) != {'sale', 'rent'}:
        raise ValueError('Training requires eligible rows for BOTH sale and rent')
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='.recommendation-', dir=output.parent))
    manifest = dict(schema_version=SCHEMA_VERSION, dataset_version=dataset_version,
                    source_sha256=file_hash(input_path), as_of=now.isoformat(),
                    max_age_days=max_age_days, max_features=max_features, min_df=min_df,
                    python=platform.python_version(), sklearn=sklearn.__version__,
                    input_rows=len(df), eligible_rows=len(selected), markets={},
                    weights={m: DEFAULT_WEIGHTS.copy() for m in ['sale', 'rent']},
                    quality_status='not_evaluated_with_human_labels')
    try:
        for market in ['sale', 'rent']:
            started = time.perf_counter()
            sub = selected.loc[selected.transaction_type.eq(market)].sort_values('listing_key').reset_index(drop=True)
            directory = stage / market
            directory.mkdir()
            vec = TfidfVectorizer(analyzer='char_wb', ngram_range=(3, 5),
                                  max_features=max_features, min_df=min(min_df, len(sub)),
                                  sublinear_tf=True, dtype=np.float32, norm='l2')
            X = vec.fit_transform(sub.content_text)
            sub.to_parquet(directory / 'catalog.parquet', index=False)
            save_npz(directory / 'matrix.npz', X)
            joblib.dump(vec, directory / 'vectorizer.joblib')
            manifest['markets'][market] = dict(
                rows=len(sub), features=X.shape[1], key_order_sha256=key_hash(sub.listing_key),
                fit_seconds=round(time.perf_counter() - started, 3),
                files={p.name: file_hash(p) for p in sorted(directory.iterdir())})
            print(json.dumps({'market': market, **{k: v for k, v in manifest['markets'][market].items()
                                                  if k != 'files'}}), flush=True)
            del X, vec, sub
            gc.collect()
        (stage / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
        os.rename(stage, output)
    except BaseException:
        shutil.rmtree(stage)
        raise
    return manifest


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--dataset-version', required=True)
    p.add_argument('--as-of')
    p.add_argument('--max-age-days', type=int, default=90)
    p.add_argument('--max-features', type=int, default=30000)
    p.add_argument('--min-df', type=int, default=2)
    a = p.parse_args()
    result = train(a.input, a.output, dataset_version=a.dataset_version, as_of=a.as_of,
                   max_age_days=a.max_age_days, max_features=a.max_features, min_df=a.min_df)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

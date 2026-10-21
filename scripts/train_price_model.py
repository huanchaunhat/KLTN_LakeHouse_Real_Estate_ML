#!/usr/bin/env python3
"""Train the sale-price model from gold.ml_price_features.

Input : gold.ml_price_features, read through the Hive catalog (the Delta
        version that was read is logged to MLflow).
Output: an MLflow run + a version of the registered model `price_model`
        (alias `champion` only when it passes the quality gate), and
        gold.ml_price_predictions with the candidate's predictions.

The split is by posted_date: the newest --test-days are held out, the model
is trained on older listings only and is deployed as trained (no refit), so
comparing it to the current champion on the same holdout is out-of-time for
both models.
"""

import argparse
import json
import os
import tempfile

import mlflow
import numpy as np
import pandas as pd
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import OrdinalEncoder, TargetEncoder

FEATURE_TABLE = "gold.ml_price_features"
PREDICTION_TABLE = "gold.ml_price_predictions"
EXPERIMENT = "price_prediction"
MODEL_NAME = "price_model"
CHAMPION_ALIAS = "champion"
TARGET = "log_price_vnd"

# Feature groups. The API builds its input frame from this same spec
# (logged as feature_spec.json), so the two cannot drift.
NUMERIC = [
    "area_m2", "living_area_m2", "width_m", "length_m",
    "bedrooms", "bathrooms", "total_floors", "floor_number",
    "latitude", "longitude",
]
BOOLEAN = [
    "is_in_project", "has_car_alley", "is_frontage", "is_wide_back",
    "is_narrow_back", "is_dilapidated", "is_not_completed",
    "is_planning_affected", "has_certificate",
]
# Few levels: native categorical splits in the boosted trees.
LOW_CARD = [
    "district_id", "area_zone", "district_type", "property_type",
    "property_subtype", "house_direction", "furnishing",
    "handover_status", "legal_category",
]
# Too many levels for native splits (HGB allows <= 255): cross-fitted target
# encoding, so a row's own price never leaks into its encoding.
HIGH_CARD = ["ward_id", "street", "project_name"]
CATEGORICAL = LOW_CARD + HIGH_CARD
FEATURES = NUMERIC + BOOLEAN + CATEGORICAL
MISSING = "__missing__"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True, help="Airflow run id, for lineage")
    parser.add_argument("--test-days", type=int, default=10)
    parser.add_argument("--min-train-rows", type=int, default=5000)
    parser.add_argument("--min-test-rows", type=int, default=1000)
    parser.add_argument(
        "--max-mape-regression", type=float, default=0.0,
        help="Promote if MAPE <= champion MAPE * (1 + this)",
    )
    return parser.parse_args()


def create_spark():
    return (
        SparkSession.builder
        .appName("REIP-Train-Price-Model")
        .enableHiveSupport()
        .getOrCreate()
    )


def load_features(spark):
    version = (
        spark.sql(f"DESCRIBE HISTORY {FEATURE_TABLE} LIMIT 1")
        .select("version").first()["version"]
    )
    # Read the exact version recorded above, even if dbt rewrites the table meanwhile.
    location = spark.sql(f"DESCRIBE DETAIL {FEATURE_TABLE}").first()["location"]
    rows = (
        spark.read.format("delta").option("versionAsOf", version).load(location)
        .withColumn("district_id", F.col("district_id").cast("string"))
        .withColumn("ward_id", F.col("ward_id").cast("string"))
        .collect()
    )
    # Not toPandas(): Spark 3.3 still uses np.bool there, removed in numpy 1.24.
    df = pd.DataFrame.from_records([r.asDict() for r in rows])
    return df, int(version)


def prepare(df):
    """Cast to the dtypes the pipeline expects. Shared contract with the API."""
    out = df.copy()
    for c in NUMERIC:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
    for c in BOOLEAN:
        out[c] = out[c].map({True: 1.0, False: 0.0}).astype("float64")
    for c in CATEGORICAL:
        out[c] = out[c].astype("object").where(out[c].notna(), MISSING).astype(str)
    return out[FEATURES]


def build_pipeline(max_iter=2000, early_stopping=True):
    low_card = make_pipeline(
        OrdinalEncoder(
            handle_unknown="use_encoded_value", unknown_value=np.nan,
            dtype=np.float64,
        ),
    )
    high_card = make_pipeline(
        SimpleImputer(strategy="constant", fill_value=MISSING),
        TargetEncoder(target_type="continuous", random_state=42),
    )
    prep = ColumnTransformer(
        [
            ("num", "passthrough", NUMERIC + BOOLEAN),
            ("low", low_card, LOW_CARD),
            ("high", high_card, HIGH_CARD),
        ],
        verbose_feature_names_out=False,
    ).set_output(transform="pandas")
    model = HistGradientBoostingRegressor(
        loss="squared_error",
        learning_rate=0.05,
        max_iter=max_iter,
        max_leaf_nodes=63,
        min_samples_leaf=20,
        l2_regularization=1.0,
        categorical_features=LOW_CARD,
        early_stopping=early_stopping,
        validation_fraction=0.1,
        n_iter_no_change=50,
        random_state=42,
    )
    return Pipeline([("prep", prep), ("model", model)])


def baseline_predict(train, test):
    """Median price per m2 of (district, property type) x area."""
    ppm2 = train["price_vnd"] / train["area_m2"]
    by_segment = ppm2.groupby([train["district_id"], train["property_type"]]).median()
    by_type = ppm2.groupby(train["property_type"]).median()
    keys = pd.MultiIndex.from_frame(test[["district_id", "property_type"]])
    rate = pd.Series(by_segment.reindex(keys).to_numpy(), index=test.index)
    rate = rate.fillna(test["property_type"].map(by_type)).fillna(ppm2.median())
    return rate * test["area_m2"]


def metrics(actual, predicted, prefix):
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    ape = np.abs(predicted - actual) / actual
    return {
        f"{prefix}_mape": float(ape.mean()),
        f"{prefix}_mdape": float(np.median(ape)),
        f"{prefix}_mae_billion_vnd": float(mean_absolute_error(actual, predicted) / 1e9),
        f"{prefix}_r2_log": float(r2_score(np.log(actual), np.log(predicted))),
        f"{prefix}_within_10pct": float((ape <= 0.10).mean()),
        f"{prefix}_within_20pct": float((ape <= 0.20).mean()),
    }


def segment_report(test, predicted):
    ape = (predicted - test["price_vnd"]).abs() / test["price_vnd"]
    frame = test[["property_type", "district"]].assign(ape=ape.to_numpy())
    return (
        frame.groupby("property_type")["ape"]
        .agg(rows="size", mape="mean", mdape="median")
        .reset_index()
    )


def champion_mape(client, x_test, y_test):
    """MAPE of the current champion on today's holdout, or None if there is none."""
    try:
        version = client.get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS)
    except MlflowException:
        return None, None
    model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}")
    predicted = np.exp(model.predict(x_test))
    return metrics(y_test, predicted, "champion")["champion_mape"], version.version


PREDICTION_SCHEMA = (
    "listing_key string, posted_date date, district string, property_type string, "
    "area_m2 double, price_vnd double, predicted_price_vnd double, "
    "abs_pct_error double, split string"
)


def write_predictions(spark, df, predicted, split, run_id, model_version):
    price = df["price_vnd"].astype(float)
    predicted = pd.Series(np.asarray(predicted, dtype=float), index=df.index)
    columns = [
        df["listing_key"].astype(str),
        df["posted_date"].dt.date,
        df["district"],
        df["property_type"],
        df["area_m2"].astype(float),
        price,
        predicted,
        (predicted - price).abs() / price,
        split,
    ]
    # Plain Python rows: Spark 3.3's pandas conversion predates pandas 2.
    rows = list(zip(*(c.tolist() for c in columns)))
    (
        spark.createDataFrame(rows, PREDICTION_SCHEMA)
        .withColumn("mlflow_run_id", F.lit(run_id))
        .withColumn("model_version", F.lit(str(model_version)))
        .withColumn("scored_at", F.current_timestamp())
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(PREDICTION_TABLE)
    )


def main():
    args = parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    df, data_version = load_features(spark)
    df["posted_date"] = pd.to_datetime(df["posted_date"])
    cutoff = df["posted_date"].max() - pd.Timedelta(days=args.test_days)
    is_test = df["posted_date"] > cutoff
    train, test = df[~is_test], df[is_test]
    print(f"rows={len(df)} train={len(train)} test={len(test)} cutoff={cutoff.date()} "
          f"data_version={data_version}")
    if len(train) < args.min_train_rows or len(test) < args.min_test_rows:
        raise SystemExit(f"Not enough rows: train={len(train)}, test={len(test)}")

    x_train, x_test = prepare(train), prepare(test)
    y_train = train[TARGET].astype(float)

    mlflow.set_experiment(EXPERIMENT)
    client = MlflowClient()
    with mlflow.start_run(run_name=args.run_id) as run:
        mlflow.set_tags({
            "airflow_run_id": args.run_id,
            "feature_table": FEATURE_TABLE,
            "feature_table_delta_version": data_version,
            "split": "posted_date",
        })
        mlflow.log_params({
            "rows_total": len(df), "rows_train": len(train), "rows_test": len(test),
            "test_days": args.test_days, "cutoff_posted_date": str(cutoff.date()),
            "train_posted_from": str(train["posted_date"].min().date()),
            "test_posted_to": str(test["posted_date"].max().date()),
        })

        pipeline = build_pipeline()
        pipeline.fit(x_train, y_train)
        hgb = pipeline.named_steps["model"]
        mlflow.log_params({
            k: v for k, v in hgb.get_params().items()
            if k in {"learning_rate", "max_leaf_nodes", "min_samples_leaf",
                     "l2_regularization", "max_iter"}
        })
        mlflow.log_param("n_iter", hgb.n_iter_)

        test_pred = np.exp(pipeline.predict(x_test))
        results = metrics(test["price_vnd"], test_pred, "test")
        results.update(metrics(test["price_vnd"], baseline_predict(train, test), "baseline"))
        results.update(metrics(
            train["price_vnd"], np.exp(pipeline.predict(x_train)), "train"))

        champ_mape, champ_version = champion_mape(client, x_test, test["price_vnd"])
        if champ_mape is not None:
            results["champion_mape"] = champ_mape
        mlflow.log_metrics(results)

        with tempfile.TemporaryDirectory() as tmp:
            spec = {
                "numeric": NUMERIC, "boolean": BOOLEAN, "categorical": CATEGORICAL,
                "features": FEATURES, "missing_token": MISSING, "target": TARGET,
                "target_transform": "log",
            }
            with open(os.path.join(tmp, "feature_spec.json"), "w") as f:
                json.dump(spec, f, indent=2)
            # Lets the API derive district-level features from district_id alone.
            districts = (
                df[["district_id", "district", "district_type", "area_zone"]]
                .drop_duplicates("district_id").sort_values("district_id")
            )
            districts.to_json(os.path.join(tmp, "districts.json"),
                              orient="records", force_ascii=False, indent=2)
            segment_report(test, test_pred).to_csv(
                os.path.join(tmp, "test_by_property_type.csv"), index=False)
            mlflow.log_artifacts(tmp)

        mlflow.sklearn.log_model(
            pipeline, artifact_path="model",
            registered_model_name=MODEL_NAME,
            input_example=x_test.head(3),
        )
        new_version = client.search_model_versions(
            f"run_id='{run.info.run_id}'")[0].version

        # Quality gate: beat the naive baseline, and do not regress against
        # the champion on the same out-of-time holdout.
        beats_baseline = results["test_mape"] < results["baseline_mape"]
        beats_champion = (
            champ_mape is None
            or results["test_mape"] <= champ_mape * (1 + args.max_mape_regression)
        )
        promoted = beats_baseline and beats_champion
        reason = (
            "promoted" if promoted
            else "worse than baseline" if not beats_baseline
            else f"worse than champion v{champ_version}"
        )
        if promoted:
            client.set_registered_model_alias(MODEL_NAME, CHAMPION_ALIAS, new_version)
        client.set_model_version_tag(MODEL_NAME, new_version, "gate", reason)
        client.set_model_version_tag(MODEL_NAME, new_version, "test_mape",
                                     f"{results['test_mape']:.4f}")
        mlflow.set_tags({"gate": reason, "model_version": new_version})

        split = pd.Series(np.where(is_test, "test", "train"), index=df.index)
        all_pred = np.exp(pipeline.predict(prepare(df)))
        write_predictions(spark, df, all_pred, split, run.info.run_id, new_version)

    print(json.dumps({
        "model_version": new_version, "gate": reason,
        "champion_version_before": champ_version,
        **{k: round(v, 4) for k, v in results.items()},
    }, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()

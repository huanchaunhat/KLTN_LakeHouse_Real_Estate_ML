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
import time
from datetime import datetime

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


TOTAL_STEPS = 9
_STARTED_AT = time.time()
# log_step() updates this so every following line says which step it belongs to.
_CURRENT = {"step": 0, "name": "INIT"}


def _emit(line):
    # Write one fully prefixed line to stdout.
    #
    # SparkSubmitOperator runs this script in client mode, so the driver is a
    # child process of the Airflow worker and SparkSubmitHook forwards every
    # line of its stdout to the task log via self.log.info(line). Two results:
    #   * stdout is a pipe and therefore block-buffered, so flush=True is
    #     required or nothing appears in Airflow until the process exits.
    #   * the hook logs line by line, so a multi-line message would lose its
    #     prefix on all lines but the first; log_block() prefixes each line.
    elapsed = time.time() - _STARTED_AT
    clock = datetime.now().strftime("%H:%M:%S")
    print(
        "{} +{:6.1f}s | {}/{} {:<11} | {}".format(
            clock, elapsed, _CURRENT["step"], TOTAL_STEPS,
            _CURRENT["name"], line),
        flush=True,
    )


def log_step(step, name, message):
    # Open a section. Everything logged afterwards is tagged with it.
    _CURRENT["step"], _CURRENT["name"] = step, name
    _emit("=" * 70)
    _emit("STEP {}/{} - {}: {}".format(step, TOTAL_STEPS, name, message))
    _emit("=" * 70)


def log(message):
    # One detail line inside the current section.
    _emit("  " + str(message))


def log_block(title, text):
    # Multi-line detail (DataFrame dumps, JSON): keep the prefix on every line.
    _emit("  " + title)
    for line in str(text).splitlines():
        _emit("    | " + line)


def log_frame(frame, label, columns=None, head=5):
    # Compact DataFrame overview: shape plus a few key columns, never the lot.
    log("{}: {:,} rows x {} columns".format(label, len(frame), len(frame.columns)))
    if columns:
        available = [c for c in columns if c in frame.columns]
        if available:
            log_block(
                "{}: first {} rows of key columns".format(label, head),
                frame[available].head(head).to_string(index=False),
            )


def log_missing(frame, label, top=15):
    # Which columns hold NULLs and how many: the main data-quality signal.
    missing = frame.isna().sum()
    missing = missing[missing > 0].sort_values(ascending=False)
    if missing.empty:
        log("{}: no missing values".format(label))
        return
    report = pd.DataFrame({
        "missing": missing,
        "pct": (missing / len(frame) * 100).round(1),
    }).head(top)
    log_block(
        "{}: {} of {} columns have missing values (top {})".format(
            label, len(missing), len(frame.columns), min(top, len(missing))),
        report.to_string(),
    )


def log_change(title, before, after, columns, rows=5):
    """Show the same rows before and after a transformation, side by side.

    This is the evidence that a step did what it claims: same index, same
    columns, only the values differ.
    """
    left = [c for c in columns if c in before.columns]
    right = [c for c in columns if c in after.columns]
    if not left or not right:
        return
    index = before.index[:rows]
    log(title)
    log("    BEFORE")
    for line in before.loc[index, left].to_string().splitlines():
        log("      " + line)
    log("    AFTER")
    for line in after.loc[index, right].to_string().splitlines():
        log("      " + line)


def log_metrics(results, prefix, title):
    # One line per metric so the Airflow log stays greppable.
    keys = sorted(k for k in results if k.startswith(prefix + "_"))
    if not keys:
        return
    log(title)
    for key in keys:
        log("    {:<26} = {:.4f}".format(key, results[key]))


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
    log("delta version {} pinned".format(version))
    raw = spark.read.format("delta").option("versionAsOf", version).load(location)
    log("district_id/ward_id in Delta: {} / {}".format(
        dict(raw.dtypes)["district_id"], dict(raw.dtypes)["ward_id"]))
    cast = (
        raw
        .withColumn("district_id", F.col("district_id").cast("string"))
        .withColumn("ward_id", F.col("ward_id").cast("string"))
    )
    log("after cast to string: {} / {} (ids are categories, not quantities)".format(
        dict(cast.dtypes)["district_id"], dict(cast.dtypes)["ward_id"]))
    # Not toPandas(): Spark 3.3 still uses np.bool there, removed in numpy 1.24.
    rows = cast.collect()
    df = pd.DataFrame.from_records([r.asDict() for r in rows])
    log("loaded {:,} rows x {} columns into pandas".format(len(df), len(df.columns)))
    return df, int(version)


def _log_casts(label, rows, title, losses, note):
    """Report the values a cast turned into NaN (or into the missing token)."""
    if not losses:
        log("{}: {} - nothing to report".format(label, title))
        return
    frame = pd.DataFrame(
        [{"column": c, "values": n, "pct": round(n / rows * 100, 1)}
         for c, n in sorted(losses.items(), key=lambda kv: -kv[1])]
    )
    log_block("{}: {} ({})".format(label, title, note), frame.to_string(index=False))


def prepare(df, label=None):
    """Cast to the dtypes the pipeline expects. Shared contract with the API.

    Pass a label to log what each cast changed. Counting is cheap next to the
    copy this function already makes, and a silent cast is exactly how a column
    that arrives as text ends up as all-NaN without anyone noticing.
    """
    out = df.copy()
    coerced, unmapped, filled = {}, {}, {}
    for c in NUMERIC:
        before = int(out[c].notna().sum())
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
        lost = before - int(out[c].notna().sum())
        if lost:
            coerced[c] = lost
    for c in BOOLEAN:
        before = int(out[c].notna().sum())
        out[c] = out[c].map({True: 1.0, False: 0.0}).astype("float64")
        lost = before - int(out[c].notna().sum())
        if lost:
            unmapped[c] = lost
    for c in CATEGORICAL:
        nulls = int(out[c].isna().sum())
        out[c] = out[c].astype("object").where(out[c].notna(), MISSING).astype(str)
        if nulls:
            filled[c] = nulls
    if label:
        log("{}: {:,} rows, {} numeric + {} boolean + {} categorical -> {} columns".format(
            label, len(out), len(NUMERIC), len(BOOLEAN), len(CATEGORICAL),
            len(FEATURES)))
        log_change(
            "{}: booleans True/False -> 1.0/0.0, NULL -> NaN".format(label),
            df, out, ["is_in_project", "has_car_alley", "has_certificate"])
        log_change(
            "{}: categorical NULL -> '{}'".format(label, MISSING),
            df, out, ["house_direction", "furnishing", "project_name"])
        _log_casts(label, len(out), "numeric values that would not parse",
                   coerced, "pd.to_numeric(errors='coerce') turned these into NaN")
        _log_casts(label, len(out), "boolean values that were neither True nor False",
                   unmapped, "these became NaN")
        _log_casts(label, len(out), "categorical NULLs",
                   filled, "replaced by the '{}' token".format(MISSING))
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
    log("ColumnTransformer, 3 branches:")
    log("  'num'  passthrough           -> {} cols (HGB splits on NaN natively)".format(
        len(NUMERIC + BOOLEAN)))
    log("  'low'  OrdinalEncoder        -> {} cols (native categorical splits)".format(
        len(LOW_CARD)))
    log("  'high' Imputer+TargetEncoder -> {} cols (too many levels for native "
        "splits, cross-fitted so a row's own price never leaks)".format(len(HIGH_CARD)))
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


def log_fitted_preprocessor(pipeline, x_train):
    """Report what the encoders actually learned, once the pipeline is fitted.

    Everything above only states the intent; these are the real numbers, and
    they are where encoding bugs show up (a column with a single level, or a
    cardinality so high the target encoding is fitted on a handful of rows).
    """
    prep = pipeline.named_steps["prep"]
    fitted = dict(zip([name for name, _, _ in prep.transformers], prep.transformers_))

    ordinal = fitted["low"][1].named_steps["ordinalencoder"]
    rows = [
        {"column": column, "levels": len(categories)}
        for column, categories in zip(LOW_CARD, ordinal.categories_)
    ]
    log_block(
        "OrdinalEncoder learned these level counts (each becomes a native "
        "categorical split in the trees)",
        pd.DataFrame(rows).to_string(index=False))

    encoder = fitted["high"][1].named_steps["targetencoder"]
    rows = []
    for column, categories, encoding in zip(
            HIGH_CARD, encoder.categories_, encoder.encodings_):
        rows.append({
            "column": column,
            "levels": len(categories),
            "rows_per_level": round(len(x_train) / max(len(categories), 1), 1),
            "min_encoded": round(float(np.min(encoding)), 3),
            "max_encoded": round(float(np.max(encoding)), 3),
        })
    log_block(
        "TargetEncoder learned these mappings, in {} units (the global "
        "fallback for unseen levels is {:.3f})".format(
            TARGET, float(encoder.target_mean_)),
        pd.DataFrame(rows).to_string(index=False))
    log("a low rows_per_level means that encoding rests on few listings and "
        "is therefore noisy")

    # The encoders are the one place where the data stops being readable, so
    # show the same rows as text and as the numbers the trees actually get.
    sample = x_train.head(200)
    transformed = prep.transform(sample)
    log_change("encoded: category text -> number", sample, transformed,
               ["property_type", "house_direction", "ward_id", "street"])
    log("preprocessor output: {} columns fed to the trees".format(transformed.shape[1]))


def baseline_predict(train, test):
    """Median price per m2 of (district, property type) x area."""
    ppm2 = train["price_vnd"] / train["area_m2"]
    by_segment = ppm2.groupby([train["district_id"], train["property_type"]]).median()
    by_type = ppm2.groupby(train["property_type"]).median()
    keys = pd.MultiIndex.from_frame(test[["district_id", "property_type"]])
    rate = pd.Series(by_segment.reindex(keys).to_numpy(), index=test.index)
    # Three tiers, each catching what the previous one could not match.
    matched = int(rate.notna().sum())
    rate = rate.fillna(test["property_type"].map(by_type))
    by_type_used = int(rate.notna().sum()) - matched
    global_used = int(rate.isna().sum())
    rate = rate.fillna(ppm2.median())
    log("baseline reference prices built from {:,} training listings: "
        "{:,} district x property_type medians, {} property_type medians".format(
            len(train), len(by_segment), len(by_type)))
    log("baseline applied to the {:,} test rows: {:,} matched a "
        "district x property_type median, {:,} fell back to the property_type "
        "median, {:,} fell back to the global median {:,.0f} VND/m2".format(
            len(test), matched, by_type_used, global_used, ppm2.median()))
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
    log("downloading models:/{}@{} (v{}) from the registry".format(
        MODEL_NAME, CHAMPION_ALIAS, version.version))
    model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}")
    log("scoring the champion on the same {:,} holdout rows".format(len(x_test)))
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
    log("error on the written rows: mean APE {:.4f}, median APE {:.4f}".format(
        float(((predicted - price).abs() / price).mean()),
        float(((predicted - price).abs() / price).median())))
    # Plain Python rows: Spark 3.3's pandas conversion predates pandas 2.
    rows = list(zip(*(c.tolist() for c in columns)))
    log("writing {:,} rows to {} (mode=overwrite)".format(len(rows), PREDICTION_TABLE))
    (
        spark.createDataFrame(rows, PREDICTION_SCHEMA)
        .withColumn("mlflow_run_id", F.lit(run_id))
        .withColumn("model_version", F.lit(str(model_version)))
        .withColumn("scored_at", F.current_timestamp())
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(PREDICTION_TABLE)
    )
    log("{} written, tagged with mlflow_run_id={} and model_version={}".format(
        PREDICTION_TABLE, run_id, model_version))


def main():
    args = parse_args()
    log_step(1, "SETUP", "parse arguments and start the Spark session")
    log("run_id              = {}".format(args.run_id))
    log("test_days           = {} (size of the out-of-time holdout)".format(args.test_days))
    log("min_train_rows      = {:,}".format(args.min_train_rows))
    log("min_test_rows       = {:,}".format(args.min_test_rows))
    log("max_mape_regression = {}".format(args.max_mape_regression))
    log("target              = {} (predicts log price, exp() back to VND)".format(TARGET))
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    log("Spark {} started, app_id={}".format(spark.version, spark.sparkContext.applicationId))

    log_step(2, "LOAD", "read the feature table {}".format(FEATURE_TABLE))
    df, data_version = load_features(spark)
    log_frame(df, "feature table", [
        "listing_key", "posted_date", "district", "property_type",
        "area_m2", "price_vnd", TARGET,
    ])
    log_missing(df, "raw feature table")
    log_block("price_vnd summary (VND)", df["price_vnd"].describe().to_string())
    log_block("rows per property_type",
              df["property_type"].value_counts().head(10).to_string())

    log_step(3, "SPLIT", "hold out the newest {} days by posted_date".format(args.test_days))
    df["posted_date"] = pd.to_datetime(df["posted_date"])
    cutoff = df["posted_date"].max() - pd.Timedelta(days=args.test_days)
    is_test = df["posted_date"] > cutoff
    train, test = df[~is_test], df[is_test]
    log("cutoff posted_date = {}".format(cutoff.date()))
    log("train = {:,} rows ({} -> {})".format(
        len(train), train["posted_date"].min().date(), train["posted_date"].max().date()))
    log("test  = {:,} rows ({} -> {})".format(
        len(test), test["posted_date"].min().date(), test["posted_date"].max().date()))
    log("split by time, not at random: the model must predict listings posted "
        "after everything it was trained on")
    log("no overlap: max(train) {} < min(test) {}".format(
        train["posted_date"].max().date(), test["posted_date"].min().date()))
    if len(train) < args.min_train_rows or len(test) < args.min_test_rows:
        log("ABORT: need at least {:,} train and {:,} test rows".format(
            args.min_train_rows, args.min_test_rows))
        raise SystemExit("Not enough rows: train={}, test={}".format(len(train), len(test)))
    log("row-count check passed")

    log_step(4, "PREPROCESS", "cast features to the dtypes the pipeline expects")
    log("the API applies this very same function (logged as feature_spec.json), "
        "so training and serving cannot drift")
    x_train = prepare(train, label="train")
    x_test = prepare(test, label="test")
    y_train = train[TARGET].astype(float)
    log("x_train = {} rows x {} features".format(x_train.shape[0], x_train.shape[1]))
    log("x_test  = {} rows x {} features".format(x_test.shape[0], x_test.shape[1]))
    log_change(
        "target: price_vnd -> {} (log scale, so the trees optimise relative "
        "error; predictions are exp()'d back to VND)".format(TARGET),
        train, train, ["price_vnd", TARGET])
    log_missing(x_train, "x_train after preprocessing")

    mlflow.set_experiment(EXPERIMENT)
    client = MlflowClient()
    with mlflow.start_run(run_name=args.run_id) as run:
        log("MLflow experiment='{}' run_id={}".format(EXPERIMENT, run.info.run_id))
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

        log_step(5, "TRAIN", "build the pipeline and fit it on {:,} rows".format(len(x_train)))
        log("assembling the preprocessing + model pipeline")
        pipeline = build_pipeline()
        log("HistGradientBoostingRegressor hyperparameters:")
        params = pipeline.named_steps["model"].get_params()
        for key in ["loss", "learning_rate", "max_iter", "max_leaf_nodes",
                    "min_samples_leaf", "l2_regularization", "early_stopping",
                    "validation_fraction", "n_iter_no_change", "random_state"]:
            log("    {:<20} = {}".format(key, params[key]))
        log("fitting now: the TargetEncoder is cross-fitted first, then the "
            "trees are boosted; this is the slow part")
        fit_started = time.time()
        pipeline.fit(x_train, y_train)
        hgb = pipeline.named_steps["model"]
        log("fitted in {:.1f}s".format(time.time() - fit_started))
        log("built {} of max {} trees, then early stopping kicked in".format(
            hgb.n_iter_, hgb.max_iter))
        log_fitted_preprocessor(pipeline, x_train)
        mlflow.log_params({
            k: v for k, v in hgb.get_params().items()
            if k in {"learning_rate", "max_leaf_nodes", "min_samples_leaf",
                     "l2_regularization", "max_iter"}
        })
        mlflow.log_param("n_iter", hgb.n_iter_)

        log_step(6, "EVALUATE", "score the holdout, the baseline and the champion")
        raw_pred = pipeline.predict(x_test)
        test_pred = np.exp(raw_pred)
        log_change(
            "model output -> VND, next to the true price",
            pd.DataFrame({"log_pred": raw_pred}, index=test.index),
            pd.DataFrame({"predicted_vnd": test_pred,
                          "actual_vnd": test["price_vnd"].to_numpy()},
                         index=test.index),
            ["log_pred", "predicted_vnd", "actual_vnd"])
        log("predicted {:,} test rows: min={:,.0f} median={:,.0f} max={:,.0f} VND".format(
            len(test_pred), np.min(test_pred), np.median(test_pred), np.max(test_pred)))
        results = metrics(test["price_vnd"], test_pred, "test")
        results.update(metrics(test["price_vnd"], baseline_predict(train, test), "baseline"))
        results.update(metrics(
            train["price_vnd"], np.exp(pipeline.predict(x_train)), "train"))
        log("now scoring the current champion on this very same holdout")
        champ_mape, champ_version = champion_mape(client, x_test, test["price_vnd"])
        if champ_mape is None:
            log("no champion registered yet, so this run has nothing to beat")
        else:
            log("champion is v{} with MAPE {:.4f} on this holdout".format(
                champ_version, champ_mape))
            results["champion_mape"] = champ_mape
        mlflow.log_metrics(results)
        log_metrics(results, "test", "candidate on the holdout:")
        log_metrics(results, "baseline",
                    "baseline (median price/m2 of district x property_type):")
        log_metrics(results, "train", "in-sample, shown to gauge overfitting:")
        segments = segment_report(test, test_pred)
        log_block("holdout error per property_type", segments.to_string(index=False))

        log_step(7, "ARTIFACTS", "log the serving contract and reports to MLflow")
        with tempfile.TemporaryDirectory() as tmp:
            spec = {
                "numeric": NUMERIC, "boolean": BOOLEAN, "categorical": CATEGORICAL,
                "features": FEATURES, "missing_token": MISSING, "target": TARGET,
                "target_transform": "log",
            }
            with open(os.path.join(tmp, "feature_spec.json"), "w") as f:
                json.dump(spec, f, indent=2)
            log("feature_spec.json         - {} features; the API builds its "
                "input frame from this file, so the two cannot drift".format(len(FEATURES)))
            # Lets the API derive district-level features from district_id alone.
            districts = (
                df[["district_id", "district", "district_type", "area_zone"]]
                .drop_duplicates("district_id").sort_values("district_id")
            )
            districts.to_json(os.path.join(tmp, "districts.json"),
                              orient="records", force_ascii=False, indent=2)
            log("districts.json            - {} districts; lets the API fill "
                "district/area_zone from district_id alone".format(len(districts)))
            segments.to_csv(
                os.path.join(tmp, "test_by_property_type.csv"), index=False)
            log("test_by_property_type.csv - {} segments".format(len(segments)))
            mlflow.log_artifacts(tmp)
        log("artifacts uploaded to the MinIO bucket behind MLflow")

        log_step(8, "REGISTER", "save the model and run the quality gate")
        mlflow.sklearn.log_model(
            pipeline, artifact_path="model",
            registered_model_name=MODEL_NAME,
            input_example=x_test.head(3),
        )
        new_version = client.search_model_versions(
            "run_id='{}'".format(run.info.run_id))[0].version
        log("registered {} v{} as the candidate".format(MODEL_NAME, new_version))

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
            else "worse than champion v{}".format(champ_version)
        )
        log("rule 1, beat the baseline : {} (test {:.4f} < baseline {:.4f})".format(
            beats_baseline, results["test_mape"], results["baseline_mape"]))
        if champ_mape is None:
            log("rule 2, do not regress    : True (no champion to compare with)")
        else:
            log("rule 2, do not regress    : {} (test {:.4f} <= v{} {:.4f} x {:.2f})".format(
                beats_champion, results["test_mape"], champ_version, champ_mape,
                1 + args.max_mape_regression))
        if promoted:
            client.set_registered_model_alias(MODEL_NAME, CHAMPION_ALIAS, new_version)
            log("PROMOTED: alias '{}' now points to v{}; the API picks it up "
                "on the next reload_price_api".format(CHAMPION_ALIAS, new_version))
        else:
            log("REJECTED: {}. Alias '{}' still points to v{}, so the API "
                "keeps serving the old model".format(reason, CHAMPION_ALIAS, champ_version))
        client.set_model_version_tag(MODEL_NAME, new_version, "gate", reason)
        client.set_model_version_tag(MODEL_NAME, new_version, "test_mape",
                                     "{:.4f}".format(results["test_mape"]))
        mlflow.set_tags({"gate": reason, "model_version": new_version})

        log_step(9, "PREDICTIONS", "score every listing into {}".format(PREDICTION_TABLE))
        split = pd.Series(np.where(is_test, "test", "train"), index=df.index)
        log("re-running the same preprocessing over every row, train included, "
            "so the table shows how the model fits what it has already seen")
        all_pred = np.exp(pipeline.predict(prepare(df, label="all rows")))
        log("scored all {:,} listings ({:,} train, {:,} test); predictions are "
            "exp() of the model output, back in VND".format(
                len(all_pred), int((split == "train").sum()),
                int((split == "test").sum())))
        write_predictions(spark, df, all_pred, split, run.info.run_id, new_version)

    log_step(9, "DONE", "run finished")
    summary = {
        "model_version": new_version,
        "gate": reason,
        "champion_version_before": champ_version,
        "feature_table_delta_version": data_version,
    }
    summary.update({k: round(v, 4) for k, v in results.items()})
    log_block("run summary", json.dumps(summary, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()

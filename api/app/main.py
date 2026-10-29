"""Sale-price prediction API.

Serves the `champion` version of the MLflow registered model `price_model`
(trained by scripts/train_price_model.py). Input columns and dtypes follow the
run's feature_spec.json, so the API and the training script share one contract.
"""

import json
import os
from threading import Lock

import mlflow
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from mlflow.tracking import MlflowClient
from pydantic import BaseModel, Field

MODEL_NAME = os.getenv("MODEL_NAME", "price_model")
MODEL_ALIAS = os.getenv("MODEL_ALIAS", "champion")
WEB_PAGE = os.path.join(os.path.dirname(__file__), "static", "index.html")

app = FastAPI(title="Real estate price API")
_lock = Lock()
_state = {}


class PropertyInput(BaseModel):
    district_id: str = Field(..., examples=["13096"])
    property_type: str = Field(..., examples=["Nhà ở"])
    area_m2: float = Field(..., gt=0, le=20000)
    property_subtype: str | None = None
    ward_id: str | None = None
    street: str | None = None
    project_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    living_area_m2: float | None = None
    width_m: float | None = None
    length_m: float | None = None
    bedrooms: float | None = None
    bathrooms: float | None = None
    total_floors: float | None = None
    floor_number: float | None = None
    house_direction: str | None = None
    furnishing: str | None = None
    handover_status: str | None = None
    legal_category: str | None = None
    has_certificate: bool | None = None
    has_car_alley: bool | None = None
    is_frontage: bool | None = None
    is_wide_back: bool | None = None
    is_narrow_back: bool | None = None
    is_dilapidated: bool | None = None
    is_not_completed: bool | None = None
    is_planning_affected: bool | None = None


class Prediction(BaseModel):
    predicted_price_vnd: float
    predicted_price_per_m2_vnd: float
    model_version: str


def load_champion():
    client = MlflowClient()
    version = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
    model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@{MODEL_ALIAS}")
    artifacts = mlflow.artifacts.download_artifacts(run_id=version.run_id, artifact_path="")
    with open(os.path.join(artifacts, "feature_spec.json")) as f:
        spec = json.load(f)
    with open(os.path.join(artifacts, "districts.json"), encoding="utf-8") as f:
        districts = {str(d["district_id"]): d for d in json.load(f)}
    # Written by training runs since the model comparison; older versions lack it.
    form_path = os.path.join(artifacts, "form_options.json")
    form = {}
    if os.path.exists(form_path):
        with open(form_path, encoding="utf-8") as f:
            form = json.load(f)
    run = client.get_run(version.run_id)
    return {
        "model": model,
        "spec": spec,
        "districts": districts,
        "options": build_options(model, spec, districts, form),
        "version": version.version,
        "run_id": version.run_id,
        "metrics": {k: v for k, v in run.data.metrics.items() if k.startswith("test_")},
        "trained_on": run.data.tags.get("feature_table_delta_version"),
    }


def build_options(model, spec, districts, form):
    """Every value the web form may offer, read from what the model was fitted on."""
    # Logged by training since the boosting candidates; older runs only have
    # them in the fitted encoder (CatBoost pipelines have no "prep" step).
    levels = dict(form.get("levels") or {})
    if not levels and "prep" in model.named_steps:
        for name, transformer, columns in model.named_steps["prep"].transformers_:
            if name != "low":
                continue
            encoder = transformer[-1] if hasattr(transformer, "steps") else transformer
            for column, categories in zip(columns, encoder.categories_):
                levels[column] = [str(c) for c in categories if c != spec["missing_token"]]
    return {
        "districts": [
            {"district_id": d["district_id"], "district": d["district"]}
            for d in districts.values()
        ],
        "property_types": levels.get("property_type", []),
        # Grouped by property_type when the run logged form_options.json.
        "subtypes": form.get("subtypes") or {"*": levels.get("property_subtype", [])},
        "wards": form.get("wards", {}),
        "house_directions": levels.get("house_direction", []),
        "furnishings": levels.get("furnishing", []),
        "handover_statuses": levels.get("handover_status", []),
        "legal_categories": levels.get("legal_category", []),
    }


def to_frame(item: PropertyInput, spec, districts):
    district = districts.get(item.district_id)
    if district is None:
        raise HTTPException(422, f"Unknown district_id {item.district_id}")
    row = item.model_dump()
    row["district_type"] = district["district_type"]
    row["area_zone"] = district["area_zone"]
    row["is_in_project"] = item.project_name is not None
    # Training derives it from the legal status (dim_legal_status).
    if item.has_certificate is None and item.legal_category is not None:
        row["has_certificate"] = item.legal_category == "Có sổ"

    frame = pd.DataFrame([row])
    for c in spec["numeric"]:
        frame[c] = pd.to_numeric(frame[c], errors="coerce").astype("float64")
    for c in spec["boolean"]:
        frame[c] = frame[c].map({True: 1.0, False: 0.0}).astype("float64")
    for c in spec["categorical"]:
        frame[c] = frame[c].where(frame[c].notna(), spec["missing_token"]).astype(str)
    return frame[spec["features"]]


@app.on_event("startup")
def startup():
    try:
        _state.update(load_champion())
    except Exception as exc:  # API still starts; /predict reports 503 until /model/reload.
        _state["error"] = str(exc)


@app.get("/")
def root():
    return {"status": "ok", "model_loaded": "model" in _state}


@app.get("/model")
def model_info():
    if "model" not in _state:
        raise HTTPException(503, _state.get("error", "No model loaded"))
    return {
        "name": MODEL_NAME,
        "alias": MODEL_ALIAS,
        "version": _state["version"],
        "run_id": _state["run_id"],
        "feature_table_delta_version": _state["trained_on"],
        "test_metrics": _state["metrics"],
    }


@app.get("/options")
def options():
    if "model" not in _state:
        raise HTTPException(503, _state.get("error", "No model loaded"))
    return _state["options"]


@app.get("/web", include_in_schema=False)
def web():
    return FileResponse(WEB_PAGE)


@app.post("/model/reload")
def reload_model():
    with _lock:
        try:
            loaded = load_champion()
        except Exception as exc:
            raise HTTPException(503, f"Could not load {MODEL_NAME}@{MODEL_ALIAS}: {exc}")
        _state.clear()
        _state.update(loaded)
    return model_info()


@app.post("/predict", response_model=Prediction)
def predict(item: PropertyInput):
    if "model" not in _state:
        raise HTTPException(503, _state.get("error", "No model loaded"))
    frame = to_frame(item, _state["spec"], _state["districts"])
    price = float(np.exp(_state["model"].predict(frame)[0]))
    return Prediction(
        predicted_price_vnd=round(price, -6),
        predicted_price_per_m2_vnd=round(price / item.area_m2, -3),
        model_version=_state["version"],
    )

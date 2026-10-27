"""Registry-backed FastAPI serving and a small same-origin demo."""
import csv
import io
import json
import logging
import os
import secrets
import sqlite3
import threading
import uuid
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Literal

import mlflow
import pandas as pd
from fastapi import FastAPI, Depends, HTTPException, Header, Query
from fastapi.responses import FileResponse, Response, JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StrictInt
from mlflow.tracking import MlflowClient

from recommendation.contract import file_hash, normalize_text, utc
from recommendation.registry import MODEL_NAME
from recommendation.evaluate import evaluate_engine

ROOT = Path(os.getenv('REIP_STATE', '/state'))
WEB_ROOT = Path(__file__).resolve().parents[3] / 'web' / 'recommendation'


class Manager:
    def __init__(self):
        self.state = None
        self.lock = threading.Lock()
        self.error = None

    def load(self, version=None):
        with self.lock:
            client = MlflowClient()
            version = str(version or client.get_model_version_by_alias(MODEL_NAME, 'champion').version)
            if self.state and self.state['version'] == version:
                return self.info()
            directory = ROOT/'cache'/version
            directory.mkdir(parents=True, exist_ok=True)
            model = mlflow.pyfunc.load_model(f'models:/{MODEL_NAME}/{version}', dst_path=str(directory))
            engine = model.unwrap_python_model().engine
            for market, (df, _, _) in engine.markets.items():
                seeds = df.loc[df.province_id.ne('')]
                if seeds.empty:
                    raise ValueError(f'No valid seeds for {market}')
                engine.similar(seeds.iloc[0].listing_key, limit=1, as_of=engine.manifest['as_of'])
            if str(client.get_registered_model(MODEL_NAME).aliases.get('champion')) != version:
                raise ValueError('Champion changed while loading; keep the previous model')
            # Atomic reference swap: in-flight requests retain their original engine.
            self.state = {'model': model, 'engine': engine, 'version': version,
                          'fingerprint': file_hash(engine.bundle/'manifest.json')}
            self.error = None
            return self.info()

    def get(self):
        if self.state is None:
            raise HTTPException(503, 'Recommendation model not loaded')
        return self.state

    def info(self):
        state = self.get()
        manifest = state['engine'].manifest
        return dict(name=MODEL_NAME, version=state['version'], fingerprint=state['fingerprint'],
                    dataset_version=manifest['dataset_version'], as_of=manifest['as_of'],
                    markets={m: {'rows': len(v[0])} for m, v in state['engine'].markets.items()},
                    quality_status=manifest.get('quality_status'), weights=manifest['weights'])


class Base(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)


class Common(Base):
    limit: int = Field(default=10, strict=True, ge=1, le=50)
    max_age_days: int = Field(default=90, strict=True, ge=0, le=3650)
    as_of: str | None = None


class Similar(Common):
    listing_key: str = Field(min_length=1, max_length=200)


class Preferences(Common):
    transaction_type: Literal['sale', 'rent']
    property_category: Literal['HOUSE', 'APARTMENT', 'LAND', 'ROOM', 'COMMERCIAL', 'OTHER']
    price_period: Literal['total', 'month', 'day', 'year', 'unknown'] | None = None
    province_id: str | None = None
    district_id: str | None = None
    max_price: float | None = Field(default=None, gt=0)
    min_area: float | None = Field(default=None, gt=0)
    min_bedrooms: int | None = Field(default=None, ge=0, le=100)
    query: str | None = Field(default=None, max_length=1000)
    target_price: float | None = Field(default=None, gt=0)
    target_area: float | None = Field(default=None, gt=0)


class Search(Base):
    method: Literal['similar', 'preferences']
    request: dict


class CreateTask(Search):
    model_version: str
    annotator: str = Field(min_length=1, max_length=100)
    split: Literal['tune', 'test'] = 'tune'
    k: int = Field(default=10, strict=True, ge=1, le=50)


class Grade(Base):
    listing_key: str
    grade: StrictInt | None = Field(ge=0, le=3)
    note: str = Field(default='', max_length=2000)
    version: int = Field(strict=True, ge=0)


class Reload(Base):
    version: str = Field(pattern=r'^[1-9][0-9]*$')


def create_app(manager=None, database=None):
    manager = manager or Manager()
    database = Path(database or ROOT/'labels.sqlite')

    @contextmanager
    def db():
        con = sqlite3.connect(database, timeout=10)
        try:
            with con:
                yield con
        finally:
            con.close()

    @asynccontextmanager
    async def lifespan(app):
        database.parent.mkdir(parents=True, exist_ok=True)
        with db() as con:
            con.execute('CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        stop = threading.Event()
        def initial_load():
            while not stop.is_set():
                try:
                    manager.load()
                    return
                except Exception as exc:
                    manager.error = str(exc)
                    logging.getLogger(__name__).warning('Initial model load failed: %s; retry in 30s', exc)
                    stop.wait(30)
        loader = threading.Thread(target=initial_load, daemon=True)
        loader.start()
        yield
        stop.set()
        loader.join(timeout=1)

    app = FastAPI(title='REIP Recommendation API', lifespan=lifespan)

    @app.exception_handler(ValueError)
    async def invalid(request, exc):
        return JSONResponse(status_code=422, content={'detail': str(exc)})

    @app.exception_handler(KeyError)
    async def missing(request, exc):
        return JSONResponse(status_code=404, content={'detail': 'Không tìm thấy tin/phiếu.'})

    def admin(x_admin_token: str = Header(default='')):
        expected = Path(os.environ['REIP_ADMIN_TOKEN_FILE']).read_text().strip()
        if not expected or not secrets.compare_digest(expected, x_admin_token):
            raise HTTPException(401, 'Invalid admin token')

    def details(engine, key):
        market, index = engine.lookup[key]
        row = engine.markets[market][0].iloc[index]
        out = {}
        for field in ['listing_key', 'title', 'description', 'price_vnd', 'price_period', 'area_m2',
                      'bedrooms', 'province', 'district', 'province_id', 'district_id',
                      'transaction_type', 'property_category', 'url']:
            value = row.get(field)
            if field == 'description' and (value is None or pd.isna(value)):
                value = row.get('content_text')
            if value is None or pd.isna(value):
                value = None
            elif hasattr(value, 'item'):
                value = value.item()
            out[field] = value
        return out

    def search(state, body):
        args = (Similar if body.method == 'similar' else Preferences).model_validate(body.request).model_dump(exclude_none=True)
        args['as_of'] = utc(args.get('as_of')).isoformat()
        if 'query' in args and not args['query'].strip():
            args.pop('query')
        engine = state['engine']
        results = getattr(engine, body.method)(**args)
        return args, results

    def read_task(con, task_id):
        row = con.execute('SELECT payload FROM tasks WHERE id=?', (task_id,)).fetchone()
        if row is None:
            raise HTTPException(404, 'Unknown task')
        return json.loads(row[0])

    def complete(task):
        return bool(task['relevance']) and all(type(v) is int for v in task['relevance'].values())

    @app.get('/', include_in_schema=False)
    def home():
        return FileResponse(WEB_ROOT / 'index.html')

    @app.get('/health')
    def health():
        return {'status': 'ok', **manager.info()}

    @app.get('/model')
    def info():
        return manager.info()

    @app.get('/catalog')
    def catalog(market: Literal['sale', 'rent']='rent', q: str=Query('', max_length=200), limit: int=Query(20, ge=1, le=50)):
        state = manager.get()
        df = state['engine'].markets[market][0]
        if q:
            df = df.loc[df.title.map(normalize_text).str.contains(normalize_text(q), regex=False) | df.listing_key.eq(q)]
        return {'items': [details(state['engine'], k) for k in df.listing_key.head(limit)]}

    @app.get('/filters')
    def filters(market: Literal['sale', 'rent']='rent'):
        df = manager.get()['engine'].markets[market][0]
        provinces = df.loc[df.province_id.ne(''), ['province_id', 'province']].drop_duplicates('province_id')
        districts = df.loc[df.province_id.ne('') & df.district_id.ne('') & df.location_trusted,
                           ['province_id', 'district_id', 'district']].drop_duplicates(['province_id', 'district_id'])
        return {'provinces': provinces.sort_values('province').to_dict('records'),
                'districts': districts.sort_values(['province_id', 'district']).to_dict('records')}

    @app.post('/recommend')
    def recommend(body: Search):
        state = manager.get()
        args, results = search(state, body)
        return dict(version=state['version'], dataset_version=state['engine'].manifest['dataset_version'],
                    method=body.method, request=args,
                    items=[{**details(state['engine'], r['listing_key']), **r} for r in results])

    @app.post('/admin/reload', dependencies=[Depends(admin)])
    def reload(body: Reload):
        return manager.load(body.version)

    @app.post('/tasks', status_code=201)
    def create(body: CreateTask):
        state = manager.get()
        if state['version'] != body.model_version:
            raise HTTPException(409, 'Model vừa cập nhật. Hãy tìm lại trước khi tạo phiếu.')
        if not body.annotator.strip():
            raise ValueError('Cần nhập người chấm.')
        args, results = search(state, body)
        if not results or body.k > args['limit']:
            raise ValueError('Cần có ứng viên và K ≤ số ứng viên yêu cầu.')
        keys = [r['listing_key'] for r in results]
        secrets.SystemRandom().shuffle(keys)
        task = dict(query_id=uuid.uuid4().hex, method=body.method, as_of=args.pop('as_of'),
            request={k: v for k, v in args.items() if k != 'limit'}, k=body.k,
            dataset_version=state['engine'].manifest['dataset_version'], model_version=state['version'],
            model_fingerprint=state['fingerprint'], annotator=body.annotator.strip(), split=body.split,
            version=0, relevance=dict.fromkeys(keys), notes={},
            items=[details(state['engine'], k) for k in keys],
            seed=details(state['engine'], args['listing_key']) if body.method == 'similar' else None)
        with db() as con:
            con.execute('INSERT INTO tasks VALUES (?,?)', (task['query_id'], json.dumps(task, ensure_ascii=False)))
        return task

    @app.get('/tasks')
    def tasks():
        with db() as con:
            rows = [json.loads(r[0]) for r in con.execute('SELECT payload FROM tasks ORDER BY rowid DESC')]
        return [{k: v for k, v in t.items() if k not in ('items', 'seed', 'notes')} for t in rows]

    @app.get('/tasks/{task_id}')
    def task(task_id: str):
        with db() as con:
            return read_task(con, task_id)

    @app.patch('/tasks/{task_id}')
    def grade(task_id: str, body: Grade):
        with db() as con:
            con.execute('BEGIN IMMEDIATE')
            task = read_task(con, task_id)
            if task['version'] != body.version:
                raise HTTPException(409, 'Phiếu đã thay đổi. Mở lại phiếu để tiếp tục.')
            if body.listing_key not in task['relevance']:
                raise ValueError('Tin không thuộc phiếu.')
            task['relevance'][body.listing_key] = body.grade
            task['notes'][body.listing_key] = body.note
            task['version'] += 1
            con.execute('UPDATE tasks SET payload=? WHERE id=?', (json.dumps(task, ensure_ascii=False), task_id))
        return task

    @app.get('/tasks/{task_id}/metrics')
    def metrics(task_id: str):
        with db() as con:
            task = read_task(con, task_id)
        state = manager.get()
        if task['model_fingerprint'] != state['fingerprint']:
            raise HTTPException(409, 'Phiếu thuộc model cũ. Xuất JSONL và đánh giá với đúng bundle đã lưu.')
        if not complete(task):
            raise HTTPException(409, 'Cần chấm hết phiếu trước khi tính chỉ số.')
        return {'metrics': evaluate_engine(state['engine'], [task], task['k']),
                'scope': 'Một phiếu; Recall chỉ tính trong pool đã chấm, không phải toàn bộ kho tin.'}

    @app.get('/export')
    def export(task_id: str, format: Literal['jsonl', 'csv']='jsonl'):
        with db() as con:
            task = read_task(con, task_id)
        if not complete(task):
            raise HTTPException(409, 'Chấm hết phiếu trước khi xuất; nhãn trống không phải 0.')
        label = {k: v for k, v in task.items() if k not in ('items', 'seed')}
        if format == 'jsonl':
            content, media = json.dumps(label, ensure_ascii=False)+'\n', 'application/x-ndjson'
        else:
            out = io.StringIO()
            writer = csv.writer(out)
            writer.writerow(['query_id', 'dataset_version', 'split', 'listing_key', 'grade', 'note'])
            for key, value in task['relevance'].items():
                note = task['notes'].get(key, '')
                if note.lstrip().startswith(('=', '+', '-', '@')):
                    note = "'" + note
                writer.writerow([task_id, task['dataset_version'], task['split'], key, value, note])
            content, media = '\ufeff'+out.getvalue(), 'text/csv'
        return Response(content, media_type=media, headers={'Content-Disposition': f'attachment; filename="labels_{task_id[:8]}.{format}"'})

    return app


app = create_app()

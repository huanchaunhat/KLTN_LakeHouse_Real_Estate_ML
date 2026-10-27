"""Internal, authenticated job endpoint used by Airflow; no Docker socket needed."""
import hashlib
import json
import os
import secrets
import signal
import subprocess
import sys
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Depends
from pydantic import BaseModel, Field
from recommendation.pipeline import write_json

ROOT = Path(os.getenv('REIP_STATE', '/state'))
processes = {}
lock = threading.Lock()


def auth(x_admin_token: str = Header(default='')):
    expected = Path(os.environ['REIP_ADMIN_TOKEN_FILE']).read_text().strip()
    if not expected or not secrets.compare_digest(expected, x_admin_token):
        raise HTTPException(401, 'Invalid admin token')


@asynccontextmanager
async def lifespan(app):
    yield
    for process in processes.values():
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)


app = FastAPI(title='Recommendation retrain worker', lifespan=lifespan, dependencies=[Depends(auth)])


class Job(BaseModel):
    job_id: str = Field(min_length=1, max_length=250)


def location(job_id):
    return ROOT/'jobs'/hashlib.sha256(job_id.encode()).hexdigest()[:24]


def status(job_id):
    directory = location(job_id)
    file = directory/'worker.json'
    if not file.exists():
        raise HTTPException(404, 'Unknown job')
    data = json.loads(file.read_text())
    process = processes.get(job_id)
    if data['status'] == 'running':
        code = process.poll() if process else -1
        if code is not None:
            data.update(status='succeeded' if code == 0 else 'failed', exit_code=code)
            write_json(file, data)
    if (directory/'plan.json').exists():
        plan = json.loads((directory/'plan.json').read_text())
        data['version'] = plan.get('deployed_version')
    return data


@app.post('/jobs', status_code=202)
def start(body: Job):
    with lock:
        directory = location(body.job_id)
        if (directory/'worker.json').exists():
            current = status(body.job_id)
            if current['status'] in ('running', 'succeeded'):
                return current
        if any(p.poll() is None for p in processes.values()):
            raise HTTPException(409, 'Another training job is running')
        directory.mkdir(parents=True, exist_ok=True)
        with (directory/'worker.log').open('a') as log:
            process = subprocess.Popen([sys.executable, '-m', 'recommendation.pipeline', '--job-id', body.job_id],
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        processes[body.job_id] = process
        write_json(directory/'worker.json', dict(job_id=body.job_id, status='running'))
        return status(body.job_id)


@app.get('/jobs')
def get(job_id: str):
    with lock:
        return status(job_id)

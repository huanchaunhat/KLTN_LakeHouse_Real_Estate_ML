"""Read-only MinIO Delta snapshot export using existing PyArrow dependencies.

Supports Delta reader protocol 1 and classic single-file checkpoints. Fail closed
for column mapping, deletion vectors, multipart/V2-only checkpoints or missing logs.
Do not scan every Parquet file: superseded Delta files are NOT current records.
"""
import argparse
import json
import os
import re
import shutil
import tempfile
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

from .contract import file_hash, utc


def filesystem(endpoint, env_file=None):
    config = {}
    if env_file:
        for line in Path(env_file).read_text().splitlines():
            if '=' in line and not line.lstrip().startswith('#'):
                k, v = line.split('=', 1)
                config[k.strip()] = v.strip().strip('\"\'')
    config.update(os.environ)
    access = config.get('AWS_ACCESS_KEY_ID') or config.get('MINIO_ACCESS_KEY') or config.get('MINIO_ROOT_USER')
    secret = config.get('AWS_SECRET_ACCESS_KEY') or config.get('MINIO_SECRET_KEY') or config.get('MINIO_ROOT_PASSWORD')
    if not access or not secret:
        raise ValueError('Configure MinIO credentials through environment or --env-file')
    url = urlparse(endpoint)
    if url.scheme not in ('http', 'https') or not url.netloc:
        raise ValueError('Endpoint must be an http(s) URL')
    return fs.S3FileSystem(access_key=access, secret_key=secret,
                          endpoint_override=url.netloc, scheme=url.scheme,
                          region=config.get('AWS_DEFAULT_REGION', 'us-east-1'),
                          connect_timeout=5, request_timeout=60)


def read_snapshot(storage, table, version=None):
    entries = storage.get_file_info(fs.FileSelector(table + '/_delta_log', recursive=False))
    logs = {int(e.base_name[:20]): e.path for e in entries if re.fullmatch(r'\d{20}\.json', e.base_name)}
    checkpoints = {int(e.base_name[:20]): e.path for e in entries
                   if re.fullmatch(r'\d{20}\.checkpoint\.parquet', e.base_name)}
    known = set(logs) | set(checkpoints)
    if not known:
        raise ValueError('No supported Delta transaction history')
    version = max(known) if version is None else version
    if version not in known:
        raise ValueError('Requested Delta version is unavailable')
    base = max([v for v in checkpoints if v <= version], default=-1)
    active, metadata, protocol = {}, {}, {}

    def apply(action):
        nonlocal metadata, protocol
        if action.get('metaData'):
            metadata = action['metaData']
        if action.get('protocol'):
            protocol = action['protocol']
        if action.get('remove'):
            active.pop(action['remove']['path'], None)
        if action.get('add'):
            active[action['add']['path']] = action['add']

    if base >= 0:
        with storage.open_input_file(checkpoints[base]) as f:
            for action in pq.ParquetFile(f).read().to_pylist():
                apply(action)
    for v in range(base + 1, version + 1):
        if v not in logs:
            raise ValueError(f'Missing Delta log {v}; use a Delta-native exporter')
        with storage.open_input_file(logs[v]) as f:
            for line in f.read().decode().splitlines():
                apply(json.loads(line))
    if protocol.get('minReaderVersion') != 1 or protocol.get('readerFeatures'):
        raise ValueError('Unsupported Delta reader protocol; use Spark Delta export')
    configuration = metadata.get('configuration') or {}
    if isinstance(configuration, list):
        configuration = dict(configuration)
    if configuration.get('delta.columnMapping.mode', 'none') != 'none':
        raise ValueError('Column mapping needs a Delta-native reader')
    if not active:
        raise ValueError('Empty Delta snapshot')
    tables = []
    for name, entry in sorted(active.items()):
        if entry.get('deletionVector'):
            raise ValueError('Deletion vectors need a Delta-native reader')
        relative = unquote(name)
        if '..' in PurePosixPath(relative).parts or relative.startswith('/') or '://' in relative:
            raise ValueError('Non-relative Delta add path')
        with storage.open_input_file(table + '/' + relative) as f:
            chunk = pq.ParquetFile(f).read(use_threads=False)
        partitions = entry.get('partitionValues') or {}
        if isinstance(partitions, list):
            partitions = dict(partitions)
        for col, value in partitions.items():
            if col != 'transaction_type':
                raise ValueError('Only transaction_type partitioning is supported')
            if col not in chunk.column_names:
                chunk = chunk.append_column(col, pa.array([value] * len(chunk), type=pa.string()))
        tables.append(chunk)
    result = pa.concat_tables(tables, promote_options='default')
    return result, dict(table='s3://' + table, delta_version=version,
                        active_files=len(active), rows=len(result),
                        exported_at=utc().isoformat(), delta_table_id=metadata.get('id'))


def export(endpoint, table, output, *, env_file=None, version=None):
    output = Path(output)
    if output.exists():
        raise FileExistsError('Use a new immutable export directory')
    storage = filesystem(endpoint, env_file)
    data, meta = read_snapshot(storage, table, version)
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='.export-', dir=output.parent))
    try:
        pq.write_table(data, stage / 'catalog.parquet', compression='snappy')
        meta['sha256'] = file_hash(stage / 'catalog.parquet')
        (stage / 'snapshot.json').write_text(json.dumps(meta, indent=2))
        os.rename(stage, output)
    except BaseException:
        shutil.rmtree(stage)
        raise
    return meta


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--endpoint', default='http://localhost:9000')
    p.add_argument('--env-file', type=Path)
    p.add_argument('--table', default='lakehouse/warehouse/gold.db/ml_recommendation_features')
    p.add_argument('--version', type=int)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    print(json.dumps(export(args.endpoint, args.table, args.output, env_file=args.env_file,
                            version=args.version), indent=2))


if __name__ == '__main__':
    main()

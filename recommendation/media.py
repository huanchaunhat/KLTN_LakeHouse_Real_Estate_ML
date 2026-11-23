"""Display-only listing photos, including a Silver sidecar for older bundles.

The sidecar never changes an immutable recommendation bundle or its ranking.
New bundles carry image_urls directly from Gold and do not need the sidecar.
"""
import argparse
import json
import os
import tempfile
from pathlib import Path
from urllib.parse import urlsplit


def image_urls(value):
    """Return JSON-safe, unique HTTP(S) URLs from a Parquet/Silver image array."""
    if hasattr(value, 'tolist'):
        value = value.tolist()
    if not isinstance(value, (list, tuple)):
        return []
    result = []
    for item in value:
        if not isinstance(item, str):
            continue
        item = item.strip()
        try:
            parsed = urlsplit(item)
            valid = parsed.scheme in ('http', 'https') and parsed.hostname and not parsed.username and not parsed.password
        except ValueError:
            valid = False
        if valid and item not in result:
            result.append(item)
    return result


def load_images(path):
    import pyarrow.parquet as pq

    path = Path(path)
    if not path.exists():
        return {}
    rows = pq.read_table(path, columns=['listing_key', 'image_urls']).to_pylist()
    return {row['listing_key']: image_urls(row['image_urls']) for row in rows}


def export_images(endpoint, bundle, output, *, env_file=None,
                  table='lakehouse/warehouse/silver.db/listing_current'):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from .export import filesystem, read_snapshot

    keys = set()
    for market in ('sale', 'rent'):
        keys.update(pq.read_table(Path(bundle)/market/'catalog.parquet',
                                  columns=['listing_key'])['listing_key'].to_pylist())
    data, snapshot = read_snapshot(filesystem(endpoint, env_file), table,
                                   columns=['listing_key', 'image_urls'], listing_keys=keys)
    rows = [{'listing_key': row['listing_key'], 'image_urls': image_urls(row['image_urls'])}
            for row in data.to_pylist()]
    if len({row['listing_key'] for row in rows}) != len(rows):
        raise ValueError('Duplicate listing keys in the Silver image snapshot')
    schema = pa.schema([('listing_key', pa.string()), ('image_urls', pa.list_(pa.string()))])
    snapshot.update(model_keys=len(keys), listings_with_images=sum(bool(row['image_urls']) for row in rows))
    result = pa.Table.from_pylist(rows, schema=schema).replace_schema_metadata(
        {'snapshot': json.dumps(snapshot)})
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.listing-images-', suffix='.parquet', dir=output.parent)
    os.close(fd)
    try:
        pq.write_table(result, name, compression='snappy')
        os.replace(name, output)
    finally:
        Path(name).unlink(missing_ok=True)
    return snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', default=os.getenv('MLFLOW_S3_ENDPOINT_URL', 'http://localhost:9000'))
    parser.add_argument('--env-file', type=Path)
    parser.add_argument('--bundle', required=True, type=Path)
    parser.add_argument('--output', type=Path,
                        default=Path(os.getenv('REIP_STATE', '/state'))/'listing_images.parquet')
    args = parser.parse_args()
    print(json.dumps(export_images(args.endpoint, args.bundle, args.output, env_file=args.env_file), indent=2))


if __name__ == '__main__':
    main()

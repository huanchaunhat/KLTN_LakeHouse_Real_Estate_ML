"""Photo metadata handles real Parquet arrays, missing values and Delta updates."""
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

from recommendation.export import read_snapshot
from recommendation.media import image_urls, load_images


class MediaTests(unittest.TestCase):
    def test_parquet_arrays_and_invalid_urls(self):
        url = 'https://cdn.example.com/house.jpg?a=1&b=2'
        self.assertEqual(image_urls(np.array([url, url, 'javascript:alert(1)',
            'data:image/png;base64,xx', 'file:///tmp/a', '//example.com/a',
            'https://user:password@example.com/a', 'http://[broken', None])), [url])
        for missing in (None, float('nan'), '', url):
            self.assertEqual(image_urls(missing), [])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'images.parquet'
            self.assertEqual(load_images(path), {})
            pq.write_table(pa.table({'listing_key': ['a', 'b'], 'image_urls': [[url], None]}), path)
            self.assertEqual(load_images(path), {'a': [url], 'b': []})

    def test_projection_and_keys_respect_current_delta_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'_delta_log').mkdir()
            pq.write_table(pa.table({'listing_key': ['a'], 'image_urls': [['https://cdn.example.com/old.jpg']],
                                     'unused': ['large body']}), root/'old.parquet')
            pq.write_table(pa.table({'listing_key': ['a', 'b'], 'image_urls': [
                ['https://cdn.example.com/new.jpg'], []], 'unused': ['body', 'body']}), root/'new.parquet')
            actions = [dict(protocol=dict(minReaderVersion=1)),
                       dict(metaData=dict(id='images-test', configuration={})),
                       dict(add=dict(path='old.parquet', partitionValues={}))]
            (root/'_delta_log/00000000000000000000.json').write_text('\n'.join(map(json.dumps, actions)))
            actions = [dict(remove=dict(path='old.parquet')),
                       dict(add=dict(path='new.parquet', partitionValues={}))]
            (root/'_delta_log/00000000000000000001.json').write_text('\n'.join(map(json.dumps, actions)))
            data, meta = read_snapshot(fs.LocalFileSystem(), str(root),
                columns=['listing_key', 'image_urls'], listing_keys={'a'})
            self.assertEqual(data.to_pylist(), [{'listing_key': 'a',
                'image_urls': ['https://cdn.example.com/new.jpg']}])
            self.assertEqual(meta['delta_version'], 1)
            self.assertEqual(data.column_names, ['listing_key', 'image_urls'])


if __name__ == '__main__':
    unittest.main()

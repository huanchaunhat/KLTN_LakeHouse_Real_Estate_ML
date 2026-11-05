"""Import fully completed annotation CSV into the existing evaluator format."""
import argparse
import csv
import json
from pathlib import Path


def convert(queries, annotations, output):
    tasks = [json.loads(line) for line in queries.read_text(encoding='utf-8').splitlines() if line.strip()]
    expected = {(t['query_id'], key) for t in tasks for key in t['relevance']}
    grades = {}
    with annotations.open(encoding='utf-8-sig', newline='') as f:
        for row in csv.DictReader(f):
            pair = (row['query_id'], row['listing_key'])
            if pair not in expected or pair in grades:
                raise ValueError(f'Unknown or duplicated annotation: {pair}')
            raw = row['grade'].strip()
            if raw not in {'0', '1', '2', '3'}:
                raise ValueError(f'Complete grade 0..3 for {pair}; received {raw!r}')
            if not row['annotator_id'].strip():
                raise ValueError(f'Missing annotator_id for {pair}')
            grades[pair] = int(raw)
    if set(grades) != expected:
        raise ValueError(f'Missing {len(expected - set(grades))} annotations')
    if output.exists():
        raise ValueError('Output exists; choose a new file')
    for task in tasks:
        task['relevance'] = {key: grades[(task['query_id'], key)] for key in task['relevance']}
    output.write_text(''.join(json.dumps(t, ensure_ascii=False) + '\n' for t in tasks), encoding='utf-8')
    print(f'Imported {len(grades)} grades for {len(tasks)} queries into {output}')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--queries', type=Path, required=True)
    p.add_argument('--annotations', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    convert(a.queries, a.annotations, a.output)

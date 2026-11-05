"""Complete only blank annotations using the user's 2026-11-05 column policy."""
import argparse
import collections
import copy
from datetime import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shutil

ROOT = Path('/Users/macbookpro/2025-2026/KhoaLuanTotNghiep/LakeHouse-Architecture-For-RealEstateData')
DATA = ROOT / 'docs/evaluation/query_set_20261103'
PREVIOUS = DATA / 'ai_review_20261103'
OUT = DATA / 'ai_review_20261105'
WORK = Path('/private/tmp/reip_annotation_complete_20261105')
POLICY_ID = 'user_column_authority_20261105'
POLICY = {
    'confirmed_by': 'user',
    'confirmed_on': '2026-11-05',
    'price': 'price_vnd and its price_period column govern assessment when prose differs',
    'geography': 'province, district, ward and corresponding snapshot IDs govern assessment',
    'bedrooms': 'bedrooms column governs hard filters and numeric comparison',
    'area': 'area_m2 column is the comparison basis; house/land area is land area, while prose may describe total floor area',
    'area_example': '38 m2 land with ground floor and one upper floor can be described as 76 m2 total floor area; compare 38 to land-area targets',
    'scope': 'Only rows whose grade is blank on this run; preserve every existing grade, annotator_id and reason',
}

spec = importlib.util.spec_from_file_location('prior_annotation', PREVIOUS/'annotation_procedure.py')
grader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(grader)

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def policy_hard_issues(q, row, item, task):
    errors, unknown = grader.hard_issues(q, row, item, task)
    # A prose count below the minimum no longer overrides the supplied count.
    ignored = [message for message in unknown if message.startswith('phòng ngủ cột ')]
    unknown = [message for message in unknown if message not in ignored]
    return errors, unknown, ignored

def features_from_offer(row):
    features = grader.extract(row)
    # Preserve the distinction between "chợ" and the unrelated word "cho".
    # The previous accent-folded detector could quote "cho người lớn tuổi".
    text = features['text']
    match = re.search(r'\b(?:gần|sát|cách|ngay|kề|đối diện|bên|ra|cạnh)[^\n.;]{0,65}\bchợ\b|\bchợ\b[^\n.;]{0,30}\b(?:gần|kề bên)\b',text,re.IGNORECASE)
    if match:
        features['features'].add('market')
        features['evidence']['market'] = text[max(0,match.start()-15):match.end()+55].replace('\n',' ')
    else:
        features['features'].discard('market')
        features['evidence'].pop('market',None)
    return features

def similar_from_columns(q, row, item, features):
    grade, reason, checks = grader.grade_similar(q,row,item,features)
    source_bedrooms = grader.number(q['source'].get('bedrooms'))
    bedrooms = grader.number(row.get('bedrooms'))
    mismatch = (source_bedrooms is not None and bedrooms is not None and abs(source_bedrooms-bedrooms)>=2)
    prose_count_present = bool(re.search(r'\b\d+\s*(?:phong ngu|pn)\b',q['sf']['norm']))
    checks['bedroom_mismatch_from_columns'] = mismatch
    # Honor structured source counts even when the source prose omits them.
    # Existing logic already uses the column values if a prose count exists.
    if mismatch and not prose_count_present:
        old_grade = grade
        if grade == '3':
            grade = '2'
        elif grade == '2':
            near = checks['price_band'] in ['close','moderate'] and checks['area_band'] in ['close','moderate']
            project = grader.clean(q['source'].get('project_name'))
            same_project = bool(project and (grader.norm(project)==grader.norm(item.get('project_name')) or grader.norm(project) in features['norm']))
            if not near and (checks['common'] or same_project):
                grade = '1'
        endings = {
            '0':'Chênh công năng/phân khúc đến mức không có giá trị thay thế thực tế.',
            '1':'Còn liên quan nhưng nhiều đánh đổi lớn.',
            '2':'Là phương án tương tự đáng cân nhắc, còn đánh đổi đáng kể.',
            '3':'Rất tương tự trên các ưu tiên chính, chưa thấy đánh đổi lớn.',
        }
        reason = reason.removesuffix(endings[old_grade]).rstrip()
        reason += ' Hạn chế cấu trúc theo cột: '+grader.fmt(bedrooms)+' PN so với '+grader.fmt(source_bedrooms)+' PN của nguồn; đây là đánh đổi mềm. '+endings[grade]
    return grade,reason,checks

def verify(outputs, before, tasks):
    report = {}
    for split, (fields, rows) in outputs.items():
        old_fields, old_rows = before[split]
        assert fields == old_fields and len(rows) == len(old_rows)
        changed = 0
        preserved = 0
        for old, row in zip(old_rows, rows):
            assert all(old[col] == row[col] for col in fields if col not in grader.EDITABLE)
            if old['grade'].strip():
                assert old == row, 'An existing annotation was modified'
                preserved += 1
            else:
                assert row['annotator_id'] == 'AI01'
                assert POLICY_ID in row['reason']
                changed += 1
            assert row['grade'] in {'0','1','2','3'}
            assert row['annotator_id'].strip() and row['reason'].strip()
        actual = {(row['query_id'],row['listing_key']) for row in rows}
        expected = {(t['query_id'],k) for t in tasks.values() if t['split'] == split for k in t['relevance']}
        assert actual == expected and len(actual) == len(rows)
        report[split] = {
            'pairs': len(rows), 'queries': len({row['query_id'] for row in rows}),
            'newly_graded_pairs': changed, 'previously_graded_rows_preserved': preserved,
            'pending_pairs': 0,
            'grade_distribution': dict(collections.Counter(row['grade'] for row in rows)),
            'non_annotation_cells_unchanged': True, 'existing_annotation_rows_unchanged': True,
            'field_and_row_order_preserved': True, 'pairs_match_original_jsonl': True,
        }
    return report

def run(commit=False):
    queries, catalog, _, before, manifest, _ = grader.load()
    # The prior loader's conflict signals are historical provenance. Under the
    # explicit new user policy they no longer block a grade or modify a scalar.
    input_hashes = {split:sha(DATA/f'annotation_{split}.csv') for split in before}
    tasks = {}
    for split in ['tune','test']:
        for line in (DATA/f'queries_{split}.jsonl').read_text().split('\n'):
            if line.strip():
                task = json.loads(line)
                task['_as_of'] = datetime.fromisoformat(task['as_of'])
                tasks[task['query_id']] = task
    outputs = {}; decisions = []
    for split, (fields, rows) in before.items():
        result = []
        for row in rows:
            if row['grade'].strip():
                result.append(dict(row))
                continue
            q = queries[row['query_id']]
            item = catalog[row['listing_key']]
            # Confirm the snapshot fields really are the columns currently
            # provided by the user, before relying on their associated IDs.
            for key in ['province','district','ward','price_period']:
                assert row[key] == (item.get(key) or ''), (row['query_id'], key)
            for key in ['price_vnd','area_m2','bedrooms']:
                assert grader.number(row[key]) == grader.number(item.get(key)), (row['query_id'],key)
            errors, unknown, ignored = policy_hard_issues(q, row, item, tasks[q['query_id']])
            assert not unknown, ('An actual missing mandatory value remains', q['query_id'], unknown)
            features = features_from_offer(row)
            if errors:
                grade = '0'
                reason = 'Vi phạm điều kiện bắt buộc theo dữ liệu cột: '+'; '.join(errors)+'.'
                checks = {}
            elif q['method'] == 'preferences':
                grade, reason, checks = grader.grade_pref(q,row,item,features)
            else:
                grade, reason, checks = similar_from_columns(q,row,item,features)
            # Make the authority rule auditable in each newly completed row.
            note = ' Quy ước người dùng 05/11/2026 ['+POLICY_ID+']: giá, kỳ hạn giá, địa bàn, số phòng ngủ và diện tích đối chiếu lấy từ cột.'
            if 'diện tích' in row['reason']:
                note += ' Diện tích cột là mốc đất/diện tích quy chiếu; diện tích sàn trong mô tả được xét riêng.'
            if q['method'] == 'similar' and 'Tin nguồn chưa đủ nhất quán' in row['reason']:
                source = q['source']
                note += ' Nguồn theo cột snapshot: '+source['district']+', '+grader.fmt(source['price_vnd'])+' đồng, '+grader.fmt(source['area_m2'])+' m².'
            if 'phòng ngủ cột' in row['reason']:
                note += ' Số phòng ngủ dùng kiểm tra: '+grader.fmt(grader.number(row['bedrooms']))+' PN.'
            result.append({**row,'grade':grade,'annotator_id':'AI01','reason':reason+note})
            decisions.append({
                'split': split, 'query_id':row['query_id'],'listing_key':row['listing_key'],
                'method':q['method'],'category':q['category'],'policy_id':POLICY_ID,
                'previous_grade':row['grade'],'previous_reason':row['reason'],
                'grade':grade,'annotator_id':'AI01','reason':reason+note,
                'checks':checks,'hard_errors':errors,'ignored_prose_bedroom_conflicts':ignored,
                'authoritative_columns':{k:row[k] for k in ['price_vnd','price_period','area_m2','province','district','ward','bedrooms']},
            })
        outputs[split] = (fields, result)
    report = verify(outputs,before,tasks)
    assert decisions, 'No blank annotations remain; refusing to produce a duplicate completion run'
    for split,(fields,rows) in outputs.items():
        grader.write_csv(WORK/f'annotation_{split}.csv',fields,rows)
    (WORK/'decisions.jsonl').write_text(''.join(json.dumps(row,ensure_ascii=False)+'\n' for row in decisions),encoding='utf-8')
    (WORK/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'report':report,'new_annotations':len(decisions),'commit':commit},ensure_ascii=False,indent=2))
    if not commit:
        return
    assert all(sha(DATA/f'annotation_{split}.csv') == digest for split,digest in input_hashes.items()), 'Inputs changed while grading'
    OUT.mkdir(exist_ok=False)
    backup = OUT/'before_completion';backup.mkdir()
    for split in ['tune','test']:
        src = DATA/f'annotation_{split}.csv'
        shutil.copy2(src,backup/src.name)
    for split in ['tune','test']:
        src = DATA/f'annotation_{split}.csv'
        shutil.copy2(WORK/src.name,src)
    after = {split:grader.read_csv(DATA/f'annotation_{split}.csv') for split in ['tune','test']}
    assert verify(after,before,tasks) == report
    summary = {
        'completed_on':'2026-11-05','policy_id':POLICY_ID,'policy':POLICY,
        'annotator_id_for_new_rows':'AI01','annotator_type':'AI rule-based draft with user-confirmed column precedence',
        'labels_status':'all_pairs_graded_ai_draft','human_relevance_verified':False,
        'dataset_version':manifest['dataset_version'],'as_of':manifest['as_of'],
        'newly_graded_pairs':len(decisions),'previously_graded_rows_preserved':sum(r['previously_graded_rows_preserved'] for r in report.values()),
        'total_pairs':sum(r['pairs'] for r in report.values()),'pending_pairs':0,
        'ready_for_full_import':True,'stats':report,
        'source_files_sha256':{name:sha(DATA/name) for name in ['queries.csv','queries_tune.jsonl','queries_test.jsonl','manifest.json']},
        'previous_grading_procedure_sha256':sha(PREVIOUS/'annotation_procedure.py'),
        'annotation_files_sha256':{s:sha(DATA/f'annotation_{s}.csv') for s in ['tune','test']},
        'pre_completion_files_sha256':input_hashes,
        'model_scores_used':False,'ranking_metrics_run':False,'weights_changed':False,
        'historical_review_directory':PREVIOUS.name,
    }
    (OUT/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    shutil.copy2(WORK/'decisions.jsonl',OUT/'completed_decisions.jsonl')
    shutil.copy2(Path(__file__),OUT/'completion_procedure.py')
    shutil.copy2(WORK/'README.md',OUT/'README.md')
    grader.write_csv(OUT/'review_required.csv',['split','query_id','listing_key','reason'],[])
    query_rows=[]
    for qid,q in queries.items():
        group=[row for _,rows in after.values() for row in rows if row['query_id']==qid]
        counts=collections.Counter(row['grade'] for row in group)
        query_rows.append({'query_id':qid,'split':q['split'],'market':q['market'],'method':q['method'],'category':q['category'],
                           'pairs':len(group),**{f'grade_{g}':counts[g] for g in ['0','1','2','3']},'review_required':0})
    grader.write_csv(OUT/'query_summary.csv',list(query_rows[0]),query_rows)

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--commit',action='store_true')
    args=parser.parse_args()
    run(args.commit)

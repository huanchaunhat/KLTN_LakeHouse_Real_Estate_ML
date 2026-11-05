"""Build Vietnamese Markdown/CSV comparisons from the completed experiment."""
import csv
import hashlib
import json
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parent
SEARCH = ROOT/'weight_search'
REPO = ROOT.parents[3]


def read(path):
    return json.loads(path.read_text(encoding='utf-8'))


def value(number):
    return 'N/A' if number is None else f'{number:.6f}'


def run():
    metrics = read(SEARCH/'metrics.json')
    verified = read(SEARCH/'verification.json')
    lock = read(SEARCH/'locked_weights.json')
    plan = read(SEARCH/'plan.json')
    intervals = read(SEARCH/'test_paired_bootstrap.json')
    policy = read(ROOT/'ai_policy_validation.json')
    mechanical = read(ROOT/'candidate_mechanical.json')
    comparison = []
    for split in ['tune', 'test']:
        for group, before in metrics[f'baseline_{split}'].items():
            after = metrics[f'selected_{split}'][group]
            row = dict(split=split, group=group, n_queries=before['n_queries'])
            for metric in ['ndcg@10', 'precision@10', 'pooled_recall@10', 'precision_grade_ge_2@10']:
                row['baseline_'+metric] = before[metric]
                row['selected_'+metric] = after[metric]
                row['delta_'+metric] = after[metric]-before[metric] if before[metric] is not None and after[metric] is not None else None
            comparison.append(row)
    with (ROOT/'comparison.csv').open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(comparison[0]))
        writer.writeheader()
        writer.writerows(comparison)
    query_comparison = []
    for split in ['tune', 'test']:
        before = [json.loads(line) for line in (SEARCH/f'baseline_{split}_per_query.jsonl').read_text().splitlines()]
        after = [json.loads(line) for line in (SEARCH/f'selected_{split}_per_query.jsonl').read_text().splitlines()]
        for left, right in zip(before, after):
            assert left['query_id'] == right['query_id']
            query_comparison.append(dict(query_id=left['query_id'], split=split, market=left['market'],
                                         method=left['method'], judged_pool_size=left['judged_pool_size'],
                                         returned_baseline=left['returned'], returned_selected=right['returned'],
                                         baseline_ndcg=left['ndcg@10'], selected_ndcg=right['ndcg@10'],
                                         delta_ndcg=right['ndcg@10']-left['ndcg@10'] if left['ndcg@10'] is not None else None))
    with (ROOT/'query_comparison.csv').open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(query_comparison[0]))
        writer.writeheader()
        writer.writerows(query_comparison)
    snapshot = ROOT/'implementation'
    snapshot.mkdir(exist_ok=True)
    code_hashes = {}
    for relative in ['recommendation/tune.py', 'scripts/run_recommendation_tuning.py',
                     'recommendation/tests/test_tune.py', 'scripts/import_recommendation_grades.py',
                     'recommendation/engine.py', 'recommendation/evaluate.py', 'recommendation/contract.py']:
        path = REPO/relative
        destination = snapshot/relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
        code_hashes[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    (ROOT/'implementation_checksums.json').write_text(json.dumps(code_hashes, indent=2)+'\n')
    text = [
        '# Kết quả đánh giá và tinh chỉnh trọng số gợi ý — 05/11/2026',
        '',
        'Đã hoàn tất nhập nhãn, đo baseline, tìm trọng số trên tune, khóa trọng số, '
        'đánh giá test và tạo bundle độc lập. Kết quả dưới đây đo mức phù hợp với '
        '**bộ nhãn AI sơ bộ**, chưa chứng minh mức hài lòng của người dùng thực.',
        '',
        '## Dữ liệu và phương pháp',
        '',
        f'- Dataset: `{plan["dataset_version"]}`.',
        f'- Giữ nguyên `as_of={plan["as_of"]}`, cửa sổ 90 ngày và snapshot TP.HCM.',
        '- 240 truy vấn tổng hợp từ catalog: 120 tune và 120 test; mỗi tập có '
        '60 sale, 60 rent, mỗi thị trường có 30 similar và 30 preferences.',
        '- 8.924 nhãn gốc được giữ nguyên. Nhập thành `labels_tune.jsonl` '
        'và `labels_test.jsonl` ở thư mục cha của thí nghiệm.',
        '- `labels_status=unjudged` trong manifest bộ truy vấn là thông tin lịch sử lúc tạo '
        'pool và được giữ nguyên để bảo toàn checksum. Trạng thái hoàn tất của lần chạy '
        'này nằm trong `weight_search/verification.json`.',
        f'- Chấm bổ sung {verified["additional_ai_pairs"]["tune"]:,} cặp tune và '
        f'{verified["additional_ai_pairs"]["test"]:,} cặp test bằng cùng quy trình AI01 '
        '`user_column_authority_20261105`, lưu riêng; không sửa CSV/query gốc.',
        '- Pool tune được mở rộng bằng hợp nhất Top-10 của toàn bộ cấu hình đã định trước. '
        'AI chỉ nhận thông tin nhu cầu và snapshot, không nhận điểm mô hình hoặc thứ hạng.',
        '- Thử 271 cấu hình mỗi thị trường: baseline, text-heavy, attribute-heavy, '
        '12 thay đổi từng trọng số quanh baseline và 256 mẫu Dirichlet có seed. '
        'Seed sale=20261105, rent=20261106. Chọn nDCG@10 trung bình cao nhất trên tune; '
        'nếu hòa, ưu tiên gần baseline rồi chỉ số cấu hình nhỏ hơn.',
        '- Một bộ sáu trọng số dùng chung cho similar/preferences trong mỗi thị trường. '
        'Preferences hiện chỉ có điểm text/price/area/freshness; vị trí/phòng ngủ vẫn là bộ lọc.',
        f'- Khóa trọng số tại `{lock["frozen_at"]}` trước khi tính chỉ số test. '
        'Pool test chỉ mở rộng từ kết quả baseline và cấu hình đã chốt; không chọn lại theo test.',
        '- Truy hồi trên toàn bộ ứng viên hợp lệ; không giới hạn engine vào pool đã chấm, '
        'không thay nhãn thiếu thành 0. Không huấn luyện lại TF-IDF.',
        '',
        '## Kết quả test trên cùng pool đã mở rộng',
        '',
        '| Nhóm | Số truy vấn | nDCG baseline | nDCG mới | Chênh lệch | P@10 baseline | P@10 mới |',
        '|---|---:|---:|---:|---:|---:|---:|',
    ]
    for group in ['all', 'sale', 'rent', 'sale/similar', 'sale/preferences', 'rent/similar', 'rent/preferences']:
        row = next(r for r in comparison if r['split']=='test' and r['group']==group)
        text.append(f'| {group} | {row["n_queries"]} | {value(row["baseline_ndcg@10"])} | '
                    f'{value(row["selected_ndcg@10"])} | {row["delta_ndcg@10"]:+.6f} | '
                    f'{value(row["baseline_precision@10"])} | {value(row["selected_precision@10"])} |')
    overall = next(r for r in comparison if r['split']=='test' and r['group']=='all')
    rent = next(r for r in comparison if r['split']=='test' and r['group']=='rent')
    text += ['', f'nDCG@10 trung bình tăng {overall["delta_ndcg@10"]:+.6f} '
             f'({overall["delta_ndcg@10"]/overall["baseline_ndcg@10"]*100:+.2f}% tương đối). '
             f'Precision@10 chung thay đổi {overall["delta_precision@10"]:+.6f}; '
             f'riêng rent thay đổi {rent["delta_precision@10"]:+.6f}. '
             'Đây là sự đánh đổi giữa thứ tự theo điểm 0–3 và số tin có điểm lớn hơn 0, '
             'không phải cải thiện đồng đều mọi chỉ số.']
    text += ['', 'Khoảng bootstrap 95% cho chênh lệch nDCG@10, lấy mẫu theo cặp truy vấn '
             '(10.000 lần, seed cố định):', '', '| Nhóm | Trung bình chênh lệch | Khoảng 95% |', '|---|---:|---|']
    for group in ['all', 'sale', 'rent']:
        entry = intervals[group]
        low, high = entry['percentile_95_interval']
        text.append(f'| {group} | {entry["mean_delta"]:+.6f} | [{low:+.6f}; {high:+.6f}] |')
    rent_low, rent_high = intervals['rent']['percentile_95_interval']
    if rent_low <= 0 <= rent_high:
        text += ['', 'Khoảng của rent chứa 0: dù trung bình nDCG tăng, mức cải thiện còn '
                 'chưa rõ theo bootstrap truy vấn. Giữ cấu hình này là candidate để rà soát, '
                 'không coi kết quả là đủ để tự động thay mô hình đang phục vụ.']
    text += ['', 'Nếu khoảng chứa 0, chưa có bằng chứng rõ về cải thiện nhất quán theo truy vấn '
             'trong bộ test này. Khoảng này không phản ánh sai số của người chấm, nhu cầu thực '
             'hoặc việc khái quát sang tỉnh/thành khác.', '', '## Kết quả tune trên cùng pool đã mở rộng', '',
             '| Thị trường | nDCG baseline | nDCG mới | Chênh lệch |', '|---|---:|---:|---:|']
    for group in ['sale', 'rent']:
        row = next(r for r in comparison if r['split']=='tune' and r['group']==group)
        text.append(f'| {group} | {value(row["baseline_ndcg@10"])} | {value(row["selected_ndcg@10"])} | {row["delta_ndcg@10"]:+.6f} |')
    text += ['', 'Baseline tune trên pool gốc nằm ở `baseline_tune.json`. Không so trực tiếp '
             'nDCG đó với nDCG sau mở rộng pool: nhãn bổ sung có thể thay đổi IDCG. '
             'Tất cả so sánh trước/sau trong báo cáo này dùng chung pool mở rộng.', '',
             '## Trọng số được chọn', '', '| Thành phần | Baseline | Sale | Rent |', '|---|---:|---:|---:|']
    for name, number in plan['baseline_weights']['sale'].items():
        text.append(f'| {name} | {number:.6f} | {lock["selected_weights"]["sale"][name]:.6f} | {lock["selected_weights"]["rent"][name]:.6f} |')
    text += ['', 'Đây là cấu hình tốt nhất trong phạm vi tìm kiếm đã khai báo và bộ nhãn AI hiện tại; '
             'không phải trọng số tối ưu phổ quát hoặc kết luận nhân quả về tầm quan trọng từng thuộc tính.', '',
             '## Cách đọc chỉ số và giới hạn', '',
             '- nDCG@10 dùng gain `2^grade-1`. Precision/pooled Recall hiện coi `grade > 0` là phù hợp, '
             'bao gồm cả điểm 1 (phù hợp yếu). Vì vậy không diễn giải Precision cao thành tỷ lệ hài lòng thực.',
             '- Có thêm `precision_grade_ge_2@10` trong CSV/JSON để xem số tin đạt mức đáng cân nhắc trở lên; '
             'chỉ số này không được dùng để chọn lại trọng số sau khi xem test.',
             '- Pooled Recall chỉ đo trên tập đã chấm, không phải Recall toàn kho.',
             '- Precision@10 vẫn chia cho 10 khi trả dưới 10 tin. Xem số truy vấn thiếu/rỗng '
             'trong `weight_search/metrics.json` và chi tiết trong `query_comparison.csv`.',
             '- Nhãn AI dựa trên quy tắc và cùng dữ liệu thuộc tính mà engine sử dụng, nên có thể '
             'ưu ái các quy tắc tương tự. Cần con người chấm độc lập trước khi dùng để kết luận khoa học '
             'về mức phù hợp hoặc thay thế mô hình phục vụ.', '',
             '## Xác minh và bundle', '',
             '- 20 kiểm thử core/tuning đã đạt; gồm đối chiếu cache với engine, tie/trùng tin, '
             'pool ngắn, nhãn thiếu, từ chối dùng test để chọn trọng số và bootstrap theo cặp.',
             f'- Adapter chấm bổ sung tái lập đúng điểm của {policy["matching_grades"]}/{policy["reference_pairs"]} '
             'quyết định AI01 đã hoàn tất ngày 05/11; đây là kiểm tra nhất quán quy trình, không phải xác nhận nhãn đúng.',
             '- Đã đối chiếu Top-10 cache và engine phục vụ trên toàn bộ tune/test của baseline và cấu hình mới; '
             'các chỉ số khớp evaluator gốc. Mọi Top-10 được đo đều có nhãn.',
             '- SHA-256 của nguồn CSV/query/JSONL/bundle giữ nguyên; file khóa trọng số giữ nguyên sau test. '
             'Bundle mới nạp lại và cho cùng kết quả test.',
             f'- Bundle độc lập: `{verified["candidate_bundle"]}`. Catalog, matrix và vectorizer giữ '
             'nguyên checksum; manifest lưu trọng số và trạng thái nhãn AI chưa được con người xác nhận.',
             '- Benchmark trực tiếp engine trên 15 tin mẫu mỗi thị trường:', '',
             '| Thị trường | Vi phạm similar | Vi phạm preferences | p95 similar (ms) | p95 preferences (ms) |',
             '|---|---:|---:|---:|---:|']
    for market, stats in mechanical['markets'].items():
        text.append(f'| {market} | {stats["similar_violations"]} | {stats["preference_violations"]} | '
                    f'{stats["similar_p95_ms"]} | {stats["preference_p95_ms"]} |')
    text += ['', 'Benchmark này đo trực tiếp engine trên máy hiện tại; không phải tải HTTP đồng thời '
             'hoặc cam kết hiệu năng sản phẩm. Chưa đăng ký MLflow, đổi alias hay nạp lại API.', '',
             '## Tệp sử dụng', '',
             '- `comparison.csv`: chỉ số trước/sau theo split, thị trường và phương thức.',
             '- `query_comparison.csv`: chênh lệch theo từng truy vấn để rà soát.',
             '- `weight_search/locked_weights.json`: trọng số đã khóa trước test.',
             '- `weight_search/labels_tune_expanded.jsonl`, `labels_test_expanded.jsonl`: bộ nhãn dùng so sánh.',
             '- `weight_search/additional_annotations_*.csv`, `additional_decisions_*.jsonl`: nhãn AI mới và dấu vết.',
             '- `weight_search/plan.json`, `search_sale.json`, `search_rent.json`: seed, cấu hình và kết quả tìm kiếm.',
             '- `weight_search/verification.json`, `ai_policy_validation.json`, `candidate_mechanical.json`: xác minh.',
             '- `implementation/`, `implementation_checksums.json`: bản sao mã và checksum để đối chiếu.', '',
             '## Tái lập', '',
             'Chạy từ gốc repository, scikit-learn 1.6.1, dùng thư mục kết quả và bundle đầu ra mới:', '',
             '```bash',
             'OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python3 scripts/run_recommendation_tuning.py \\',
             '  --data docs/evaluation/query_set_20261103 \\',
             '  --output docs/evaluation/query_set_20261103/experiment_reproduction \\',
             '  --candidate-bundle artifacts/recommendation/models/weights_reproduction \\',
             '  --ai-grader docs/evaluation/query_set_20261103/ai_review_20261105/completion_procedure.py \\',
             '  --seed 20261105 --random-count 256 --k 10',
             '```', '',
             'Chỉ chạy tái lập để kiểm tra kết quả; không dùng các lần chạy test để điều chỉnh lại cấu hình.']
    (ROOT/'BAO_CAO_KET_QUA.md').write_text('\n'.join(text)+'\n', encoding='utf-8')
    print('Wrote BAO_CAO_KET_QUA.md, comparison.csv, query_comparison.csv and implementation snapshots')


if __name__ == '__main__':
    run()

"""Snapshot-only AI annotation draft. No model scores, rank, or pool audit input."""
import argparse
import collections
import csv
import hashlib
import json
import math
import re
import shutil
import unicodedata
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path('/Users/macbookpro/2025-2026/KhoaLuanTotNghiep/LakeHouse-Architecture-For-RealEstateData')
DATA = ROOT / 'docs/evaluation/query_set_20261103'
WORK = Path('/private/tmp/reip_annotation_20261103')
OUT = DATA / 'ai_review_20261103'
AUTHOR = 'AI01'
EDITABLE = {'grade', 'annotator_id', 'reason'}

def norm(s):
    return ''.join(c for c in unicodedata.normalize('NFKD', str(s or '').lower().replace('đ','d')) if not unicodedata.combining(c))

def clean(s):
    return unicodedata.normalize('NFKC', str(s or '')).replace('\\n', '\n').replace('\u0085','\n').replace('\u2028','\n').replace('\u2029','\n').replace('\ufe0f','').strip()

def number(s):
    try:
        v = float(s)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None

def fmt(v):
    if v is None:
        return 'chưa rõ'
    return f'{v:,.0f}' if v == int(v) else f'{v:,.1f}'.rstrip('0').rstrip('.')

def read_csv(p):
    with p.open(encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        return reader.fieldnames, list(reader)

def write_csv(p, fields, rows):
    with p.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

def digest(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

# Each pattern represents a concrete assertion, not a bag of overlapping words.
# A positive clause must refer to the offered property; boilerplate is trimmed.
PATTERNS = {
    'car': r'\b(?:hem\s+(?:xe\s*)?(?:hoi|oto|o to)|hxh|hx\s*hoi|xe hoi\s+(?:vao|dau|do|toi|t[oa]n)|o\s*to\s+(?:vao|dau|do)|oto\s+(?:vao|dau|do)|duong\s+xe\s+(?:hoi|tai)|xe\s+tai\s+(?:vao|ra vao)|hem\s+\d+(?:[.,]\d+)?\s*m\s*(?:xe hoi|o to)|gara|garage|xe\s*7\s*cho\s+vao)\b',
    'front': r'\b(?:mat tien|mtkd|hai mt|2mt|2 mt|goc 2 mat|nha mt\b|dat mt\b|cho thue mt\b|ban mt\b|mt duong\b|mt\s+[a-z])',
    'business': r'\b(?:kinh doanh|buon ban|cua hang|showroom|spa|shop|mat bang|mtkd|tap hoa)\b',
    'quiet': r'\b(?:yen tinh|yen lang|khong on ao)\b',
    'market': r'\b(?:gan|sat|cach|ngay|ke|doi dien|ben|ra|canh)[^\n.;]{0,65}\bcho\b|\bcho\s+[^\n.;]{0,30}\b(?:gan|ke ben)\b',
    'school': r'\b(?:gan|sat|cach|ngay|ke|doi dien|ben|ra|canh)[^\n.;]{0,90}\b(?:truong|dai hoc|tieu hoc|thcs|thpt|mam non|dh|huit|hutech|rmit|van lang|bach khoa)\b',
    'new': r'\b(?:nha moi|moi xay|xay moi|chua ai o|chua su dung|moi [\d.]+%|moi [\d]+|nha vua xay|vua sua|sua moi)\b',
    'full_furniture': r'\b(?:full\s+(?:noi that|nt)|day du\s+(?:noi that|tien nghi)|noi that\s+day du|tang\s+(?:toan bo|full)\s+noi that|de lai\s+(?:toan bo|het)\s+noi that|san full\s+noi that)\b',
    'furniture': r'\b(?:noi that|giuong|tu lanh|sofa|may lanh|ban ghe|tu quan ao)\b',
    'private_title': r'\b(?:so hong rieng|so do rieng|so rieng|shr)\b',
    'residential_land': r'\b(?:tho cu|dat o|odt|ont|xay dung tu do|dat xay dung)\b',
    'balcony': r'\b(?:ban cong|balcony|balcon|bancol|bancong|logia|lo gia)\b',
    'security': r'\b(?:bao ve|bao an)\b',
    'pool': r'\b(?:ho boi|be boi|swimming pool)\b',
    'airy': r'\b(?:thoang mat|thoang sang|thoang gio|don gio|nhieu anh sang|cua so lon|cua so rong|2 mat thoang|hai mat thoang|view thoang|thong thoang)\b',
    'loft': r'\b(?:co gac|gac lung|duplex|gac cao|gac dep|gac moi|gac rong|phong gac)\b',
    'window': r'\b(?:cua so|cua kinh lon)\b',
    'ac': r'\b(?:may lanh|dieu hoa|air con)\b',
    'no_landlord': r'\b(?:khong|ko|k)\s+(?:o\s+)?chung chu\b',
    'parking': r'\b(?:cho\s+(?:de|dau|do|giu)\s*xe|bai\s*(?:de|dau|do|giu)?\s*xe|ham\s*xe|nha\s*xe|khu\s*(?:de|giu)\s*xe|giu xe|gui xe|free xe|mien phi\s*(?:\d+\s*)?xe|xe\s*(?:\d+\s*k|free|mien phi))\b',
    'truck': r'\b(?:xe tai|container|cong ten no|cont\b|xe cong)\b',
    'warehouse': r'\b(?:kho xuong|nha xuong|kho bai|kho chua|nha kho|thue kho|ban kho|lam kho|kho hang|xuong san xuat|xuong may|xuong in)\b',
    'office': r'\b(?:van phong|vpct|vpcty|vp cong ty|toa nha van phong|building)\b',
    'elevator': r'\bthang may\b',
    'residential_neighborhood': r'\b(?:khu dan cu|kdc|dan cu hien huu|dan cu dong|khu dan tri|khu pho|nha dan|khu nha o)\b',
    'convenient': r'\b(?:giao thong thuan tien|thuan tien di chuyen|thuan tien ket noi|duong thong|hem thong|gan cho|gan truong|tien ich xung quanh|day du tien ich|ket noi thuan tien)\b',
    'shared': r'\b(?:o ghep|phong ghep|ky tuc xa|ktx|tuyen\s+(?:\d+\s*)?ban|tim\s+(?:\d+\s*)?ban\s+(?:nam|nu))\b',
    'daily': r'\b(?:theo ngay|theo gio|thue ngay|thue gio|homestay|airbnb|staycation)\b|/\s*(?:ngay|dem|gio)\b',
    'villa': r'\b(?:biet thu|villa|biet phu)\b',
    'shophouse': r'\b(?:shophouse|can ho tret|can tret|tang tret[^\n.]{0,60}kinh doanh)\b',
    'studio': r'\b(?:studio|duplex|can ho dich vu|chdv|phong tro)\b',
    'farm': r'\b(?:nong nghiep|trong lua|trong cay|dat vuon|trong thuy san|nuoi trong|vuon dua|canh tac|trang trai|dat rung)\b',
    'industrial': r'\b(?:dat cong nghiep|dat san xuat|dat skc|dat khu cong nghiep)\b',
    'transfer_business': r'\b(?:sang nhuong|sang quan|sang tiem|sang mat bang|sang lai quan)\b',
}
NEGATIVE = {
    'front': r'\b(?:cach|gan|sat|ra|toi)\s+(?:duong\s+)?(?:mat tien|mt)\b|quy hoach[^\n.]{0,40}(?:mat tien|duong)|don dau mat tien',
    'car': r'\b(?:hem nho|hem xe may|hem ba gac|hem 3 gac|xe hoi khong vao|khong vao duoc xe hoi|khong co gara)\b',
    'balcony': r'\b(?:khong|ko|k)\s+(?:co\s+)?(?:ban cong|balcony|balcon)\b',
    'full_furniture': r'\b(?:khong\s+(?:co\s+)?noi that|noi that\s*:\s*trong|ban giao trong|tu trang bi noi that)\b',
    'private_title': r'\b(?:so hong chung|so chung|vi bang|chung so|chua co so|khong co so|giay tay)\b',
    'residential_land': r'\b(?:khong co tho cu|chua co tho cu|chua len tho cu)\b',
    'loft': r'\b(?:khong|ko)\s+(?:co\s+)?gac\b',
    'window': r'\b(?:khong|ko)\s+(?:co\s+)?cua so\b',
    'ac': r'\b(?:khong|ko)\s+(?:co\s+)?(?:may lanh|dieu hoa)\b',
    'no_landlord': r'(?<!khong )(?<!ko )(?<!k )\bchung chu\b',
    'elevator': r'\b(?:khong|ko)\s+(?:co\s+)?thang may\b',
    'new': r'\b(?:nha cu|cu tien xay|tien cai tao|xuong cap|can sua chua)\b',
    'quiet': r'\b(?:on ao|rat on|nhon nhip|sam uat)\b',
    'parking': r'\b(?:khong|ko)\s+(?:co\s+)?(?:cho de xe|cho dau xe|ham xe)\b',
    'pool': r'\b(?:khong|ko)\s+(?:co\s+)?ho boi\b',
}

LABELS = {
    'car':'hẻm/đường cho xe hơi', 'front':'mặt tiền', 'business':'công năng kinh doanh',
    'quiet':'khu ở yên tĩnh', 'market':'gần chợ', 'school':'gần trường học',
    'new':'nhà mới', 'full_furniture':'đầy đủ nội thất', 'furniture':'có nội thất',
    'private_title':'sổ riêng', 'residential_land':'đất ở/thổ cư', 'balcony':'ban công',
    'security':'bảo vệ', 'pool':'hồ bơi', 'airy':'thoáng mát', 'loft':'gác',
    'window':'cửa sổ', 'ac':'máy lạnh', 'no_landlord':'không chung chủ', 'parking':'chỗ để xe',
    'truck':'đường xe tải', 'warehouse':'kho/xưởng', 'office':'văn phòng', 'elevator':'thang máy',
    'residential_neighborhood':'khu dân cư', 'convenient':'kết nối/tiện ích được mô tả',
    'shared':'ở ghép/thuê chỗ', 'daily':'thuê ngắn ngày', 'villa':'biệt thự',
    'shophouse':'căn hộ trệt kinh doanh', 'studio':'studio/căn hộ dịch vụ',
    'farm':'đất nông nghiệp/vườn', 'industrial':'đất công nghiệp', 'transfer_business':'sang nhượng hoạt động kinh doanh',
}

def offer_text(r):
    desc = clean(r.get('description', ''))
    # Do not use unrelated listings, ad agency inventory, or hashtag spam as evidence.
    for pat in [r'(?i)rổ hàng', r'(?i)bđs rao bán.*tham khảo', r'(?i)anh chị tham khảo thêm',
                r'(?i)ngoài ra.*(?:căn|nhà|phòng|kho)', r'(?i)các sản phẩm khác',r'(?i)\n\s*CĂN\s*2\s*:']:
        match = re.search(pat, desc)
        if match:
            desc = desc[:match.start()]
    desc='\n'.join(line for line in desc.split('\n') if line.count('#')<3).replace('#','')
    return clean(r.get('title','')) + '\n' + desc

def extract(r):
    text = offer_text(r)
    # Normalization retains offsets within normalized text for readable evidence.
    t = norm(text)
    features, evidence, negatives = set(), {}, set()
    for key, pattern in PATTERNS.items():
        m = re.search(pattern, t)
        neg = re.search(NEGATIVE[key], t) if key in NEGATIVE else None
        if neg:
            negatives.add(key)
        if m:
            # Local negation immediately before a positive assertion.
            pre = t[max(0,m.start()-24):m.start()]
            if key != 'no_landlord' and re.search(r'(?:khong|ko|k)\s+(?:co\s+)?$', pre):
                negatives.add(key)
                continue
            if key == 'front':
                if re.search(r'(?:cach|gan|sat|ra|toi)\s*(?:\d+\s*m\s*)?$',pre):
                    continue
                if re.search(r'(?:quy hoach|don dau).{0,25}$',pre):
                    continue
            if key == 'ac' and re.search(r'(?:lap|gan)\s+them|neu can|them phi',t[max(0,m.start()-15):m.end()+20]):
                continue
            if key == 'market' and re.search(r'\bcho\s+(?:thue|vay|khach|gia|anh|cac|moi)\b',m.group()):
                continue
            features.add(key)
            evidence[key] = text[max(0,m.start()-15):min(len(text),m.end()+55)].replace('\n',' ').strip()
    furn = norm(r.get('furnishing',''))
    if 'day du' in furn and 'full_furniture' not in negatives:
        features.update(['full_furniture','furniture'])
        evidence['full_furniture']='cột furnishing: '+str(r.get('furnishing',''))
    if features & {'full_furniture'}:
        features.add('furniture')
        evidence.setdefault('furniture',evidence['full_furniture'])
    elif 'cao cap' in furn:
        features.add('furniture')
        evidence.setdefault('furniture','cột furnishing: '+str(r.get('furnishing','')))
    if 'full_furniture' in negatives:
        features.discard('full_furniture')
    if 'private_title' in negatives:
        features.discard('private_title')
    if 'no_landlord' in features:
        negatives.discard('no_landlord')
    return {'text':text, 'norm':t, 'features':features, 'evidence':evidence, 'negatives':negatives}

PRIORITIES = {
 'hẻm xe hơi':['car'], 'nhà mặt tiền kinh doanh':['front','business'],
 'nhà có sổ hồng riêng':['private_title'], 'nhà khu dân cư yên tĩnh':['quiet'],
 'nhà gần chợ trường học':['market','school'], 'nhà mới có nội thất':['new','furniture'],
 'căn hộ có ban công':['balcony'], 'căn hộ đầy đủ nội thất':['full_furniture'],
 'căn hộ có bảo vệ':['security'], 'căn hộ gần trường học':['school'],
 'căn hộ có hồ bơi':['pool'], 'căn hộ thoáng mát':['airy'],
 'đất thổ cư sổ riêng':['residential_land','private_title'],
 'đất mặt tiền đường':['front'], 'đất khu dân cư':['residential_neighborhood'],
 'đất đường xe hơi':['car'], 'đất gần chợ':['market'],
 'đất vị trí thuận tiện':['convenient'], 'mặt bằng mặt tiền':['front'],
 'văn phòng có thang máy':['office','elevator'], 'mặt bằng kinh doanh':['business'],
 'kho đường xe tải':['warehouse','truck'], 'mặt bằng gần chợ':['market'],
 'văn phòng có chỗ đậu xe':['office','parking'], 'phòng không chung chủ':['no_landlord'],
 'phòng có chỗ để xe':['parking'], 'phòng đầy đủ nội thất':['full_furniture'],
 'phòng có máy lạnh':['ac'], 'phòng có gác':['loft'], 'phòng có cửa sổ':['window'],
}

def ratio(v, target):
    if not v or not target:
        return None
    return max(v/target,target/v)

def range_band(v, target):
    # These are provisional AI calibration anchors, never model scoring weights.
    r = ratio(v,target)
    if r is None: return 'unknown'
    if r <= 1.25: return 'close'
    if r <= 1.67: return 'moderate'
    if r <= 2.5: return 'large'
    return 'extreme'

def scope(f, category):
    fs=f['features']
    if category=='ROOM' and 'shared' in fs: return 'shared'
    if category=='APARTMENT':
        if 'shophouse' in fs:return 'shophouse'
        if 'studio' in fs:return 'studio'
    if category=='COMMERCIAL':
        t=f['norm']
        headline=t.split('\n')[0]
        if re.search(r'kho|xuong',headline) and 'warehouse' in fs:return 'warehouse'
        if re.search(r'van phong',headline):return 'office'
        if 'warehouse' in fs and not(fs & {'office','business'}):return 'warehouse'
        return 'business'
    if category=='LAND':
        if 'farm' in fs and 'residential_land' not in fs:return 'farm'
        if 'industrial' in fs:return 'industrial'
        if 'residential_land' in fs:return 'residential'
    if category=='HOUSE' and 'villa' in fs:return 'villa'
    return 'general'

def scalar(s):
    s=s.replace(' ','')
    if re.fullmatch(r'\d{1,3}(?:[.,]\d{3})+',s):return float(re.sub(r'[.,]','',s))
    return float(s.replace(',','.'))

def conflicts(r, f, market):
    issues=[]
    t=f['norm']
    price=number(r.get('price_vnd')); area=number(r.get('area_m2'))
    # Strong, explicit address outside snapshot province, not neighboring amenities.
    outside=r'(?:long an|tay ninh|dong nai|binh duong|ba ria|vung tau|binh phuoc|can giuoc|duc hoa|ben luc)'
    for line in t.splitlines():
        if re.search(r'\b(?:dia chi|vi tri|toa lac|thuoc)\s*:',line) and re.search(r'\b'+outside+r'\b',line):
            if not re.search(r'\b(?:giap|cach|ket noi|di ve|gan|thuan tien|lien ke)\b',line):
                issues.append('địa chỉ mô tả ngoài TP.HCM: '+line.strip()[:180])
                break
    # Explicit principal asking prices; do not use installments, loan amounts,
    # per-m2 quotes, referral commissions or the rent income of sale properties.
    candidates=[]
    title=norm(clean(r.get('title','')))
    patterns=[r'(?<!\d)(\d+(?:[.,]\d+)?)\s*(ty|ti|trieu|tr)(?=\b|\d)']
    for match in re.finditer(patterns[0],t):
        before=t[max(0,match.start()-60):match.start()]
        after=t[match.end():match.end()+40]
        line=t[:match.start()].rsplit('\n',1)[-1]+match.group()+after.split('\n',1)[0]
        if not(re.search(r'gia\s*(?:ban|thue|chao|chi|con|tu|tot|re|nhinh|:)|gia\s*[:.]|\bchi\s*[:.]?\s*$',before) or match.start()<len(title)):
            continue
        if re.search(r'coc|hoa hong|\bhh\b|vay|bank|thu nhap|dong tien|giam|chiet khau|tra truoc|thanh toan|nguoi|/m2|/m²',line):
            continue
        if re.search(r'(?:re hon|thap hon|hon thi truong|tot hon thi truong|re hon thi truong)',line):
            continue
        if market=='sale' and (match.group(2) not in ['ty','ti'] or re.search(r'thue|/\s*thang|/\s*th|moi thang',line)):
            continue
        if market=='rent' and (match.group(2) in ['ty','ti'] or re.search(r'gia ban|\bban\s*:',line)):
            continue
        if re.match(r'\s*(?:-|den|–|~)',after) or re.search(r'tu\s*$',before):
            continue
        try:
            # Unit-qualified amounts such as 2.850 tỷ mean 2.85 billion,
            # whereas an area such as 1.500 m² means 1,500 square metres.
            amount=match.group(1).replace(',','.')
            amount=float(amount) if amount.count('.')<=1 else scalar(amount)
            val=amount*(1e9 if match.group(2) in ['ty','ti'] else 1e6)
            tail=re.match(r'\s*(\d{1,3})(?!\d|[.,]\d)(?:\s*(trieu|tr|ngan|k))?',after)
            if tail and not re.match(r'\s*\d+\s*(?:pn|wc|phong|lau|tang|m2)',after):
                digits=tail.group(1)
                if match.group(2) in ['ty','ti']:
                    val+=int(digits)*1e6 if len(digits)==3 or tail.group(2) in ['trieu','tr'] else int(digits)*1e9/(10**len(digits))
                elif len(digits)<=3:
                    val+=int(digits)*1e6/(10**len(digits))
        except ValueError: continue
        if val>0:candidates.append((val,match.group().strip()))
    if price and candidates and not any(ratio(price,v)<=1.10 for v,_ in candidates):
        # Vague titles such as "hơn 7 tỷ" do not establish an exact price.
        if not re.search(r'\b(?:hon|nhinh|tu|chi tu|khoang|giam|chiet khau)\s*\d',t):
            issues.append('giá cột '+fmt(price)+' đồng khác giá rao rõ '+', '.join(dict.fromkeys(s for _,s in candidates))[:100])
    # Only explicit primary area statements. Distinguish land area and total
    # floor/living area, plus inventories with several different units.
    area_values=[]
    for m in re.finditer(r'(?:dien tich(?: dat)?|dt(?: dat)?)\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*m(?:2|²)?\b',t):
        if re.match(r'\s*[x×*❌]\s*\d',t[m.end():]):
            continue
        area_values.append((scalar(m.group(1)),m.group()))
    if area and area_values and not any(ratio(area,v)<=1.35 for v,_ in area_values):
        if len(set(v for v,_ in area_values))==1:
            issues.append('diện tích cột '+fmt(area)+' m² khác '+area_values[0][1])
    dims=[]
    for m in re.finditer(r'(?<![\dm.,])(\d{1,3}(?:[.,m]\d{1,2})?)\s*m?\s*[x×*❌]\s*(\d{1,3}(?:[.,m]\d{1,2})?)\s*m?',t):
        product=float(m.group(1).replace(',','.').replace('m','.'))*float(m.group(2).replace(',','.').replace('m','.'))
        if product>=5:dims.append((product,m.group()))
    if area and dims and not any(ratio(area,v)<=1.4 for v,_ in dims):
        # Do not confuse an explicitly stated usable/floor area with land area.
        floor_area=re.search(r'dtsd|dt\s*sd|dien tich su dung|dien tich san|tong dien tich|dt san|dtxd|dt xay dung|\d\s*m(?:2|²)?\s*san',t)
        if not floor_area and not area_values:
            issues.append('diện tích cột '+fmt(area)+' m² khác lớn kích thước '+dims[0][1]+' (khoảng '+fmt(dims[0][0])+' m²); chưa xác định đất hay sàn')
    # Daily/weekly price cannot be silently reinterpreted as a monthly price.
    if market=='rent' and 'daily' in f['features']:
        if re.search(r'\b\d[\d.,]*\s*(?:k|nghin|ngan|trieu|tr|dong|d)\s*/\s*(?:ngay|dem|gio)\b',t) and not re.search(r'\b\d[\d.,]*\s*(?:trieu|tr)\s*/?\s*thang\b',t):
            issues.append('mô tả rao giá theo ngày/đêm/giờ nhưng cột price_period là month')
    # Directly reviewed mixed advertisements. Apply the issue whenever this
    # item occurs; grades are still independently assigned per query.
    reviewed_issues={
        'a7019d84c49204563ad15e3dfde49c91f9d7a95bd55bd8c2e744498e5056f61f':'cột ghép giá 21 tỷ của xưởng 2.442 m² với diện tích 61.000 m² của lô đất rao 410 tỷ trong cùng mô tả',
        'e52d031c6ceb34b637441f85f60607ad7c79f71bb0578b2b1abf5c6ae180f81c':'cột địa bàn ghi Bình Chánh/Xã Bình Hưng nhưng mô tả ghi KDC Phạm Thế Hiển, Phường 7, Quận 8; chưa có ánh xạ xác nhận',
    }
    if r.get('listing_key') in reviewed_issues:issues.append(reviewed_issues[r['listing_key']])
    return list(dict.fromkeys(issues))

def load():
    fields,qs=read_csv(DATA/'queries.csv')
    queries={q['query_id']:q for q in qs}
    manifest=json.loads((DATA/'manifest.json').read_text())
    bundle=ROOT/manifest['bundle']
    if digest(bundle/'manifest.json')!=manifest['bundle_manifest_sha256']:
        raise ValueError('Bundle manifest does not match snapshot')
    cols=['listing_key','transaction_type','property_category','price_period','province_id','district_id',
          'duplicate_group','is_recommendable','last_seen_at','price_vnd','area_m2','bedrooms','bathrooms',
          'province','district','ward','legal_status','furnishing','title','description','project_name','property_subtype']
    wanted={q['source_listing_key'] for q in qs}
    originals={}
    for split in ['tune','test']:
        f,rows=read_csv(DATA/f'annotation_{split}.csv')
        originals[split]=(f,rows)
        wanted.update(r['listing_key'] for r in rows)
    catalog={}
    for market in ['sale','rent']:
        for r in pq.read_table(bundle/market/'catalog.parquet',columns=cols).to_pylist():
            if r['listing_key'] in wanted:catalog[r['listing_key']]=r
    feats={}
    source_issues={}
    for q in qs:
        q['request']=json.loads(q['request_json'])
        source=catalog[q['source_listing_key']]
        q['source']=source
        q['sf']=extract({**source,'title':q['source_title'],'description':q['source_description']})
        if q['method']=='similar':
            source_issues[q['query_id']]=conflicts(source,q['sf'],q['market'])
    # Qualitative source review: confirmed from the supplied source text only.
    source_issues['sale_similar_house_015']=['cột địa bàn là Bình Chánh, TP.HCM nhưng mô tả ghi KDC Đặng Huỳnh, xã Cần Giuộc, tỉnh Tây Ninh (Long An cũ)']
    source_issues['rent_similar_commercial_005']=['mô tả ghép hai phương án 1.500 m²/150 triệu và 730 m²/130 triệu; nguồn lại ghép 1.500 m² với 130 triệu/tháng']
    source_issues['rent_similar_room_003']=['cột địa bàn nguồn là Quận 4 nhưng mô tả ghi Vị trí: Quận 7 và quảng cáo nhiều phòng tại Quận 4/7/8; chưa xác định đúng phòng nguồn']
    source_issues['sale_similar_commercial_003']=['cột địa bàn nguồn là Bình Chánh nhưng mô tả ghi HQC Plaza, Phường Bình Đông, Quận 8; chưa có ánh xạ địa bàn xác nhận trong phiếu']
    source_issues['sale_similar_apartment_003']=['giá nguồn trong cột là 17 tỷ nhưng tiêu đề ghi 19,5 tỷ; mô tả không xác nhận mức giá cuối']
    return queries,catalog,feats,originals,manifest,source_issues

def hard_issues(q,r,c,task):
    req=q['request']; src=q['source']; errors=[];unknown=[]
    for col,expected in [('transaction_type',q['market']),('property_category',q['category']),
                         ('price_period',req.get('price_period',src['price_period'])),
                         ('province_id',req.get('province_id',src['province_id']))]:
        if c.get(col)!=expected:errors.append(f'{col}={c.get(col)} khác bắt buộc {expected}')
    if q['method']=='similar':
        if c['listing_key']==src['listing_key'] or c['duplicate_group']==task['source_duplicate_group']:
            errors.append('cùng tin/nhóm trùng với tin nguồn')
    else:
        if req.get('district_id') and c['district_id']!=req['district_id']:
            errors.append('sai quận/huyện bắt buộc '+q['district'])
        for col,param,operator,vi in [('price_vnd','max_price','max','giá'),('area_m2','min_area','min','diện tích'),('bedrooms','min_bedrooms','min','phòng ngủ')]:
            if param not in req:continue
            v=number(r[col]);lim=req[param]
            if v is None:unknown.append(f'chưa rõ {vi} để kiểm tra {param}={fmt(lim)}')
            elif (operator=='max' and v>lim) or (operator=='min' and v<lim):errors.append(f'{vi} {fmt(v)} vi phạm {param}={fmt(lim)}')
        if 'min_bedrooms' in req:
            # Explicit current bedroom claims that disagree with the structured
            # count matter to this mandatory test; floor counts are never used.
            t=norm(offer_text(r))
            ns=[int(m.group(1)) for m in re.finditer(r'\b(\d{1,2})\s*(?:phong ngu|pn)\b',t)]
            if ns and number(r['bedrooms']) is not None and min(ns)<req['min_bedrooms']<=number(r['bedrooms']):
                unknown.append('phòng ngủ cột '+r['bedrooms']+' nhưng mô tả nêu '+','.join(map(str,sorted(set(ns))))+'; cần xác nhận số phòng hiện hữu')
    if not c['is_recommendable']:errors.append('snapshot đánh dấu is_recommendable=false')
    age=(task['_as_of']-c['last_seen_at']).total_seconds()/86400
    if age<0 or age>req.get('max_age_days',90):errors.append('last_seen_at nằm ngoài cửa sổ snapshot')
    return errors,unknown

def grade_pref(q,r,c,f):
    req=q['request'];wanted=PRIORITIES[req['query']]
    fs=f['features']; yes=[k for k in wanted if k in fs];no=[k for k in wanted if k in f['negatives'] and k not in fs];missing=[k for k in wanted if k not in yes and k not in no]
    price=number(r['price_vnd']);area=number(r['area_m2'])
    pb=range_band(price,req['target_price']);ab=range_band(area,req['target_area'])
    p_ratio=ratio(price,req['target_price']);a_ratio=ratio(area,req['target_area'])
    limitations=[]
    if no:limitations.append('mô tả không đáp ứng '+', '.join(LABELS[k] for k in no))
    if missing:limitations.append('chưa thấy bằng chứng '+', '.join(LABELS[k] for k in missing))
    if pb not in ['close']:limitations.append('giá lệch mức mong muốn '+fmt(req['target_price'])+' đồng')
    if ab not in ['close']:limitations.append('diện tích lệch quy mô mong muốn '+fmt(req['target_area'])+' m²')
    sc=scope(f,q['category']);incompatible=False
    if req['query'].startswith('văn phòng') and sc=='warehouse' and 'office' not in fs:
        incompatible=True;limitations.append('rao kho/xưởng, chưa có công năng văn phòng thay thế được')
    if req['query']=='kho đường xe tải' and 'warehouse' not in fs and sc=='office':
        incompatible=True;limitations.append('rao văn phòng, không có công năng kho')
    if sc=='shared':limitations.append('giá có thể theo chỗ ở ghép; công năng khác thuê nguyên phòng')
    if incompatible:grade=0
    elif pb=='extreme' and ab=='extreme':grade=1
    elif pb=='extreme' and ab in ['close','moderate'] and len(yes)==len(wanted) and price<req['target_price'] and not no:grade=2
    elif pb=='extreme' or ab=='extreme':grade=1
    elif no and not yes:grade=1
    elif sc=='shared':grade=1
    elif len(yes)==len(wanted) and pb=='close' and ab=='close' and not no:grade=3
    elif not yes and (pb in ['large'] or ab in ['large']):grade=1
    elif pb=='large' and ab=='large':grade=1
    elif yes or (pb in ['close','moderate'] and ab in ['close','moderate']):grade=2
    else:grade=1
    evidence='; '.join(f'{LABELS[k]} [{f["evidence"][k]}]' for k in yes)
    place=q['district'] or 'TP.HCM (request không giới hạn quận)'
    reason=f'Đáp ứng bộ lọc snapshot tại {place}; giá {fmt(price)} đồng'+('/tháng' if q['market']=='rent' else '')+f', {fmt(area)} m².'
    if evidence:reason+=' Ưu tiên có căn cứ: '+evidence+'.'
    if limitations:reason+=' Hạn chế: '+'; '.join(limitations)+'.'
    conclusion={0:'Không có giá trị đề xuất thực tế cho nhu cầu này.',1:'Nhiều đánh đổi, chỉ phù hợp yếu.',2:'Đáng cân nhắc nhưng chưa đáp ứng rất tốt toàn bộ ưu tiên.',3:'Rất phù hợp các ưu tiên chính và quy mô mong muốn.'}[grade]
    return str(grade),reason+' '+conclusion,{'price_band':pb,'area_band':ab,'yes':yes,'no':no,'missing':missing,'scope':sc}

SIM_PRIORITIES={
 'HOUSE':['front','car','business','villa','quiet','new','full_furniture'],
 'APARTMENT':['shophouse','studio','full_furniture','balcony','pool','security'],
 'ROOM':['shared','full_furniture','loft','balcony','window','ac','no_landlord','parking'],
 'LAND':['farm','industrial','residential_land','private_title','car','front'],
 'COMMERCIAL':['warehouse','office','business','front','truck','elevator','parking'],
}

def grade_similar(q,r,c,f):
    src=q['source'];sf=q['sf'];sfs=sf['features'];fs=f['features']
    wanted=[k for k in SIM_PRIORITIES[q['category']] if k in sfs]
    common=[k for k in wanted if k in fs]
    missing=[k for k in wanted if k not in fs]
    neg=[k for k in wanted if k in f['negatives'] and k not in fs]
    price=number(r['price_vnd']);area=number(r['area_m2'])
    pb=range_band(price,src['price_vnd']);ab=range_band(area,src['area_m2'])
    same_district=c['district_id']==src['district_id']
    same_ward=same_district and c['ward']==src['ward']
    project=clean(src.get('project_name'))
    same_project=bool(project and (norm(project)==norm(c.get('project_name')) or norm(project) in f['norm']))
    sc=scope(f,q['category']);ss=scope(sf,q['category'])
    major_scope=False;incompatible=False;limitations=[]
    if q['category']=='COMMERCIAL':
        if ss=='warehouse' and sc=='office' and 'warehouse' not in fs:incompatible=True
        if ss=='office' and sc=='warehouse' and 'office' not in fs:incompatible=True
        if ss=='warehouse' and 'warehouse' not in fs:major_scope=True
    if q['category']=='LAND' and ss in ['farm','industrial','residential'] and sc in ['farm','industrial','residential'] and ss!=sc:major_scope=True
    if q['category']=='ROOM' and ((ss=='shared')!=(sc=='shared')):major_scope=True
    if q['category']=='APARTMENT' and ((ss=='shophouse')!=(sc=='shophouse')):major_scope=True
    if q['category']=='HOUSE' and ((ss=='villa')!=(sc=='villa')):major_scope=True
    if q['category']=='APARTMENT' and ((ss=='studio')!=(sc=='studio')):
        # Conventional small apartments may substitute for studios; a huge
        # multi-bedroom unit does not without a substantial compromise.
        if pb in ['large','extreme'] or ab in ['large','extreme']:major_scope=True
    if major_scope or incompatible:limitations.append('công năng khác tin nguồn ('+ss+' → '+sc+')')
    sb=number(src.get('bedrooms'));cb=number(r.get('bedrooms'))
    source_bedroom_explicit=bool(re.search(r'\b\d+\s*(?:phong ngu|pn)\b',sf['norm']))
    bedroom_mismatch=bool(sb and cb and source_bedroom_explicit and abs(sb-cb)>=2)
    if bedroom_mismatch:limitations.append(f'{fmt(cb)} PN so với {fmt(sb)} PN nguồn')
    if not same_district:limitations.append('khác quận/huyện tham chiếu '+src['district']+' (ưu tiên mềm)')
    if pb!='close':limitations.append('giá khác mức nguồn '+fmt(src['price_vnd'])+' đồng')
    if ab!='close':limitations.append('quy mô khác nguồn '+fmt(src['area_m2'])+' m²')
    if neg:limitations.append('khác rõ '+', '.join(LABELS[k] for k in neg))
    if missing:limitations.append('chưa xác nhận tương đồng '+', '.join(LABELS[k] for k in missing))
    both_extreme=pb=='extreme' and ab=='extreme'
    if incompatible:grade=0
    elif both_extreme and major_scope:grade=0
    elif both_extreme:grade=1
    elif major_scope and (pb in ['large','extreme'] or ab in ['large','extreme']):grade=0 if not common else 1
    elif pb=='extreme' or ab=='extreme' or major_scope:grade=1
    elif pb=='large' and ab=='large':grade=1
    elif len(neg)>=2:grade=1
    elif pb=='close' and ab=='close' and same_district and not bedroom_mismatch and not neg and (not wanted or (len(common)>=min(2,len(wanted)) and len(missing)<=1)):
        grade=3
    elif pb in ['close','moderate'] and ab in ['close','moderate']:
        grade=2
    elif common or same_project:grade=2 if not bedroom_mismatch and not neg else 1
    elif same_district and (pb=='close' or ab=='close'):grade=2
    else:grade=1
    # Similarity cannot be "very good" solely from matching numeric fields.
    if grade==3 and not common and wanted:grade=2
    reason=f'Cùng thị trường, loại BĐS, đơn vị giá và TP.HCM theo snapshot; {r["district"]}, giá {fmt(price)} đồng'+('/tháng' if q['market']=='rent' else '')+f', {fmt(area)} m².'
    if sb is not None and cb is not None and q['category'] in ['HOUSE','APARTMENT']:
        reason+=f' Cấu trúc snapshot: {fmt(cb)} PN, nguồn {fmt(sb)} PN (tham chiếu mềm).'
    if same_project:reason+=' Cùng dự án '+project+' theo snapshot.'
    if common:
        reason+=' Tương đồng có căn cứ: '+'; '.join(f'{LABELS[k]} [{f["evidence"][k]}]' for k in common[:3])+'.'
    if limitations:reason+=' Hạn chế: '+'; '.join(limitations)+'.'
    reason+=' '+{0:'Chênh công năng/phân khúc đến mức không có giá trị thay thế thực tế.',1:'Còn liên quan nhưng nhiều đánh đổi lớn.',2:'Là phương án tương tự đáng cân nhắc, còn đánh đổi đáng kể.',3:'Rất tương tự trên các ưu tiên chính, chưa thấy đánh đổi lớn.'}[grade]
    return str(grade),reason,{'price_band':pb,'area_band':ab,'same_district':same_district,'common':common,'missing':missing,'negative':neg,'source_scope':ss,'scope':sc}

def run(commit=False):
    from datetime import datetime
    queries,catalog,feats,originals,manifest,source_issues=load()
    tasks={}
    for split in ['tune','test']:
        for line in (DATA/f'queries_{split}.jsonl').read_text().splitlines():
            t=json.loads(line);t['_as_of']=datetime.fromisoformat(t['as_of']);tasks[t['query_id']]=t
    outputs={};audit=[]
    for split,(fields,rows) in originals.items():
        done=[]
        for row in rows:
            q=queries[row['query_id']];c=catalog[row['listing_key']]
            f=extract(row)
            errors,unknown=hard_issues(q,row,c,tasks[q['query_id']])
            cissues=conflicts(row,f,q['market'])
            issues=source_issues.get(q['query_id'],[])
            if issues:
                grade='';reason='MAU_THUAN_DU_LIEU: Tin nguồn chưa đủ nhất quán để chấm similar: '+'; '.join(issues)+'. Cần xác nhận snapshot, không tự chọn giá/địa chỉ thuận tiện.';meta={}
            elif cissues:
                prefix='CHO_RA_SOAT: ' if all('chưa xác định đất hay sàn' in issue for issue in cissues) else 'MAU_THUAN_DU_LIEU: '
                grade='';reason=prefix+'; '.join(cissues)+'. Cần xác nhận dữ liệu của chính tin đang rao trước khi chấm.';meta={}
            elif unknown:
                grade='';reason='CHO_RA_SOAT: '+'; '.join(unknown)+'. Chưa kết luận điều kiện bắt buộc.';meta={}
            elif errors:
                grade='0';reason='Vi phạm điều kiện bắt buộc: '+'; '.join(errors)+'.';meta={}
            elif q['method']=='preferences':
                grade,reason,meta=grade_pref(q,row,c,f)
            else:
                grade,reason,meta=grade_similar(q,row,c,f)
            updated={**row,'grade':grade,'annotator_id':AUTHOR,'reason':reason}
            done.append(updated)
            audit.append({'split':split,'query_id':row['query_id'],'listing_key':row['listing_key'],'grade':grade,'reason':reason,
                          'candidate_title':row['title'],'source_title':q['source_title'],'method':q['method'],'category':q['category'],
                          'checks':meta,'hard_errors':errors,'unknown':unknown,'candidate_issues':cissues,'source_issues':issues})
        outputs[split]=(fields,done)
    WORK.mkdir(exist_ok=True)
    for split,(fields,rows) in outputs.items():write_csv(WORK/f'annotation_{split}.csv',fields,rows)
    (WORK/'audit.jsonl').write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in audit))
    stats={s:{'pairs':len(rows),'queries':len({r['query_id'] for r in rows}),
              'grade_distribution':dict(collections.Counter(r['grade'] or 'review' for r in rows))} for s,(_,rows) in outputs.items()}
    print(json.dumps({'stats':stats,'source_issues':{k:v for k,v in source_issues.items() if v}},ensure_ascii=False,indent=2))
    if not commit:return
    assert all(not any(row[k] for k in EDITABLE) for _,rows in originals.values() for row in rows), 'Refusing to overwrite existing annotations'
    OUT.mkdir(exist_ok=False)
    backup=OUT/'originals';backup.mkdir()
    for split in ['tune','test']:
        src=DATA/f'annotation_{split}.csv'
        shutil.copy2(src,backup/src.name)
        shutil.copy2(WORK/src.name,src)
    # Verify every original non-annotation cell, field order, pair and row order.
    verification={}
    for split,(fields,original_rows) in originals.items():
        new_fields,new_rows=read_csv(DATA/f'annotation_{split}.csv')
        assert fields==new_fields and len(original_rows)==len(new_rows)
        for a,b in zip(original_rows,new_rows):
            assert all(a[k]==b[k] for k in fields if k not in EDITABLE)
            assert b['annotator_id']==AUTHOR and b['reason'].strip()
            assert b['grade'] in ['', '0','1','2','3']
            assert b['grade'] or b['reason'].startswith(('CHO_RA_SOAT','MAU_THUAN_DU_LIEU','NGHI_TRUNG'))
        actual={(r['query_id'],r['listing_key']) for r in new_rows}
        expected={(t['query_id'],k) for t in tasks.values() if t['split']==split for k in t['relevance']}
        assert actual==expected and len(actual)==len(new_rows)
        verification[split]={'all_non_annotation_cells_unchanged':True,'row_column_pair_order_preserved':True,
                             'pairs_match_original_jsonl':True,'utf8_bom_preserved':(DATA/f'annotation_{split}.csv').read_bytes().startswith(b'\xef\xbb\xbf'),
                             'original_sha256':digest(backup/f'annotation_{split}.csv'),'annotated_sha256':digest(DATA/f'annotation_{split}.csv')}
    summary={'annotator_id':AUTHOR,'annotator_type':'AI-assisted rule-based draft with targeted qualitative review',
             'human_verified':False,'dataset_version':manifest['dataset_version'],'as_of':manifest['as_of'],
             'labels_status':'provisional_ai_with_unresolved_review_items', 'stats':stats,'verification':verification,
             'model_scores_used':False,'pool_audit_opened':False,'ranking_metrics_run':False,'weights_changed':False}
    summary['source_files_sha256']={name:digest(DATA/name) for name in ['queries.csv','queries_tune.jsonl','queries_test.jsonl','manifest.json']}
    summary['annotation_guideline_sha256']=digest(ROOT/'docs/HUONG_DAN_CHAM_NHAN_GOI_Y_BAT_DONG_SAN.md')
    summary['graded_pairs']=sum(bool(r['grade']) for r in audit)
    summary['pending_pairs']=sum(not r['grade'] for r in audit)
    summary['ready_for_full_import']=summary['pending_pairs']==0
    summary['final_ground_truth']=False
    (OUT/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    review=[r for r in audit if not r['grade']]
    write_csv(OUT/'review_required.csv',['split','query_id','listing_key','candidate_title','source_title','reason'],
              [{k:r[k] for k in ['split','query_id','listing_key','candidate_title','source_title','reason']} for r in review])
    shutil.copy2(WORK/'audit.jsonl',OUT/'decisions.jsonl')
    shutil.copy2(Path(__file__),OUT/'annotation_procedure.py')
    shutil.copy2(WORK/'protocol.md',OUT/'README.md')
    query_summary=[]
    for qid,q in queries.items():
        group=[r for r in audit if r['query_id']==qid]
        counts=collections.Counter(r['grade'] or 'review' for r in group)
        query_summary.append({'query_id':qid,'split':q['split'],'market':q['market'],'method':q['method'],'category':q['category'],
                              'pairs':len(group),**{f'grade_{g}':counts[g] for g in ['0','1','2','3']},'review_required':counts['review']})
    write_csv(OUT/'query_summary.csv',list(query_summary[0]),query_summary)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--commit',action='store_true');args=p.parse_args();run(args.commit)

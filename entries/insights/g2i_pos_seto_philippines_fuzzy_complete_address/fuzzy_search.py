import pandas as pd
import unidecode
import re
from rapidfuzz import process, fuzz

number_to_roman = {
    '1': 'i', '2': 'ii', '3': 'iii', '4': 'iv', '5': 'v',
    '6': 'vi', '7': 'vii', '8': 'viii', '9': 'ix', '10': 'x'
}

def replace_number_letter(match):
    num = match.group(1)
    letter = match.group(2)
    roman = number_to_roman.get(num)
    if roman:
        return f"{roman} {letter}"
    return match.group(0)

def normalize_text(text):
    if not isinstance(text, str):
        return ''
    text = unidecode.unidecode(text.strip().lower())
    text = text.replace('-', ' ')
    text = re.sub(r'\b([1-9]|10)\s*([a-z])\b', replace_number_letter, text)
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def normalize_for_match(text):
    # Giống normalize_text nhưng xoá phần trong ngoặc
    if not isinstance(text, str):
        return ''
    text = unidecode.unidecode(text.strip().lower())
    text = re.sub(r'\([^)]*\)', '', text)  # xoá phần trong ngoặc
    text = re.sub(r'\[[^\]]*\]', '', text)
    text = re.sub(r'\{[^}]*\}', '', text)
    text = text.replace('-', '')
    text = re.sub(r'\b([1-9]|10)\s*([a-z])\b', replace_number_letter, text)
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def fuzzy_match_province(province_input, province_list, threshold=85):
    input_norm = normalize_text(province_input)
    province_norms = [normalize_text(p) for p in province_list]
    match, score, _ = process.extractOne(input_norm, province_norms, scorer=fuzz.partial_ratio)
    if score >= threshold:
        return province_list[province_norms.index(match)]
    return None

def fuzzy_extract_location(address_detail, province_name_matched, admin_df, district_input=None, normalize_func=normalize_text):
    address_norm = normalize_func(address_detail)
    province_name_norm = normalize_text(province_name_matched)

    subset = admin_df[admin_df['province_name'].apply(normalize_text) == province_name_norm]

    # Nếu có district input → filter luôn
    if district_input:
        district_input_norm = normalize_text(district_input)
        district_names = subset['district_name'].dropna().unique()
        district_map = {normalize_text(d): d for d in district_names}
        match, score, _ = process.extractOne(district_input_norm, list(district_map.keys()), scorer=fuzz.partial_ratio)
        if score >= 85:
            district_matched = district_map[match]
            subset = subset[subset['district_name'] == district_matched]
        else:
            return None, "Không tìm được huyện"

    commune_names = subset['commune_name'].dropna().unique()
    commune_map = {normalize_func(c): c for c in commune_names}

    # === LỚP 1: Exact match với normalize_func ===
    sorted_communes = sorted(commune_map.items(), key=lambda x: -len(x[0]))
    for commune_norm, commune_original in sorted_communes:
        if commune_norm in address_norm:
            exact_row = subset[subset['commune_name'] == commune_original]
            if not exact_row.empty:
                row = exact_row.iloc[0]
                return {
                    "commune_id": row['commune_id'],
                    "district_id": row['district_id'],
                    "commune": row['commune_name'],
                    "district": row['district_name'],
                    "province": row['province_name'],
                }, "Đầy đủ (exact match)"

    # === LỚP 2: Fuzzy match ===
    matches = process.extract(address_norm, list(commune_map.keys()), scorer=fuzz.token_sort_ratio, limit=5)
    matches = [(m, s) for m, s, _ in matches if s >= 80]

    matched_rows = []
    for match_norm, _ in matches:
        commune_original = commune_map[match_norm]
        rows = subset[subset['commune_name'] == commune_original]
        matched_rows.extend(rows.to_dict(orient='records'))

    if len(matched_rows) == 1:
        r = matched_rows[0]
        return {
            "commune_id": r['commune_id'],
            "district_id": r['district_id'],
            "commune": r['commune_name'],
            "district": r['district_name'],
            "province": r['province_name'],
        }, "Đầy đủ (fuzzy)"
    elif len(matched_rows) > 1:
        return matched_rows, "Nhiều xã trùng"
    else:
        return None, "Không khớp"


def match_address(province_input, address_detail, admin_df, district_input=None):
    province_list = admin_df['province_name'].unique()
    province_matched = fuzzy_match_province(province_input, province_list)

    if not province_matched:
        return None, "Không tìm được tỉnh"

    # Lần 1: Dùng normalize_text
    result, status = fuzzy_extract_location(address_detail, province_matched, admin_df, district_input, normalize_func=normalize_text)
    if status != "Không khớp":
        match_type = "OK_EXACT" if status == "Đầy đủ (exact match)" else (
            "OK_FUZZY" if status == "Đầy đủ (fuzzy)" else "MULTIPLE"
        )
        return result if isinstance(result, dict) else {
            "province": province_matched,
            "matches": result,
            "status": status
        }, match_type

    # Lần 2: Dùng normalize_for_match
    result, status = fuzzy_extract_location(address_detail, province_matched, admin_df, district_input, normalize_func=normalize_for_match)
    if status != "Không khớp":
        match_type = "OK_EXACT" if status == "Đầy đủ (exact match)" else (
            "OK_FUZZY" if status == "Đầy đủ (fuzzy)" else "MULTIPLE"
        )
        return result if isinstance(result, dict) else {
            "province": province_matched,
            "matches": result,
            "status": status
        }, match_type

    # Không ra kết quả
    return None, "NOT_FOUND"


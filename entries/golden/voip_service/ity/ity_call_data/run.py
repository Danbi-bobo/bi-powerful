import os
import sys
from dotenv import load_dotenv

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LAST_RUN_FILE_PATH = os.path.join(SCRIPT_DIR, "last_run.txt")

URL = "https://report.ity.vn/wsapi/ity/ws_cdr.php"

ITY_USERNAME = "C88004"
ITY_PASSWORD = os.getenv(f"ITY_PASSWORD_{ITY_USERNAME}")

def ensure_directory_exists(path):
    """Kiểm tra và tạo thư mục nếu không tồn tại"""
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def read_last_run():
    """Đọc thời gian lần chạy trước từ file, trả về None nếu lỗi"""
    if not os.path.exists(LAST_RUN_FILE_PATH):
        return None

    try:
        with open(LAST_RUN_FILE_PATH, "r", encoding="utf-8") as f:
            return datetime.strptime(f.read().strip(), "%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logging.warning(f"Lỗi đọc last_run.txt: {e}")
        return None

def get_time_range():
    """Trả về (start_time, end_time), trong đó start = last_run - 1h hoặc now - 1h nếu không có last_run"""
    now = datetime.now()

    last_run = read_last_run()
    start_time = (last_run - timedelta(hours=1)) if last_run else (now - timedelta(hours=1))

    return start_time, now

def fetch_ity_data(start: str, end: str):
    """Lấy dữ liệu ITY từ API"""
    params = {
        "action": "query_cdr",
        "fromdate": start,
        "todate": end,
        "limit": "1000"
    }
    
    try:
        response = requests.get(URL, params=params, auth=HTTPBasicAuth(ITY_USERNAME, ITY_PASSWORD))
        response.raise_for_status()
        return response.json().get('cdr', [])
    except requests.exceptions.RequestException as e:
        logging.error(f"Lỗi khi lấy dữ liệu ITY: {e}")
        return []

def transform_ity_data(data):
    """Chuyển đổi dữ liệu ITY thành DataFrame và xử lý các cột không cần thiết"""
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df.drop(columns=['clid', 'accountcode', 'cnam', 'cnum'], inplace=True, errors='ignore')
    df.rename(columns={'uniqueid': 'call_id'}, inplace=True)
    return df

def update_last_run_time(end_time: datetime):
    """Cập nhật thời gian hiện tại vào tệp last_run.txt"""
    ensure_directory_exists(LAST_RUN_FILE_PATH)
    with open(LAST_RUN_FILE_PATH, "w", encoding="utf-8") as f:
        f.write(end_time.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info(f"Đã cập nhật thời gian kết thúc vào tệp {LAST_RUN_FILE_PATH}")

def prepare_golden_df(raw_df):
    raw_df = raw_df.copy()
    rename_dict = {
        'uniqueid': 'call_id',
        'calldate': 'time_start',
        'direction': 'call_direction',
        'dst': 'phone',
        'duration': 'call_duration',
        'billsec': 'talk_duration',
        'disposition': 'call_status',
        'recordingfile': 'recording'
    }
    raw_df.rename(columns = rename_dict, inplace = True)
    raw_df.replace(np.nan, None, inplace = True)
    required_columns = ['call_id', 'time_start', 'call_direction', 'phone', 'call_duration', 'talk_duration', 'call_status', 'recording', 'src']
    columns_to_keep = list(set(raw_df.columns).intersection(required_columns))
    raw_df = raw_df[columns_to_keep]
    
    return raw_df

def main():
    start_time, end_time = get_time_range()
    start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"Lấy dữ liệu ITY từ {start_str} đến {end_str}...")

    raw_data = fetch_ity_data(start_str, end_str)
    ity_df = transform_ity_data(raw_data)

    if ity_df.empty:
        logging.info("Không có dữ liệu ITY mới.")
        return ity_df

    logging.info(f"Đã lấy {len(ity_df)} bản ghi dữ liệu ITY mới.")
    ity_df = prepare_golden_df(ity_df)
    update_last_run_time(end_time)
    
    return ity_df


if __name__ == "__main__":
    setup_logger(__file__)
    result_df = main()
    if not result_df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database='alomix_skyward_data'
            , table='ity_call_data'
            , df=result_df,
              unique_columns=["call_id"]
        )
result_df
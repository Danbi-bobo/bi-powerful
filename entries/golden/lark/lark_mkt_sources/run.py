import sys
import os
import pymysql
from dotenv import load_dotenv

load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.domain.utils.log_helper import setup_logger
setup_logger(__file__)

# --- Config ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = "insight_data"
TABLE_NAME = "marketing_sources"

base_id = "Gy1YbgLGGa7TLVsw54mlvLTAgIe"
table_id = "tblp7CXMckwFgOPL"

mapping_dict = {
    "shop_id": {"path": "shop_id", "type": "str"},
    "id": {"path": "id", "type": "str"},
    "name": {"path": "name", "type": "str"},
    "account_id": {"path": "account_id", "type": "str"},
    "user_id": {"path": "marketer.[0].id", "type": "str"},
    "marketer": {"path": "marketer.[0].name", "type": "str"},
    "from_at": {"path": "from_at", "type": "ms_timestamp"},
    "until_at": {"path": "until_at", "type": "ms_timestamp"},
}

def shrink_record(record, max_len=255):
    """Cắt bớt string quá dài để tránh crash"""
    out = {}
    for k, v in record.items():
        if v is None:
            out[k] = None
        else:
            s = str(v)
            if len(s) > max_len:
                s = s[:max_len]
            out[k] = s
    return out

def insert_records(records, chunk_size=50):
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    with conn:
        with conn.cursor() as cur:
            for i in range(0, len(records), chunk_size):
                chunk = [shrink_record(r) for r in records[i:i+chunk_size]]
                values = [
                    (r.get("shop_id"), r.get("id"), r.get("name"), r.get("account_id"), r.get("user_id"), r.get("marketer"), r.get("from_at"), r.get("until_at"), r.get("record_id"))
                    for r in chunk
                ]
                sql = f"""
                INSERT INTO {TABLE_NAME} (shop_id, id, name, account_id, user_id, marketer, from_at, until_at, record_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    shop_id=VALUES(shop_id),
                    id=VALUES(id),
                    name=VALUES(name),
                    account_id=VALUES(account_id),
                    user_id=VALUES(user_id),
                    marketer=VALUES(marketer),
                    from_at=VALUES(from_at),
                    until_at=VALUES(until_at)
                """
                cur.executemany(sql, values)
                conn.commit()
                print(f"Inserted {i+1}–{i+len(chunk)} / {len(records)}")

def main():
    lark = LarkApiHandle()
    df = lark.extract_table_to_df(base_id=base_id, table_id=table_id, mapping_dict=mapping_dict)
    if not df.empty:
        records = df.to_dict(orient="records")
        print(f"📦 Got {len(records)} records from Lark")
        insert_records(records, chunk_size=50)

if __name__ == "__main__":
    main()

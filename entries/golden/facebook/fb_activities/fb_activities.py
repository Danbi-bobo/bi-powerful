import sys
import os
import json
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load env
load_dotenv()

CDP_PATH = os.getenv("CDP_PATH", "")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.facebook.fb_api_handler import FacebookAPIHandler
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler

# Minimal logging
logging.basicConfig(level=logging.ERROR)

# Config
TARGET_DATABASE = os.getenv("DB_GOLDEN_NAME")
TARGET_TABLE = "fb_activities"

def get_all_accounts():
    """Lấy tất cả ad accounts"""
    query = """
        SELECT id, name, token, market
        FROM ad_account
        WHERE account_status IN (1, 3)
        AND token IS NOT NULL AND token != ''
        ORDER BY market, name
    """
    try:
        mdb = MariaDBHandler()
        accounts = mdb.read_from_db(
            database=TARGET_DATABASE,
            query=query,
            output_type='list_of_dicts'
        )
        if accounts:
            print(f"✅ Found {len(accounts)} accounts")
            return accounts
        else:
            print("📭 No accounts found")
            return []
    except Exception as e:
        print(f"Error getting accounts: {e}")
        return []

def extract_activities_for_account(account):
    """Lấy activities của 1 account"""
    account_id = account['id']
    token = account['token']
    try:
        fb_handler = FacebookAPIHandler(
            access_token=token,
            timeout=180,
            max_retries=2
        )
        endpoint = f"{account_id}/activities"
        params = {
            'fields': 'event_type,event_time,extra_data',
            'limit': 1000
        }
        activities = fb_handler.get_all(endpoint=endpoint, params=params)
        # Gắn thêm account_id để tạo id duy nhất
        for act in activities:
            act["account_id"] = account_id
        return activities
    except Exception as e:
        print(f"{account_id}: {str(e)[:50]}...")
        return []
    finally:
        if 'fb_handler' in locals():
            fb_handler.close()

def save_activities_batch(activities_list):
    """Lưu activities vào DB"""
    if not activities_list:
        return 0
    try:
        mdb = MariaDBHandler()
        batch_records = []
        for act in activities_list:
            try:
                extra_data_raw = act.get("extra_data")
                new_amount = None
                new_currency = None
                if extra_data_raw:
                    try:
                        extra = json.loads(extra_data_raw)
                        new_val = extra.get("new_value", {})
                        new_amount = new_val.get("new_value")
                        new_currency = new_val.get("currency")
                    except json.JSONDecodeError:
                        pass
                # Tạo id duy nhất: accountid_eventtime_eventtype
                record_id = f"{act['account_id']}_{act.get('event_time')}_{act.get('event_type','')}"
                # Convert event_time sang datetime
                try:
                    evt_time = datetime.fromisoformat(act.get("event_time").replace("Z", "+00:00"))
                except Exception:
                    evt_time = None
                clean_record = {
                    "id": record_id,
                    "event_time": evt_time,
                    "value": new_amount,
                    "currency": new_currency,
                    "event_type": act.get("event_type", "")
                }
                batch_records.append(clean_record)
            except Exception:
                continue
        if batch_records:
            mdb.insert_and_update_from_dict(
                database=TARGET_DATABASE,
                table=TARGET_TABLE,
                data=batch_records,
                unique_columns=["id"]
            )
            return len(batch_records)
        return 0
    except Exception as e:
        print(f"Save error: {str(e)[:50]}...")
        return 0

def main():
    print(f"Database: {TARGET_DATABASE}.{TARGET_TABLE}")
    accounts = get_all_accounts()
    if not accounts:
        return
    total_acts = 0
    total_saved = 0
    for i, acc in enumerate(accounts, 1):
        print(f"[{i}/{len(accounts)}] {acc['id']}...", end=" ")
        acts = extract_activities_for_account(acc)
        if acts:
            total_acts += len(acts)
            saved = save_activities_batch(acts)
            total_saved += saved
            print(f"✅ {len(acts)} activities → {saved} saved")
        else:
            print("📭 No activities")
        if i < len(accounts):
            time.sleep(3)
    print(f"Done. Total: {total_acts} activities, {total_saved} saved.")

if __name__ == "__main__":
    main()

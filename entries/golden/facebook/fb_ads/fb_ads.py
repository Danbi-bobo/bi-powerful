import sys
import os
from dotenv import load_dotenv

load_dotenv()
CDP_PATH = os.getenv("CDP_PATH", "")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.facebook.fb_api_handler import FacebookAPIHandler
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from datetime import datetime
import time
import logging
from urllib.parse import urlparse, parse_qs

# Minimal logging - chỉ ERROR
logging.basicConfig(level=logging.ERROR)

# Configuration
TARGET_DATABASE = os.getenv("DB_GOLDEN_NAME")
TARGET_TABLE = "fb_ad_ads_v2"
START_DATE = "2024-01-01"


def get_all_accounts():
    """Lấy tất cả ad accounts"""
    query = """
        SELECT campaign_id, access_token, market
        FROM fb_ad_campaigns
        WHERE access_token IS NOT NULL AND access_token != ''
    """

    try:
        mdb = MariaDBHandler()
        accounts = mdb.read_from_db(
            database=TARGET_DATABASE, query=query, output_type="list_of_dicts"
        )

        if accounts:
            # Group by market
            markets = {}
            for acc in accounts:
                market = acc["market"]
                markets[market] = markets.get(market, 0) + 1

            print(f"✅ Found {len(accounts)} accounts:")
            for market, count in markets.items():
                print(f"   {market}: {count}")

            return accounts
        else:
            print("No accounts found")
            return []

    except Exception as e:
        print(f"Error getting accounts: {e}")
        return []


def extract_campaigns_for_account(account):
    campaign_id = account["campaign_id"]
    token = account["access_token"]
    market = account["market"]
    name = account.get("name", "")

    try:
        fb_handler = FacebookAPIHandler(access_token=token, timeout=180, max_retries=2)

        adset_endpoint = f"{campaign_id}/ads"
        params = {
            "fields": "id,ad_active_time,created_time,name,last_updated_by_app_id,status,updated_time,creative{actor_id,call_to_action_type,object_story_spec},campaign_id,adset_id,account_id",
            "limit": 1000,
            "filtering": f"""[{{"field": "ad.updated_time", "operator": "GREATER_THAN", "value": "{START_DATE}"}}]""",
        }

        campaigns = fb_handler.get_all(endpoint=adset_endpoint, params=params)

        if campaigns:
            # Add metadata
            for campaign in campaigns:
                campaign["market"] = market
                campaign["account_name"] = name
                campaign["access_token"] = token

            return campaigns
        else:
            return []

    except Exception as e:
        print(f"{campaign_id}: {str(e)[:50]}...")
        return []
    finally:
        if "fb_handler" in locals():
            fb_handler.close()


def save_campaigns_batch(ad_list):
    if not ad_list:
        return 0

    try:
        mdb = MariaDBHandler()
        batch_records = []

        for ad in ad_list:
            try:
                creative = ad.get("creative", {}) or {}
                object_story_spec = creative.get("object_story_spec", {}) or {}
                video_data = object_story_spec.get("video_data", {}) or {}
                cta = video_data.get("call_to_action", {}) or {}
                cta_value = cta.get("value", {}) or {}
                link = cta_value.get("link", "")

                # Parse utm_source
                utm_source = ""
                if link:
                    parsed_url = urlparse(link)
                    query_params = parse_qs(parsed_url.query)
                    utm_source = query_params.get("utm_source", [""])[0]

                clean_record = {
                    "campaign_id": str(ad.get("campaign_id", "")) or None,
                    "adset_id": str(ad.get("adset_id", "")) or None,
                    "account_id": str(ad.get("account_id", "")) or None,
                    "ad_id": str(ad.get("id", "")) or None,
                    "actor_id": str(creative.get("actor_id", "")) or None,
                    "ad_name": str(ad.get("name", "")) or None,
                    "ad_active_time": (
                        int(ad["ad_active_time"]) if ad.get("ad_active_time") is not None else None
                    ),
                    "status": str(ad.get("status", "")) or None,
                    "created_time": _normalize_datetime(ad.get("created_time")),
                    "updated_time": _normalize_datetime(ad.get("updated_time")),
                    "last_updated_by_app_id": str(ad.get("last_updated_by_app_id", "")) or None,
                    "call_to_action_type": str(creative.get("call_to_action_type", "")) or None,
                    "utm_source": utm_source or None,
                    "market": str(ad.get("market", ""))[:100] or None,
                }

                batch_records.append(clean_record)

            except Exception as inner_e:
                print(f"Record error: {inner_e}")
                continue

        if batch_records:
            try:
                mdb.insert_and_update_from_dict(
                    database=TARGET_DATABASE,
                    table=TARGET_TABLE,
                    data=batch_records,
                    unique_columns=["ad_id"],
                )
                return len(batch_records)
            except Exception as insert_e:
                print("Insert error:", insert_e)
                print("Sample record:", batch_records[0])
                return 0

        return 0

    except Exception as e:
        print(f"Save error: {str(e)[:50]}...")
        return 0


def _normalize_datetime(dt_str):
    """Chuyển định dạng datetime FB sang MySQL"""
    if not dt_str:
        return None
    try:
        # Ví dụ: 2023-10-13T15:49:37+0700
        dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S%z")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def main():
    print(f"Database: {TARGET_DATABASE}.{TARGET_TABLE}")
    print(f"Date: {START_DATE} → now")

    start_time = datetime.now()

    accounts = get_all_accounts()
    if not accounts:
        return

    print(f"\nProcessing {len(accounts)} accounts...")

    total_campaigns = 0
    total_saved = 0

    for i, account in enumerate(accounts, 1):
        account_id = account["campaign_id"]
        market = account["market"]

        print(f"[{i:2d}/{len(accounts)}] {account_id} ({market})...", end=" ")

        campaigns = extract_campaigns_for_account(account)

        if campaigns:
            total_campaigns += len(campaigns)
            saved = save_campaigns_batch(campaigns)
            total_saved += saved
            print(f"✅ {len(campaigns)} campaigns → {saved} saved")
        else:
            print("📭 No campaigns")

        if i < len(accounts):
            time.sleep(3)

    duration = datetime.now() - start_time

    if total_campaigns > 0:
        success_rate = total_saved / total_campaigns * 100
        print(f"   Success rate: {success_rate:.1f}% ({total_saved}/{total_campaigns})")
        print(f"   Duration: {duration}")


if __name__ == "__main__":
    main()
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

# Minimal logging - chỉ ERROR
logging.basicConfig(level=logging.ERROR)

# Configuration
TARGET_DATABASE = os.getenv("DB_GOLDEN_NAME")
TARGET_TABLE = "fb_ad_adsets"
START_DATE = "2024-01-01"


def get_all_accounts():
    """Lấy tất cả ad accounts"""
    query = """
        SELECT id, name, token, market
        FROM ad_account
        WHERE token IS NOT NULL AND token != ''
        ORDER BY market, name
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
    account_id = account["id"]
    token = account["token"]
    market = account["market"]
    
    # ✅ Extract clean account ID từ account_id (bỏ "act_" prefix nếu có)
    clean_account_id = account_id.replace('act_', '') if account_id.startswith('act_') else account_id

    try:
        fb_handler = FacebookAPIHandler(access_token=token, timeout=180, max_retries=2)

        adset_endpoint = f"{account_id}/adsets"
        params = {
            "fields": "id,budget_remaining,status,created_time,daily_budget,name,optimization_goal,start_time,updated_time,campaign_id,account_id",
            "limit": 1000,
            "filtering": f"""[{{"field": "adset.updated_time", "operator": "GREATER_THAN", "value": "{START_DATE}"}}]""",
        }

        adsets = fb_handler.get_all(endpoint=adset_endpoint, params=params)

        if adsets:
            # Add metadata
            for adset in adsets:
                adset["market"] = market
                adset["account_name"] = account["name"]
                adset["access_token"] = token
                adset["clean_account_id"] = clean_account_id  # ✅ Pass clean account ID

            return adsets
        else:
            return []

    except Exception as e:
        print(f"{account_id}: {str(e)[:50]}...")
        return []
    finally:
        if "fb_handler" in locals():
            fb_handler.close()


def save_campaigns_batch(adset_list):
    if not adset_list:
        return 0

    try:
        mdb = MariaDBHandler()
        batch_records = []

        for adset in adset_list:
            try:
                # ✅ Lấy account_id từ API response, fallback về clean_account_id từ URL
                api_account_id = adset.get('account_id')
                clean_account_id = adset.get('clean_account_id')  # From URL without "act_"
                
                # Priority: API response -> Clean URL account ID
                final_account_id = None
                if api_account_id:
                    # API trả về account_id, bỏ "act_" nếu có
                    final_account_id = str(api_account_id).replace('act_', '') if str(api_account_id).startswith('act_') else str(api_account_id)
                elif clean_account_id:
                    # Fallback to clean account ID from URL
                    final_account_id = str(clean_account_id)

                clean_record = {
                    "campaign_id": str(adset.get("campaign_id", "")) if adset.get("campaign_id") else None,
                    "account_id": final_account_id,  # ✅ Sử dụng account ID đã xử lý
                    "adset_id": str(adset.get("id", "")),
                    "adset_name": str(adset.get("name", "")) if adset.get("name") else None,
                    "budget_remaining": (
                        int(adset["budget_remaining"])
                        if adset.get("budget_remaining")
                        else None
                    ),
                    "status": str(adset.get("status", "")) if adset.get("status") else None,
                    "created_time": adset.get("created_time"),
                    "updated_time": adset.get("updated_time"),
                    "daily_budget": (
                        int(adset["daily_budget"])
                        if adset.get("daily_budget")
                        else None
                    ),
                    "optimization_goal": str(adset.get("optimization_goal", "")) if adset.get("optimization_goal") else None,
                    "start_time": str(adset.get("start_time", ""))[:100] if adset.get("start_time") else None,
                    "market": str(adset.get("market", ""))[:100] if adset.get("market") else None,
                }

                # ✅ Chỉ loại bỏ các field bắt buộc nếu rỗng, giữ lại NULL values cho optional fields
                if clean_record['adset_id']:  # Chỉ cần adset_id không rỗng
                    # Convert empty strings to None for optional fields
                    for key, value in clean_record.items():
                        if value == '' and key != 'adset_id':  # adset_id không được phép rỗng
                            clean_record[key] = None
                    
                    batch_records.append(clean_record)
                    
                    # ✅ Debug log để xem account_id được xử lý như thế nào
                    if not api_account_id and clean_account_id:
                        print(f"  🔄 Fallback account_id: {clean_account_id} for adset {clean_record['adset_id']}")

            except Exception as e:
                print(f"Error processing adset: {e}")
                continue

        # Save all at once
        if batch_records:
            mdb.insert_and_update_from_dict(
                database=TARGET_DATABASE,
                table=TARGET_TABLE,
                data=batch_records,
                unique_columns=["adset_id"],
            )
            return len(batch_records)

        return 0

    except Exception as e:
        print(f"Save error: {str(e)[:50]}...")
        return 0


def main():
    """Main function"""
    print(f"Database: {TARGET_DATABASE}.{TARGET_TABLE}")
    print(f"Date: {START_DATE} → now")

    start_time = datetime.now()

    # Get accounts
    accounts = get_all_accounts()
    if not accounts:
        return

    # Process accounts
    print(f"\nProcessing {len(accounts)} accounts...")

    total_adsets = 0
    total_saved = 0
    success_accounts = 0

    for i, account in enumerate(accounts, 1):
        account_id = account["id"]
        market = account["market"]

        # Show progress
        print(f"[{i:2d}/{len(accounts)}] {account_id} ({market})...", end=" ")

        # Extract adsets
        adsets = extract_campaigns_for_account(account)

        if adsets:
            total_adsets += len(adsets)

            # Save adsets
            saved = save_campaigns_batch(adsets)
            total_saved += saved
            success_accounts += 1

            print(f"✅ {len(adsets)} adsets → {saved} saved")
        else:
            print("📭 No adsets")

        # Rate limiting
        if i < len(accounts):
            time.sleep(3)

    # Final summary
    duration = datetime.now() - start_time

    if total_adsets > 0:
        success_rate = total_saved / total_adsets * 100
        print(f"   Success rate: {success_rate:.1f}% - Duration: {duration}")


if __name__ == "__main__":
    main()
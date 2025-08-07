import datetime
import sys
import os
import json
import numpy as np
import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.pos.pos_api_handler import PosAPIHandler
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger

setup_logger(__file__)


# def load_config():
#     """Đọc file config.json"""
#     config_file = rf"{CDP_PATH}/entries/golden/pos/pos_orders/config.json"
#     with open(config_file, 'r') as file:
#         return json.load(file)
    
def load_config_shops():
    config_file = rf"{CDP_PATH}/entries/golden/pos/config_shops.json"
    with open(config_file, 'r') as file:
        return json.load(file)

# config = load_config()
config_shops = load_config_shops()

DB_GOLDEN_NAME = os.getenv("DB_GOLDEN_NAME")
GOLDEN_TABLE_NAME = "pos_orders"
CURRENT_TIME = 1743440400 + 5*60 #end
LAST_RUN_TIME = 0 - 5*60 #start
SHOPS = config_shops.get("shops", [])

db_columns = [
    "creator", "updated_at", "status", "fee_marketplace", 
    "order_sources_name", "order_sources","id", "note", "sub_status", "histories", "customer", "items_length", 
    "assigning_care_id", "money_to_collect", "inserted_at", "customer_pay_fee", "partner_fee", 
    "prepaid", "assigning_seller", "status_history", "cod", "bill_phone_number",
    "bill_full_name", "returned_reason_name", "total_quantity", "assigning_seller_id", 
    "total_discount", "time_assign_care", "partner", "p_utm_source", "time_assign_seller", 
    "shipping_address", "marketer", "transfer_money",
    "total_price_after_sub_discount", "pke_mkter", "time_send_partner", "total_price", 
    "account", "tags", "assigning_care", "items", "partner_account", 
    "partner_name", "first_delivery_at", "shop_id"
]

required_columns = {
    "id", "shop_id", "status", "partner_name", "extend_code", "first_delivery_at","items", "transfer_money",
    "total_price", "cod", "partner_fee", "order_sources_name", "total_discount", "bill_phone_number",
    "account", "time_assign_seller", "time_assign_care", "shipping_address", "bill_full_name", "order_sources",
    "assigning_seller_id", "assigning_care_id", "note", "tags", "reason", "products_name",
    "inserted_at", "updated_at", "province_id", "province_name", "utm_source", "product", "returned_reason", "returned_reason_detail"
    "sent_time", "returned_time", "receive_time", "tags_id", "money_to_collect", "partial_return_time", "total_price_after_sub_discount","fee_marketplace"
}

def update_last_run_time(end_time):
    """Cập nhật last_run trong config.json mà không ghi đè toàn bộ file."""
    config_file = rf"{CDP_PATH}/entries/golden/pos/pos_orders/config.json"

    new_value = end_time

    with open(config_file, 'r+') as file:
        config = json.load(file)
        config["last_run"] = new_value
        file.seek(0)
        json.dump(config, file, indent=4)
        file.truncate()
    logging.info(f"Update last_run inin config.json to '{new_value}'")

def extract_product_names(items):
    if isinstance(items, str):  # Trường hợp items là chuỗi JSON
        try:
            items = json.loads(items)
        except json.JSONDecodeError:
            return ""  # Nếu lỗi khi parse JSON, trả về chuỗi rỗng
    if not isinstance(items, list):  # Nếu không phải list, bỏ qua
        return ""
    
    return ", ".join([item["variation_info"]["name"] for item in items if isinstance(item, dict) and not item.get("is_bonus_product", False)])

def extract_ids(tag_str):
    try:
        tags = json.loads(tag_str) if isinstance(tag_str, str) else []
        tags_id = [str(tag["id"]) for tag in tags if isinstance(tag, dict) and "id" in tag]
        return ', '.join(tags_id)
    except json.JSONDecodeError:
        return ''

def convert_all_dicts_to_json(df):
    """Tự động tìm và chuyển tất cả các giá trị kiểu dict/list thành JSON string."""
    return df.apply(lambda col: col.map(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x))

def parse_json(json_str):
    """Chuyển JSON string thành dictionary, nếu lỗi thì trả về None."""
    if isinstance(json_str, str):
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return None
    return json_str

def get_updated_at(history_list, old_status=None, new_status=None):
    if isinstance(history_list, str):
        try:
            history_list = json.loads(history_list)
        except json.JSONDecodeError:
            return None

    if isinstance(history_list, list):
        for item in history_list:
            if isinstance(item, dict) and "status" in item and isinstance(item["status"], dict):
                old = item["status"].get("old")
                new = item["status"].get("new")
                if (old_status is None or old == old_status) and (new_status is None or new == new_status):
                    return item.get("updated_at")
    return None

def fetch_orders_for_shop(shop_id, api_key):
    page_number = 1

    with PosAPIHandler(shop_id, api_key) as pos_handler:
        orders = pos_handler.get_all("orders", params={"page_number": page_number, "page_size": 200, "startDateTime": LAST_RUN_TIME,"endDateTime":CURRENT_TIME, "updateStatus":"inserted_at"})
        if not orders:
            logging.warning("No orders retrieved from API.")
            return
        
        df = pd.DataFrame(orders)
        df = convert_all_dicts_to_json(df)
        df.replace(np.nan, None, inplace=True)

        df = df.loc[:, df.columns.intersection(db_columns)]


        numeric_cols = ["total_price","cod", "partner_fee","total_discount","money_to_collect", "transfer_money"]

        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        df["partner"] = df["partner"].apply(parse_json)
        df["shipping_address"] = df["shipping_address"].apply(parse_json)

        df["extend_code"] = df["partner"].apply(lambda x: x.get("extend_code") if isinstance(x, dict) else None)
        df["partner_name"] = df["partner"].apply(lambda x: x.get("partner_name") if isinstance(x, dict) else None)
        df["first_delivery_at"] = df["partner"].apply(lambda x: x.get("first_delivery_at") if isinstance(x, dict) else None)
        df["province_id"] = df["shipping_address"].apply(lambda x: x.get("province_id") if isinstance(x, dict) else None)
        df["province_name"] = df["shipping_address"].apply(lambda x: x.get("province_name") if isinstance(x, dict) else None)
        df["shipping_address"] = df["shipping_address"].apply(lambda x: x.get("full_address") if isinstance(x, dict) else None)
        
        # Trích xuất thời gian từ histories
        df["sent_time"] = df["histories"].apply(lambda x: get_updated_at(x, old_status = None, new_status=2))
        df["returned_time"] = df["histories"].apply(lambda x: get_updated_at(x, old_status=4, new_status=5))
        df["receive_time"] = df["histories"].apply(lambda x: get_updated_at(x, old_status=2, new_status=3))
        df["collect_money_time"] = df["histories"].apply(lambda x: get_updated_at(x, old_status=3, new_status=16))
        df["partial_return_time"] = df["histories"].apply(lambda x: get_updated_at(x, old_status=4, new_status=15))
        df["tags_id"] = df["tags"].apply(extract_ids)
        df["products_name"] = df["items"].apply(extract_product_names)

        # Xác định các cột đã transform
        transformed_cols = ["extend_code", "shipping_address", "province_id", 
                            "sent_time", "returned_time", "receive_time", "collect_money_time", "partial_return_time"]
        
        df["returned_reason"] = df["returned_reason_name"].apply(lambda x: x.split("/")[0] if isinstance(x, str) and "/" in x else x)
        df["returned_reason_detail"] = df["returned_reason_name"].apply(lambda x: x.split("/")[1] if isinstance(x, str) and "/" in x else None)

        # Giữ lại tất cả các cột không bị transform + cột đã transform
        original_cols = [col for col in df.columns if col not in transformed_cols]
        df_transformed = df[original_cols + transformed_cols]

        df_transformed = convert_all_dicts_to_json(df_transformed)
        df_transformed = df_transformed.loc[:, df_transformed.columns.intersection(required_columns)]
        rename_dict = {
            'id': 'order_id',
            'items': 'product',
            'p_utm_source': 'utm_source'
        }
        df_transformed.rename(columns=rename_dict, inplace=True, errors='ignore')
        df_transformed.replace(np.nan, None, inplace=True)

        # Cập nhật vào GOLDEN_DB
        if not df_transformed.empty:
            MariaDBHandler().insert_and_update_from_df(DB_GOLDEN_NAME, GOLDEN_TABLE_NAME, df_transformed, unique_columns=["shop_id","order_id"])

def fetch_orders_from_db():
    """Chạy lấy đơn hàng cho từng shop"""
    for shop in SHOPS:
        shop_id = shop.get("shop_id")
        api_key = shop.get("api_key")
        if shop_id and api_key:
            logging.info(f"Fetching orders for shop {shop_id}...")
            fetch_orders_for_shop(shop_id, api_key)

if __name__ == "__main__":
    fetch_orders_from_db()
    update_last_run_time(end_time=CURRENT_TIME)
    logging.info("Orders processing completed.")

import os
import json
from dotenv import load_dotenv

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")

def get_pos_shop_ids():
    config_file = rf"{CDP_PATH}/config/config.json"
    with open(config_file, "r", encoding="utf-8") as file:
        data = json.load(file)
    shop_ids = [shop["shop_id"] for shop in data["pos"]["shops"]]
    return shop_ids
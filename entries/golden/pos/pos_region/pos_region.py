import sys
import os
import json
import time
import logging
from dotenv import load_dotenv
from typing import List, Dict, Any

load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.http.http_client import HttpClient
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger

# Config
POS_BASE_URL = "https://pos.pages.fm/api/v1"
DB_NAME = os.getenv("DB_GOLDEN_NAME")
TABLE_NAME = "pos_geo_locations"

def load_config_shops() -> Dict[str, Any]:
    """Load config_shops.json từ CDP_PATH"""
    try:
        config_file = rf"{CDP_PATH}/entries/golden/pos/config_shops.json"
        with open(config_file, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as e:
        logging.error(f"Error loading config_shops.json: {e}")
        return {"shops": []}

def normalize_postcode(value):
    """Convert list [12345] => '12345', [123,456] => '123,456'"""
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    return value

class GeoExtractor:
    def __init__(self, country_code: str):
        self.country_code = str(country_code)
        self.http_client = HttpClient(timeout=30)
        self.db_handler = MariaDBHandler()

    def get_provinces(self) -> List[Dict[str, Any]]:
        try:
            logging.info(f"[{self.country_code}] Fetching provinces...")
            response = self.http_client.get(
                url=f"{POS_BASE_URL}/geo/provinces",
                params={"country_code": self.country_code, "all": "true"}
            )
            data = response.json()
            logging.debug(f"[{self.country_code}] Raw provinces response: {data}")
            provinces = data.get("data", []) or data.get("results", [])
            logging.info(f"[{self.country_code}] Found {len(provinces)} provinces")
            return provinces
        except Exception as e:
            logging.error(f"[{self.country_code}] Error fetching provinces: {e}")
            return []

    def get_districts(self, province_id: str) -> List[Dict[str, Any]]:
        try:
            response = self.http_client.get(
                url=f"{POS_BASE_URL}/geo/districts",
                params={"province_id": province_id}
            )
            data = response.json()
            logging.debug(f"[{self.country_code}] Districts for {province_id}: {data}")
            return data.get("data", []) or data.get("results", [])
        except Exception as e:
            logging.error(f"[{self.country_code}] Error fetching districts for province {province_id}: {e}")
            return []

    def get_communes(self, district_id: str) -> List[Dict[str, Any]]:
        try:
            response = self.http_client.get(
                url=f"{POS_BASE_URL}/geo/communes",
                params={"district_id": district_id}
            )
            data = response.json()
            logging.debug(f"[{self.country_code}] Communes for {district_id}: {data}")
            return data.get("data", []) or data.get("results", [])
        except Exception as e:
            logging.error(f"[{self.country_code}] Error fetching communes for district {district_id}: {e}")
            return []

    def build_complete_locations(self) -> List[Dict[str, Any]]:
        all_locations = []
        provinces = self.get_provinces()
        if not provinces:
            logging.error(f"[{self.country_code}] No provinces found, aborting")
            return []

        for province in provinces:
            province_id = province.get("id")
            province_name = province.get("name")
            logging.info(f"[{self.country_code}] Processing province: {province_name} ({province_id})")

            districts = self.get_districts(province_id)
            if not districts:
                logging.warning(f"[{self.country_code}] No districts found for province {province_name}")
                continue

            for district in districts:
                district_id = district.get("id")
                district_name = district.get("name")
                district_postcode = normalize_postcode(district.get("postcode"))

                communes = self.get_communes(district_id)
                if not communes:
                    logging.warning(f"[{self.country_code}] No communes found for district {district_name}")
                    continue

                for commune in communes:
                    commune_postcode = normalize_postcode(commune.get("postcode"))

                    location_record = {
                        "country_code": self.country_code,

                        # Province
                        "province_id": province.get("id"),
                        "province_name": province.get("name"),
                        "province_name_en": province.get("name_en"),
                        "province_new_id": province.get("new_id"),

                        # District
                        "district_id": district.get("id"),
                        "district_name": district.get("name"),
                        "district_name_en": district.get("name_en"),
                        "district_postcode": district_postcode,

                        # Commune
                        "commune_id": commune.get("id"),
                        "commune_name": commune.get("name"),
                        "commune_name_en": commune.get("name_en"),
                        "commune_new_id": commune.get("new_id"),
                        "commune_postcode": commune_postcode
                    }

                    cleaned = {k: v for k, v in location_record.items() if v is not None}
                    all_locations.append(cleaned)

                time.sleep(0.1)

            time.sleep(0.3)

        logging.info(f"[{self.country_code}] Total location records: {len(all_locations)}")
        return all_locations

    def save_locations_to_db(self, locations: List[Dict[str, Any]]) -> bool:
        if not locations:
            logging.warning(f"[{self.country_code}] No locations to save")
            return False

        try:
            batch_size = 1000
            total_saved = 0
            for i in range(0, len(locations), batch_size):
                batch = locations[i:i+batch_size]
                batch_num = i // batch_size + 1
                try:
                    self.db_handler.insert_and_update_from_dict(
                        database=DB_NAME,
                        table=TABLE_NAME,
                        data=batch,
                        unique_columns=["province_id", "district_id", "commune_id"]
                    )
                    total_saved += len(batch)
                    logging.info(f"[{self.country_code}] Batch {batch_num}: {len(batch)} saved (Total: {total_saved})")
                except Exception as e:
                    logging.error(f"[{self.country_code}] Error saving batch {batch_num}: {e}")
                    continue
            return total_saved > 0
        except Exception as e:
            logging.error(f"[{self.country_code}] Error saving to DB: {e}")
            return False

    def extract_and_save(self) -> bool:
        try:
            logging.info(f"[{self.country_code}] Starting extraction...")
            locations = self.build_complete_locations()
            if not locations:
                logging.error(f"[{self.country_code}] No location data extracted")
                return False
            success = self.save_locations_to_db(locations)
            if success:
                logging.info(f"[{self.country_code}] Extraction completed successfully")
                return True
            else:
                logging.error(f"[{self.country_code}] Failed to save data")
                return False
        except Exception as e:
            logging.error(f"[{self.country_code}] Error in extract_and_save: {e}")
            return False

def main():
    setup_logger(__file__)
    config = load_config_shops()
    shops = config.get("shops", [])
    if not shops:
        logging.error("No shops found in config_shops.json")
        sys.exit(1)

    for shop in shops:
        country_code = shop.get("country_code")
        if not country_code:
            logging.warning("Shop missing country_code, skip")
            continue
        extractor = GeoExtractor(country_code)
        success = extractor.extract_and_save()
        if success:
            logging.info(f"=== Extraction for {country_code} completed ===")
        else:
            logging.error(f"=== Extraction for {country_code} failed ===")

if __name__ == "__main__":
    main()

import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger
setup_logger(__file__)

base_id = 'Wu88bGY7taVWyEsg5cUlK07KgUh'
table_id = 'tbluQk6rXWJLNhIN'
golden_table_name = "lark_seto_philippines_direct_area"

mapping_dict = {
    'province_name': {'path': 'Tỉnh', 'type': 'str'},
    'province_id': {'path': 'province_id.[0].text', 'type': 'str'},
}

if __name__ == "__main__":
    df = LarkApiHandle().extract_table_to_df(base_id=base_id, table_id=table_id, mapping_dict=mapping_dict)

    if not df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database='alomix_skyward_data',
            table=golden_table_name, 
            df=df, 
            unique_columns=["record_id"], 
            log=True, 
            create_table=True, 
            mapping_dict=mapping_dict,
            db_type="golden"
        )
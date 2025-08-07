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

base_id = 'WaOrbnSSgamvogsQeivlAHn7g3c'
table_id = 'tbl472D8g89PIBBH'
golden_table_name = "lark_seto_lollibooks_pages"

mapping_dict = {
    'page_id': {'path': 'Page ID', 'type': 'str'},
    'page_name': {'path': 'Tên fanpage', 'type': 'str'},
}

if __name__ == "__main__":
    lark_client = LarkApiHandle(app_id='cli_a67977bad73a1010', app_secret='RxKUuC8PwRPxISumT2EQHbBo1VzWBwco')
    df = lark_client.extract_table_to_df(base_id=base_id, table_id=table_id, mapping_dict=mapping_dict)

    if not df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database='alomix_seto_data',
            table=golden_table_name, 
            df=df, 
            unique_columns=["page_id"], 
            log=True, 
            create_table=True, 
            mapping_dict=mapping_dict,
            db_type="golden"
        )
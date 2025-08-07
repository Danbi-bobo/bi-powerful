import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from queries import query
from cdp.domain.utils.log_helper import setup_logger

if __name__ == "__main__":
    setup_logger(__file__)

    base_id = 'PqisbUbgRaN3THsb8mdlndnWgXb'
    table_id = 'tblFLfi89YOxuL4e'
    lark_api_handler = LarkApiHandle()

    data = MariaDBHandler().read_from_db(query=query, output_type='dataframe')
    lark_api_handler.overwrite_table(base_id=base_id, table_id=table_id, input_type='dataframe', df=data)
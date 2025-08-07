import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from queries import card_transaction, account_transaction, topas_global_account, seto_phil_global_account
from cdp.domain.utils.log_helper import setup_logger

if __name__ == "__main__":
    setup_logger(__file__)
    
    base_id = 'JkBKbSSPQapddtshUdjle7ylg2g'

    account_transaction_data = MariaDBHandler().read_from_db(query=account_transaction, output_type='dataframe')
    card_transaction_data = MariaDBHandler().read_from_db(query=card_transaction, output_type='dataframe')
    seto_phil_global_account_transaction_data = MariaDBHandler().read_from_db(query=seto_phil_global_account, output_type='dataframe')
    topas_global_account_transaction_data = MariaDBHandler().read_from_db(query=topas_global_account, output_type='dataframe')

    lark_client = LarkApiHandle()

    lark_client.overwrite_table(
        base_id=base_id,
        table_id='tblSKEdaTs7L1GfO',
        input_type='dataframe',
        df=account_transaction_data
    )

    lark_client.overwrite_table(
        base_id=base_id,
        table_id='tbluWWL8rIxDifmF',
        input_type='dataframe',
        df=card_transaction_data
    )

    lark_client.overwrite_table(
        base_id=base_id,
        table_id='tblJlWc8WboBKqMu',
        input_type='dataframe',
        df=seto_phil_global_account_transaction_data
    )

    lark_client.overwrite_table(
        base_id=base_id,
        table_id='tblZLc9BzTS4Qjto',
        input_type='dataframe',
        df=topas_global_account_transaction_data
    )

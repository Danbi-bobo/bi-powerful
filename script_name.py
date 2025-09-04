import sys
import os
from dotenv import load_dotenv
load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler

# Mapping dictionary for pos_tags table
mapping_dict = {
    'id': {'type': 'str', 'sql_type': 'BIGINT'},
    'name': {'type': 'str', 'sql_type': 'VARCHAR(255)'},
    'color': {'type': 'str', 'sql_type': 'VARCHAR(50)'},
    'is_system_tag': {'type': 'bool', 'sql_type': 'BOOLEAN'},
    'shop_id': {'type': 'str', 'sql_type': 'BIGINT'},
    'group_id': {'type': 'str', 'sql_type': 'BIGINT'},
    'group_name': {'type': 'str', 'sql_type': 'VARCHAR(255)'},
    'updated_at': {'type': 'datetime', 'sql_type': 'TIMESTAMP'}
}

def create_pos_tags_table():
    """Create the pos_tags table in the database"""
    db_golden_name = os.getenv("DB_GOLDEN_NAME")
    table_name = "pos_tags"
    unique_columns = ["id", "shop_id"]
    
    # Initialize MariaDB handler
    db_handler = MariaDBHandler()
    
    # Create table using the mapping dictionary
    db_handler.create_table_from_mapping(
        database=db_golden_name,
        table=table_name,
        mapping_dict=mapping_dict,
        unique_columns=unique_columns,
        db_type="golden",
        output="create_table"
    )
    
    print(f"Successfully created table '{table_name}' in database '{db_golden_name}'")

def get_create_table_sql():
    """Get the CREATE TABLE SQL statement without executing it"""
    db_golden_name = os.getenv("DB_GOLDEN_NAME")
    table_name = "pos_tags"
    unique_columns = ["id", "shop_id"]
    
    # Initialize MariaDB handler
    db_handler = MariaDBHandler()
    
    # Get SQL query only
    sql_query = db_handler.create_table_from_mapping(
        database=db_golden_name,
        table=table_name,
        mapping_dict=mapping_dict,
        unique_columns=unique_columns,
        db_type="golden",
        output="query_only"
    )
    
    return sql_query

if __name__ == "__main__":
    # Option 1: Print the SQL statement
    print("CREATE TABLE SQL:")
    print("=" * 50)
    print(get_create_table_sql())
    print("=" * 50)
    
    # Option 2: Create the table (uncomment the line below to execute)
    create_pos_tags_table()
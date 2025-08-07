import mysql.connector
import pandas as pd
import numpy as np
from mysql.connector import Error, OperationalError
import os
from dotenv import load_dotenv
from typing import List, Dict, Literal, Optional
import logging
import time

load_dotenv()
logging.basicConfig(level=logging.INFO)

class MariaDBHandler:
    def __init__(self):
        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.database = None
        self.table = None
        self.connection = None

    def connect(self, database=None, max_retries=3, delay=5):
        retries = 0
        while retries < max_retries:
            try:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=database,
                    connection_timeout=30
                )
                if database:
                    logging.info(f"Connected to database {database}")
                else:
                    logging.info(f"Connected to host {self.host}")
                return
            
            except mysql.connector.Error as err:
                retries += 1
                logging.error(f"Connection attempt {retries} failed: {err}")
                if retries < max_retries:
                    logging.info(f"Retrying... ({retries}/{max_retries})")
                    time.sleep(delay)
                else:
                    logging.error("Maximum retry attempts reached, unable to connect to the database.")
                    self.connection = None
                    break

    def close(self):
        if self.connection and self.connection.is_connected():
            try:
                if self.connection.in_transaction:
                    self.connection.commit()
                self.connection.close()
                logging.info("Connection closed.")
            except Error as err:
                logging.error(f"Error while closing the connection: {err}")

    def insert_and_update_from_dict(self, database, table, data: List[Dict[str, any]], unique_columns: List[str], log=False, create_table = False, mapping_dict = None, db_type: Literal["golden", "raw"] = "golden", updated_flag: Literal[True, False] = False, overwrite_table: Literal[True, False] = False):
        if not data:
            logging.warning("Input data is empty.")
            return

        if log:
            logging.info(f"List contains {len(data)} records.")
        if overwrite_table == True:
                self.truncate_table(database=database, table=table)
        
        self.connect(database)
        if not self.connection:
            logging.error("Failed to connect to the database.")
            return

        cursor = self.connection.cursor()
        if create_table == True:
            create_table_sql = self.create_table_from_mapping(database=database, table=table, mapping_dict=mapping_dict, db_type=db_type, output="query_only", unique_columns=unique_columns)
            cursor.execute(create_table_sql)
            self.connection.commit()
        
        try:
            cursor.execute(f"SHOW COLUMNS FROM `{table}`")
            table_columns = {row[0] for row in cursor.fetchall()}
            has_updated_flag = "updated_flag" in table_columns and updated_flag

            columns = ', '.join(f"`{col}`" for col in data[0].keys())
            placeholders = ', '.join(['%s'] * len(data[0]))
            update_columns = ', '.join([
                f"{col} = COALESCE(VALUES({col}), {col})"
                for col in data[0].keys() if col not in unique_columns
            ])
            
            if has_updated_flag:
                update_columns += ", updated_flag = 1"
            
            query = f"""
                INSERT INTO `{table}` ({columns})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_columns};
            """

            values = [tuple(item.values()) for item in data]
            
            cursor.executemany(query, values)
            
            if has_updated_flag:
                cursor.execute(f"""
                    UPDATE `{table}`
                    SET updated_flag = CASE 
                        WHEN updated_flag <> 1 or updated_flag IS NULL THEN -1
                        WHEN updated_flag = 1 THEN 0
                        ELSE updated_flag
                    END;                
                """)
            
            self.connection.commit()
            logging.info(f"{cursor.rowcount} rows have been inserted or updated.")

        except OperationalError:
            logging.error("Connection error, retrying...")
        except Error as e:
            logging.error(f"Error: {e}")
            self.connection.rollback()
        finally:
            cursor.close()
            self.close()

    def insert_and_update_from_df(self, database=None, table=None, df: pd.DataFrame=None, unique_columns: Optional[List[str]]=None, log=False, create_table = False, mapping_dict = None, db_type: Literal["golden", "raw"] = "golden", updated_flag = False, overwrite_table=False):
        if df is None:
            df = pd.DataFrame()
        else:
            df = df.copy()
        
        if df.empty:
            logging.warning("Empty DataFrame.")
            return
        
        if log:
            logging.info(f"DataFrame has {len(df)} rows.")
            logging.info(f"DataFrame head:\n{df.head().to_string()}")

        if mapping_dict is None:
            mapping_dict = {}

        if "record_id" in df.columns and "record_id" not in mapping_dict:
            mapping_dict["record_id"] = {"type": "str", "sql_type": "VARCHAR(255)"}
        
        if db_type == "raw":
            df = df.astype(str)
            if not database:
                database = os.getenv("DB_RAW_NAME")
        else:
            if not database:
                database = os.getenv("DB_GOLDEN_NAME")
        
        df.replace([np.nan, "None"], None, inplace=True)
        data = df.to_dict(orient='records')
        self.insert_and_update_from_dict(database=database, table=table, data=data, unique_columns=unique_columns, create_table=create_table, mapping_dict=mapping_dict, db_type=db_type, updated_flag=updated_flag, overwrite_table=overwrite_table)

    def read_from_db(self, query: str, database=None, output_type: Literal['dataframe', 'list_of_dicts', 'list_of_tuples'] = 'dataframe', params: tuple = ()) -> any:
        self.connect(database=database)
        if not self.connection:
            logging.error("Failed to connect to the database.")
            return None

        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()

            if not rows:
                logging.warning("No data returned.")
                return None

            if output_type == 'dataframe':
                df = pd.DataFrame(rows)
                logging.info(f"Read {len(df)} rows of data.")
                return df

            elif output_type == 'list_of_dicts':
                logging.info(f"Read {len(rows)} rows of data.")
                return rows

            elif output_type == 'list_of_tuples':
                data = [tuple(row.values()) for row in rows]
                logging.info(f"Read {len(data)} rows of data.")
                return data

        except mysql.connector.Error as e:
            logging.error(f"Error executing query: {e}")
            return None
        finally:
            cursor.close()
            if self.connection and self.connection.is_connected():
                self.close()

    def create_table_from_mapping(self, database, table, mapping_dict, unique_columns: List[str], db_type: Literal["golden", "raw"] = "golden", output: Literal["query_only", "create_table"] = "create_table"):
        type_mapping = {
            "int": "INT",
            "float": "FLOAT",
            "double": "DOUBLE",
            "ms_timestamp": "TIMESTAMP",
            "lark_date": "DATETIME",
            "user_email": "VARCHAR(255)",
            "user_id": "VARCHAR(255)",
            "lark_formula": "TEXT",
            "lark_user": "JSON",
            "str": "VARCHAR(255)"
        }

        for col in unique_columns:
            if col not in mapping_dict:
                mapping_dict[col] = {"type": "str"}

        columns = []
        for col_name, col_info in mapping_dict.items():
            if db_type == "raw":
                sql_type = "VARCHAR(255)"
            else:
                sql_type = col_info.get("sql_type", type_mapping.get(col_info["type"], "VARCHAR(255)"))
            columns.append(f"`{col_name}` {sql_type}")

        unique_key = f",\n  UNIQUE KEY (`{'`, `'.join(unique_columns)}`)" if unique_columns else ""
        etl_column = "`etl_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (
                {", ".join(columns)},
                {etl_column}
                {unique_key}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """

        if output == "query_only":
            return create_table_sql.strip()
        else:
            try:
                self.connect(database)
                if not self.connection:
                    logging.error("Failed to connect to the database.")
                    return

                cursor = self.connection.cursor()
                cursor.execute(create_table_sql)
                self.connection.commit()

            except OperationalError:
                logging.error("Connection error, retrying...")
            except Error as e:
                logging.error(f"Error: {e}")
                self.connection.rollback()
            finally:
                cursor.close()
                self.close()
    
    def truncate_table(self, database, table):
        query = f"TRUNCATE TABLE `{database}`.`{table}`"

        try:
            self.connect(database)
            if not self.connection:
                logging.error("Failed to connect to the database.")
                return

            cursor = self.connection.cursor()
            cursor.execute(query)

        except OperationalError:
            logging.error("Connection error, retrying...")
        except Error as e:
            logging.error(f"Error: {e}")
            self.connection.rollback()
        finally:
            cursor.close()
            self.close()

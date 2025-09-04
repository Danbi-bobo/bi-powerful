import sys
import os
import json
import logging
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.pos.pos_api_handler import PosAPIHandler
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger

setup_logger(__file__)

def load_config_shops():
    config_file = rf"{CDP_PATH}/entries/golden/pos/config_shops.json"
    with open(config_file, 'r') as file:
        return json.load(file)

def safe_convert_to_int(value):
    """Safely convert value to int"""
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def parse_datetime_safe(dt_str):
    """Safely parse datetime string"""
    if not dt_str:
        return None
    try:
        if isinstance(dt_str, str):
            # Handle format: "2023-05-27T02:17:12"
            if '.' in dt_str:
                dt_str = dt_str.split('.')[0]  # Remove microseconds
            dt_str = dt_str.replace('Z', '')  # Remove Z if present
            return datetime.fromisoformat(dt_str)
        return dt_str
    except:
        return None

def process_employee(employee, shop_id):
    """Convert employee data to database record"""
    
    # Extract department info
    department = employee.get('department', {})
    department_id = department.get('id') if department else None
    department_name = department.get('name') if department else None
    
    # Extract user info
    user_info = employee.get('user', {})
    
    record = {
        # Primary keys
        'shop_id': int(shop_id),
        'user_id': employee.get('user_id'),
        
        # Department and role
        'department_id': safe_convert_to_int(department_id),
        'department_name': department_name,
        'role': safe_convert_to_int(employee.get('role')),
        'pending_order_count': safe_convert_to_int(employee.get('pending_order_count', 0)),
        
        # User information from user object
        'name': user_info.get('name') if user_info else None,
        'fb_id': user_info.get('fb_id') if user_info else None,
        'email': user_info.get('email') if user_info else None,
        'phone_number': user_info.get('phone_number') if user_info else None,
        
        # Timestamps
        'inserted_at': parse_datetime_safe(employee.get('inserted_at')),
        'updated_at': parse_datetime_safe(employee.get('updated_at'))
    }
    
    # Clean empty values
    for key, value in record.items():
        if value == '' or value == 'None':
            record[key] = None
    
    return record

def fetch_and_save_employees():
    shops = load_config_shops().get("shops", [])
    db_golden = os.getenv("DB_GOLDEN_NAME")
    
    total_employees = 0
    
    for shop in shops:
        shop_id = shop.get("shop_id")
        api_key = shop.get("api_key")
        
        if not (shop_id and api_key):
            continue
            
        try:
            logging.info(f"Processing employees for shop {shop_id}")
            
            pos_handler = PosAPIHandler(shop_id=shop_id, api_key=api_key)
            employees_list = pos_handler.get_all("users")  # Change endpoint if needed
            
            if not employees_list:
                logging.info(f"No employees for shop {shop_id}")
                continue
            
            # Process all employees
            all_records = []
            for employee in employees_list:
                record = process_employee(employee, shop_id)
                if record['user_id']:  # Only add if has valid user_id
                    all_records.append(record)
            
            logging.info(f"Shop {shop_id}: {len(employees_list)} employees → {len(all_records)} records")
            
            # Save to database
            if all_records:
                MariaDBHandler().insert_and_update_from_dict(
                    database=db_golden,
                    table="pos_employees",
                    data=all_records,
                    unique_columns=["user_id", "shop_id"]
                )
                
                total_employees += len(all_records)
                logging.info(f"Shop {shop_id}: ✓ Saved {len(all_records)} employees")
            
        except Exception as e:
            logging.error(f"Shop {shop_id} failed: {e}")
            continue
    
    logging.info(f"Total employees processed: {total_employees}")

def get_employees_summary():
    """Query function to get employees summary by shop and department"""
    db_golden = os.getenv("DB_GOLDEN_NAME")
    
    query = f"""
        SELECT 
            shop_id,
            department_name,
            COUNT(*) as employee_count,
            SUM(pending_order_count) as total_pending_orders,
            COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as employees_with_email,
            COUNT(CASE WHEN phone_number IS NOT NULL THEN 1 END) as employees_with_phone
        FROM {db_golden}.pos_employees
        GROUP BY shop_id, department_name
        ORDER BY shop_id, department_name
    """
    
    return MariaDBHandler().read_from_db(query=query, database=db_golden)

def get_employees_by_shop(shop_id=None):
    """Query function to get employees by shop"""
    db_golden = os.getenv("DB_GOLDEN_NAME")
    
    where_clause = f"WHERE shop_id = '{shop_id}'" if shop_id else ""
    
    query = f"""
        SELECT 
            user_id,
            shop_id,
            name,
            email,
            phone_number,
            department_name,
            role,
            pending_order_count,
            inserted_at,
            updated_at
        FROM {db_golden}.pos_employees
        {where_clause}
        ORDER BY department_name, name
    """
    
    return MariaDBHandler().read_from_db(query=query, database=db_golden)

def get_employees_by_department(department_id, shop_id=None):
    """Query function to get employees by department"""
    db_golden = os.getenv("DB_GOLDEN_NAME")
    
    where_conditions = [f"department_id = {department_id}"]
    if shop_id:
        where_conditions.append(f"shop_id = '{shop_id}'")
    
    where_clause = "WHERE " + " AND ".join(where_conditions)
    
    query = f"""
        SELECT 
            user_id,
            shop_id,
            name,
            email,
            phone_number,
            department_name,
            role,
            pending_order_count
        FROM {db_golden}.pos_employees
        {where_clause}
        ORDER BY name
    """
    
    return MariaDBHandler().read_from_db(query=query, database=db_golden)

def get_employees_with_pending_orders():
    """Query function to get employees with pending orders"""
    db_golden = os.getenv("DB_GOLDEN_NAME")
    
    query = f"""
        SELECT 
            user_id,
            shop_id,
            name,
            email,
            department_name,
            pending_order_count
        FROM {db_golden}.pos_employees
        WHERE pending_order_count > 0
        ORDER BY pending_order_count DESC, shop_id, name
    """
    
    return MariaDBHandler().read_from_db(query=query, database=db_golden)

if __name__ == "__main__":
    logging.info("Starting POS Employees extraction")
    fetch_and_save_employees()
    logging.info("Completed")
    
    # Example usage:
    # summary = get_employees_summary()
    # employees = get_employees_by_shop()
    # specific_shop = get_employees_by_shop(shop_id="1021208973")
    # dept_employees = get_employees_by_department(department_id=12941594)
    # pending_orders = get_employees_with_pending_orders()
    # 
    # print("Summary:", summary)
    # print("Employees with pending orders:", pending_orders)
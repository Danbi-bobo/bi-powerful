import sys
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import logging

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.http.http_client import HttpClient
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger

# Configuration
EXCHANGE_RATES_API_KEY = os.getenv("EXCHANGE_RATES_API_KEY")
BASE_URL = "https://api.exchangeratesapi.io/v1"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["VND", "THB", "KHR", "USD"]

# Database configuration
DB_GOLDEN_NAME = os.getenv("DB_GOLDEN_NAME")
TABLE_NAME = "exchange_rates"

# Mapping dictionary for creating table
mapping_dict = {
    'date': {'type': 'str', 'sql_type': 'DATE'},
    'base_currency': {'type': 'str', 'sql_type': 'VARCHAR(3)'},
    'target_currency': {'type': 'str', 'sql_type': 'VARCHAR(3)'},
    'exchange_rate': {'type': 'double', 'sql_type': 'DECIMAL(15,8)'},
    'source': {'type': 'str', 'sql_type': 'VARCHAR(20)'}
}

class ExchangeRatesExtractor:
    def __init__(self):
        self.client = HttpClient(timeout=30)
        self.db_handler = MariaDBHandler()
        
    def get_exchange_rates_for_date(self, date_str):
        """
        Lấy tỷ giá Vietcombank cho một ngày cụ thể
        """
        url = f"https://www.vietcombank.com.vn/api/exchangerates?date={date_str}"
        
        try:
            response = self.client.get(url)
            data = response.json()
            
            if "Data" in data:
                return data  # trả về toàn bộ JSON
            else:
                logging.error(f"No 'Data' field in response for {date_str}")
                return None
                
        except Exception as e:
            logging.error(f"Error fetching data for {date_str}: {e}")
            return None
    
    def calculate_cross_rates(self, data):
        """
        Lấy tỷ giá USD -> VND từ JSON Vietcombank
        """
        if not data or "Data" not in data:
            return []
        
        date = data.get("Date", "")[:10]  # YYYY-MM-DD
        records = []

        for item in data["Data"]:
            if item["currencyCode"] == "USD":
                usd_to_vnd = float(item["transfer"].replace(",", ""))  # hoặc 'transfer'/'sell'
                records.append({
                    "date": date,
                    "base_currency": "USD",
                    "target_currency": "VND",
                    "exchange_rate": usd_to_vnd,
                    "source": "VIETCOMBANK"
                })
                break  # chỉ lấy USD
        
        return records

    
    def get_date_range(self, start_date, end_date):
        """
        Tạo danh sách các ngày từ start_date đến end_date
        
        Args:
            start_date (str): Ngày bắt đầu YYYY-MM-DD
            end_date (str): Ngày kết thúc YYYY-MM-DD
            
        Returns:
            list: Danh sách các ngày
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        
        return dates
    
    def get_existing_dates(self):
        """
        Lấy danh sách các ngày đã có dữ liệu trong database
        
        Returns:
            set: Set các ngày đã có dữ liệu
        """
        query = f"SELECT DISTINCT date FROM {TABLE_NAME}"
        try:
            result = self.db_handler.read_from_db(
                database=DB_GOLDEN_NAME,
                query=query,
                output_type='dataframe'
            )
            
            if result is not None and not result.empty:
                return set(result['date'].astype(str).tolist())
            return set()
            
        except Exception as e:
            logging.warning(f"Could not check existing dates: {e}")
            return set()
    
    def extract_exchange_rates(self, start_date, end_date, skip_existing=True):
        """
        Kéo dữ liệu tỷ giá cho khoảng thời gian xác định

        Args:
            start_date (str): Ngày bắt đầu YYYY-MM-DD
            end_date (str): Ngày kết thúc YYYY-MM-DD
            skip_existing (bool): Bỏ qua các ngày đã có dữ liệu
        """
        logging.info(f"Extracting exchange rates from {start_date} to {end_date}")

        # Tạo bảng nếu chưa tồn tại
        try:
            self.db_handler.create_table_from_mapping(
                database=DB_GOLDEN_NAME,
                table=TABLE_NAME,
                mapping_dict=mapping_dict,
                unique_columns=['date', 'base_currency', 'target_currency'],
                db_type='golden'
            )
            logging.info("Exchange rates table ready")
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            return

        # Lấy danh sách ngày cần xử lý
        dates_to_process = self.get_date_range(start_date, end_date)

        # Bỏ qua các ngày đã có dữ liệu nếu được yêu cầu
        if skip_existing:
            existing_dates = self.get_existing_dates()
            dates_to_process = [d for d in dates_to_process if d not in existing_dates]
            logging.info(f"Skipping {len(self.get_date_range(start_date, end_date)) - len(dates_to_process)} existing dates")

        logging.info(f"Processing {len(dates_to_process)} dates")

        total_records = 0
        successful_dates = 0
        batch_records = []
        failed_dates = []   # lưu các ngày bị fail

        for i, date_str in enumerate(dates_to_process, 1):
            logging.info(f"Processing {date_str} ({i}/{len(dates_to_process)})")

            rates_data = self.get_exchange_rates_for_date(date_str)

            if rates_data:
                # Tính tỷ giá chéo
                records = self.calculate_cross_rates(rates_data)

                if records:
                    batch_records.extend(records)
                    successful_dates += 1
                    logging.info(f"✅ {date_str}: {len(records)} rates calculated")
                else:
                    logging.warning(f"⚠️ {date_str}: No rates calculated")
                    failed_dates.append(date_str)
            else:
                logging.error(f"❌ {date_str}: Failed to fetch data")
                failed_dates.append(date_str)

            # Lưu batch khi đủ 50 records hoặc kết thúc
            if len(batch_records) >= 50 or i == len(dates_to_process):
                if batch_records:
                    try:
                        self.db_handler.insert_and_update_from_dict(
                            database=DB_GOLDEN_NAME,
                            table=TABLE_NAME,
                            data=batch_records,
                            unique_columns=['date', 'base_currency', 'target_currency']
                        )

                        total_records += len(batch_records)
                        logging.info(f"💾 Saved batch: {len(batch_records)} records")
                        batch_records = []

                    except Exception as e:
                        logging.error(f"Error saving batch: {e}")
                        batch_records = []

            # Rate limiting - API thường có giới hạn requests/phút
            time.sleep(0.2)

        # =========================
        # Retry cho những ngày fail
        # =========================
        if failed_dates:
            logging.info(f"🔁 Retrying failed dates: {failed_dates}")
            for date_str in failed_dates:
                rates_data = self.get_exchange_rates_for_date(date_str)
                if rates_data:
                    records = self.calculate_cross_rates(rates_data)
                    if records:
                        try:
                            self.db_handler.insert_and_update_from_dict(
                                database=DB_GOLDEN_NAME,
                                table=TABLE_NAME,
                                data=records,
                                unique_columns=['date', 'base_currency', 'target_currency']
                            )
                            logging.info(f"✅ Recovered {date_str} successfully")
                        except Exception as e:
                            logging.error(f"❌ Still failed to save {date_str}: {e}")
                    else:
                        logging.warning(f"⚠️ {date_str}: No rates calculated on retry")
                else:
                    logging.error(f"❌ Retry failed for {date_str}")

        # Summary
        logging.info(f"🎯 Extraction completed:")
        logging.info(f"   Successful dates: {successful_dates}/{len(dates_to_process)}")
        logging.info(f"   Total records saved: {total_records}")
        if failed_dates:
            logging.warning(f"   Still failed dates after retry: {failed_dates}")

    
    def get_latest_rates(self, base_currency=None, target_currency=None, limit=10):
        """
        Lấy tỷ giá mới nhất từ database
        
        Args:
            base_currency (str): Tiền tệ gốc (optional)
            target_currency (str): Tiền tệ đích (optional)
            limit (int): Số record trả về
            
        Returns:
            DataFrame: Dữ liệu tỷ giá
        """
        where_conditions = []
        if base_currency:
            where_conditions.append(f"base_currency = '{base_currency}'")
        if target_currency:
            where_conditions.append(f"target_currency = '{target_currency}'")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT date, base_currency, target_currency, exchange_rate, source
            FROM {TABLE_NAME}
            {where_clause}
            ORDER BY date DESC
            LIMIT {limit}
        """
        
        return self.db_handler.read_from_db(
            database=DB_GOLDEN_NAME,
            query=query,
            output_type='dataframe'
        )
    
    def close(self):
        """Đóng kết nối"""
        self.client.close()

# =============================================================================
# MAIN EXECUTION FUNCTIONS
# =============================================================================

def run_daily_update():
    """Chạy hàng ngày để cập nhật tỷ giá mới"""
    logging.info("Running daily exchange rates update")
    extractor = ExchangeRatesExtractor()
    
    try:
        # Kéo 7 ngày gần đây để đảm bảo không bỏ sót
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        
        extractor.extract_exchange_rates(start_date, end_date, skip_existing=True)
    finally:
        extractor.close()

def run_historical_data():
    """Kéo dữ liệu lịch sử"""
    logging.info("Running historical exchange rates extraction")
    
    extractor = ExchangeRatesExtractor()
    
    try:
        # Tính toán khoảng thời gian
        start_date = "2024-07-11"
        end_date = "2024-07-31"
        
        logging.info(f"Extracting historical data from {start_date} to {end_date}")
        extractor.extract_exchange_rates(start_date, end_date, skip_existing=True)
        
        # Hiển thị một số tỷ giá mới nhất để kiểm tra
        logging.info("\nLatest exchange rates:")
        # show_sample_rates(extractor)
                
    finally:
        extractor.close()

def run_specific_date_range(start_date, end_date):
    """
    Kéo dữ liệu cho khoảng thời gian cụ thể
    
    Args:
        start_date (str): Ngày bắt đầu YYYY-MM-DD
        end_date (str): Ngày kết thúc YYYY-MM-DD
    """
    extractor = ExchangeRatesExtractor()
    
    try:
        extractor.extract_exchange_rates(start_date, end_date, skip_existing=False)
    finally:
        extractor.close()

def run_year_data(year, month):
    """
    Kéo dữ liệu cho cả năm
    
    Args:
        year (int): Năm cần kéo dữ liệu
    """
    logging.info(f"Extracting exchange rates for year {year}")
    
    start_date = f"{year}-{month}-01"
    end_date = f"{year}-{month}-31"
    
    # Nếu là năm hiện tại, chỉ kéo đến hôm nay
    # current_year = datetime.now().year
    # if year == current_year:
    #     end_date = datetime.now().strftime("%Y-%m-%d")
    
    run_specific_date_range(start_date, end_date)

def main():
    """Main function để chạy script"""
    logging.info(EXCHANGE_RATES_API_KEY)
    # Kiểm tra API key
    if not EXCHANGE_RATES_API_KEY:
        logging.error("❌ EXCHANGE_RATES_API_KEY not found in .env file")
        logging.error("Please add: EXCHANGE_RATES_API_KEY=your_api_key_here")
        logging.error("Get your free API key at: https://exchangeratesapi.io/")
        return False
    
    logging.info("🚀 Starting Exchange Rates Data Extraction")
    logging.info(f"📅 Target currencies: {', '.join(TARGET_CURRENCIES)}")
    logging.info(f"🔑 API Key: {EXCHANGE_RATES_API_KEY[:8]}...{EXCHANGE_RATES_API_KEY[-4:]}")
    
    try:
        # Chạy kéo dữ liệu lịch sử (2024 -> hiện tại)
        run_historical_data()
        
        # Hiển thị ví dụ thực tế
        # show_practical_examples()
        
        logging.info("✅ Exchange rates extraction completed successfully!")
        
        # Gợi ý setup cron job
        logging.info("\n🔄 To keep data updated, set up a daily cron job:")
        logging.info("0 9 * * * cd /path/to/project && python entries/golden/exchange_rates/run.py --daily")
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Error during extraction: {e}")
        return False

if __name__ == "__main__":
    setup_logger(__file__)
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Exchange Rates Data Extractor')
    parser.add_argument('--daily', action='store_true', help='Run daily update (last 7 days)')
    parser.add_argument('--year', type=int, help='Extract data for specific year')
    parser.add_argument('--month', type=int, help='Extract data for specific month')
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    try:
        if args.daily:
            logging.info("Running daily update mode")
            run_daily_update()
        elif args.year and args.month:
            logging.info(f"Running year extraction for {args.year}")
            run_year_data(args.year, args.month)
        elif args.start and args.end:
            logging.info(f"Running custom date range: {args.start} to {args.end}")
            run_specific_date_range(args.start, args.end)
        else:
            # Default: full historical extraction
            main()
            
    except KeyboardInterrupt:
        logging.info("👋 Extraction interrupted by user")
    except Exception as e:
        logging.error(f"❌ Fatal error: {e}")
        raise
import os
import time
import glob
from dotenv import load_dotenv

# Load biến môi trường từ .env
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH", "")

# Cấu hình
LOG_EXTENSIONS = ["*.log"]  # Có thể mở rộng nếu cần
DAYS_TO_KEEP = 14  # Số ngày giữ log

def cleanup_logs():
    if not CDP_PATH:
        print("CDP_PATH is not set. Exiting cleanup.")
        return

    now = time.time()
    cutoff_time = now - (DAYS_TO_KEEP * 86400)  # 86400 giây = 1 ngày

    deleted_files = []

    # Duyệt toàn bộ thư mục con trong project
    for root, dirs, files in os.walk(CDP_PATH):
        for ext in LOG_EXTENSIONS:
            for log_file in glob.glob(os.path.join(root, ext)):
                if os.path.getmtime(log_file) < cutoff_time:
                    os.remove(log_file)
                    deleted_files.append(log_file)

    if deleted_files:
        print(f"Deleted {len(deleted_files)} old log files:")
        for file in deleted_files:
            print(f" - {file}")
    else:
        print("No old logs found.")

if __name__ == "__main__":
    cleanup_logs()

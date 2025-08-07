import sys
import os
import logging
import atexit
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH", "")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle

class ErrorNotificationHandler(logging.Handler):
    def __init__(self, script_path):
        super().__init__()
        self.script_path = script_path
        self.error_logs = []
        self.error_triggered = False
        atexit.register(self.send_errors)

    def emit(self, record):
        if record.levelno >= logging.ERROR:
            self.error_logs.append(self.format(record))
            self.error_triggered = True

    def send_errors(self):
        if self.error_triggered and self.error_logs:
            unique_logs = list(dict.fromkeys(self.error_logs))
            error_msg = "<hr />".join(unique_logs)
            LarkApiHandle().error_noti(path=self.script_path, msg=error_msg)


def setup_logger(script_path, return_logger=False):
    base_name = os.path.splitext(os.path.basename(script_path))[0]
    
    log_dir = os.path.join(os.path.dirname(script_path), "log")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = f"{base_name}-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_path = os.path.join(log_dir, log_filename)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if not logger.hasHandlers():
        logging.basicConfig(level=logging.INFO)

    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    error_handler = ErrorNotificationHandler(script_path)
    logger.addHandler(error_handler)

    if return_logger:
        return logger
    return None

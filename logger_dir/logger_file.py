import logging
import os
from datetime import datetime

DIR = "Logs"
def log_file():
    return f"log_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.log"


os.makedirs(DIR, exist_ok=True)
lf = log_file()
log_file_name = os.path.join(DIR, lf)

logging.basicConfig(filename=log_file_name,
                    filemode="w",
                    format='[%(asctime)s];%(levelname)s;%(lineno)d;%(filename)s;%(funcName)s();%(message)s',
                    level=logging.INFO
                    )





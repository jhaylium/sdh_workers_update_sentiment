import logging
import os
import datetime
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


fileHandler = logging.FileHandler(f"{os.environ.get('log_location')}load_qualtrics_{datetime.now().strftime('%Y-%m-%d')}.log")
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)

logger.setLevel(logging.INFO)
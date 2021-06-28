import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime
import time
import random
from traceback import format_exc

load_dotenv()

class PostgresDB:
    def __init__(self):
        self.conn = None

    def connect(self):
        if self.conn is None:
            self.conn = psycopg2.connect(
                host=os.environ['logging_host'],
                user=os.environ['logging_user'],
                password=os.environ['logging_pw'],
                port=os.environ['logging_port'],
                dbname=os.environ['logging_name']
            )

    def insert_event(self, payload:dict):
        if not self.conn:
            self.connect()
        conn = self.conn
        cursor = conn.cursor()
        cmd = f"""insert into activity.events(event_name, event_status, event_timestamp, event_message, machine_id,
                    event_duration, project_id, customer_id)
                    values ('{payload['event_name']}', '{payload['event_status']}', '{payload['event_timestamp']}',
                    '{payload['event_message']}', {os.environ['machine_id']}, {round(payload['duration'],3)}, 
                    {payload['project_id']}, {payload['customer_id']})
                """
        cursor.execute(cmd)
        conn.commit()
        cursor.close()
        conn.commit()

if __name__ == "__main__":
    start = time.time()
    try:
        time.sleep(random.randrange())
        payload = {"customer_id": 17,
                   "project_id": '88',
                    "event_name": "db_test",
                    "event_status":"0",
                    "event_timestamp": datetime.utcnow(),
                    "event_message":"Test Event",
                    "duration": time.time() - start
                    }

        db_logger = PostgresDB()
        db_logger.insert_event(payload)
    except Exception:
        error_msg = format_exc()
        print("Error Length:", len(format_exc()))
        error_msg = error_msg[:999].replace("'", "").replace('"', '')
        payload = {"customer_id": 17,
                   "project_id": '88',
                   "event_name": "db_test",
                   "event_status": "-1",
                   "event_timestamp": datetime.utcnow(),
                   "event_message": f"{error_msg}",
                   "duration": time.time() - start
                   }
        db_logger = PostgresDB()
        db_logger.insert_event(payload)
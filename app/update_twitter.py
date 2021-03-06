import boto3, json, psycopg2, os, logging, traceback
from dotenv import load_dotenv
from app.Postgres import PostgresDB
from datetime import datetime
from time import time


def log_text(customer, msg, project):
    return f"Activity: Load Qualtrics Refresh | Customer:{customer} | Project:{project} |msg:{msg}"

load_dotenv()

connection_string = f"""user={os.environ.get('main_db_user')} 
                    password={os.environ.get('main_db_pw')} 
                    host={os.environ.get('main_db_host')} 
                    port={os.environ.get('main_db_port')} 
                    dbname={os.environ.get('main_db_name')}"""
cmd = 'select * from data_service.refresh_twitter'
conn = psycopg2.connect(connection_string)
cursor = conn.cursor()
cursor.execute(cmd)
# conn.close()
twitter_projects = cursor.fetchall()
lam = boto3.client('lambda', aws_access_key_id=os.environ.get('aws_admin_access_key'),
                     aws_secret_access_key=os.environ.get('aws_admin_secret_key'),
                     region_name=os.environ.get('aws_admin_region'))

start = time()
for i in twitter_projects:
    project = i[0]
    project_id = project['project_id']
    customer_id = project['customer_id']
    project_name = project['internal_project_name']
    print(f"Name: {project['user_project_name']} CID: {customer_id} | PID: {project_id} | PN: {project_name}")
    logging.info(log_text(customer=customer_id, msg="Starting Refresh", project=project_name))
    try:
        get_fields_cmd = f"select field_name from sdh.project_sentiment_fields where project_id = {project_id}"
        cursor.execute(get_fields_cmd)
        sentiment_fields = cursor.fetchall()
        sentiment_fields = [field[0] for field in sentiment_fields]
        project['sentiment_fields'] = sentiment_fields
        responses_cmd = f"select tweet_id from {project['schema_name']}.{project['internal_project_name']}"
        cursor.execute(responses_cmd)
        responses = [x[0] for x in cursor.fetchall()]
        project['responses'] = responses
        lam.invoke(
            FunctionName="sdh_process_twitter_sentiment",
            InvocationType="Event",
            Payload=json.dumps(project)
        )
        payload = {"customer_id": customer_id,
                   "project_id": project_id,
                   "event_name": "Call Twitter Lambdas",
                   "event_status": "0",
                   "event_timestamp": datetime.utcnow(),
                   "event_message": "Success",
                   "duration": time() - start}
        db_logger = PostgresDB()
        db_logger.insert_event(payload)
    except Exception:
        error_message = traceback.format_exc()
        error_message = error_message[:999].replace("'", "").replace('"', '')
        payload = {"customer_id": customer_id,
         "project_id": project_id,
         "event_name": "Call Twitter Lambdas",
         "event_status": "-1",
         "event_timestamp": datetime.utcnow(),
         "event_message": "Success",
         "duration": time() - start}
        db_logger = PostgresDB()
        db_logger.insert_event(payload)
        continue



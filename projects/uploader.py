import json
import boto3
import os
from .helpers import get_redis_client
from loguru import logger
from .models import DataAction
from projects import db

from config import get_env,basedir
from celery import shared_task





@shared_task
def upload_to_S3(func_id):
    try:
        logger.info(f'UPLOADING DATA FOR {func_id}')
        s3 = boto3.resource(
        service_name='s3',
        region_name=get_env('S3_REGION'),
        aws_access_key_id=get_env('S3_ACCESS_KEY'),
        aws_secret_access_key=get_env('S3_SECRET_KEY')
        )
        db_data = DataAction.query.filter_by(func_id=func_id).first()
        func_name = func_id.split('.')[0]
        s3.Bucket('datama11').upload_file(Filename=os.path.join(f"{basedir}/Scripts",f'{func_name}.xlsx'),
                                                                Key=f'{func_name}.xlsx')
        db_data.current_state = "COMPLETED"
        db_data.is_being_processed = False
        db_data.is_main_completed = True
        db_data.comment = "Data scrapping and upload completed"
        db.session.commit()
        return True
    except Exception as e:
        print(e)
        return False
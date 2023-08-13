import json
import os
from .helpers import get_redis_client
from loguru import logger

from config import basedir
import pandas as pd
import openpyxl

from .models import DataAction
from projects import db

from celery import shared_task




@shared_task
def write_to_excel(func_id):
    try:
        logger.info(f'WRITING DATA FOR {func_id}')
        client = get_redis_client()
        if client is None:
            return False
        payloads = json.loads(client.get(func_id))
        if not payloads:
            return False
        db_data = DataAction.query.filter_by(func_id=func_id).first()
        payload = payloads['scraped_data']
        func_name = func_id.split('.')[0]
        #print(list(payload.values()),list(payload.keys()))
        df = pd.DataFrame(payload)
        df.to_excel(os.path.join(f"{basedir}/Scripts",f'{func_name}.xlsx'),sheet_name='sheet',index=False)
        db_data.current_state = "UPLOADING_DATA"
        db.session.commit()
        client.set(func_id,json.dumps(payload))
        return True
    except Exception as e:
        print(e)
        return False

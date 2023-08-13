import json
from .helpers import get_redis_client
from loguru import logger
from projects import db

from config import basedir
from celery import shared_task
from .models import DataAction






@shared_task
def aggregate_data(func_id):
    try:
        logger.info(f'AGGREGATING DATA FOR {func_id}')
        client = get_redis_client()
        if client is None:
            return False,False
        payload = json.loads(client.get(func_id))
        data_array = {}
        print('------------------',payload)
        db_data = DataAction.query.filter_by(func_id=func_id).first()
        if payload.get("depends_on",None):
            return True,False #processed,move_to_next_phase
        if not payload["linked_func"]:
            db_data.current_state = "WRITING_DATA"
            db.session.commit()
            client.set(func_id,json.dumps(payload))
            return True,True
        else:
            func_ids = payload["linked_func"]
            for func in func_ids:
                payload_1 = json.loads(client.get(func))
                if db_data.current_state != "DATA_AGGREGATION":
                    db_data.comment = f"Awaiting linked function {func} to attain aggregation state"
                    db.session.commit()
                    client.set(func_id,json.dumps(payload))
                    return True,False
                data_array.update(payload_1["scraped_data"])
            data_array.update(payload["scraped_data"])
            payload["scraped_data"] = data_array
            db_data.current_state = "WRITING_DATA"
            db.session.commit()
            client.set(func_id,json.dumps(payload))
            return True,True
    except Exception as e:
        print(e)
        return False,False
                
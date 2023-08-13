import json
from .helpers import get_redis_client
from loguru import logger

from config import basedir




def clean_data(func_id):
    try:
        client = get_redis_client()
        if client is None:
            return False
        payload = json.loads(client.get(func_id))
        data = payload["scraped_data"]
        data_copy = data.copy()
        for dt in data_copy:
            if not data[dt]:
                data.pop(dt)
        payload["scraped_data"] = data
        payload["curr_state"] = "Data Agregation"
        client.set(func_id,json.dumps(payload))
        return True
    except Exception as e:
        print(e)
        return False
        
        
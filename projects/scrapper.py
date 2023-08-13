
import subprocess
import sys
import json
from .helpers import get_redis_client
from loguru import logger
import importlib

from config import basedir
from .models import DataAction
from projects import db,create_app







def run_scrapper(func_id):

    client = get_redis_client()
    if client is None:
        return False
    payloads = json.loads(client.get(func_id))
    if payloads is not None:
        if payloads['dependencies']:
            install_dependencies(payloads['dependencies'])
        response = execute_script(func_id)
        if response is not False:
            logger.info("Got successful response")
            linked_func = payloads['linked_func']
            print("----------------------",response)
            payloads["scraped_data"] = response
            app = create_app()
            with app.app_context():
                db_data = DataAction.query.filter_by(func_id=func_id).first()
                db_data.current_state = "DATA_AGGREGATION"
                db.session.commit()
            client.set(func_id,json.dumps(payloads))
            return True
        else:
            logger.error("Scrapper returned error")
            return False
    else:
        logger.error(f"func id {func_id} has no payload")
        return False


        
        



def install_dependencies(deps):
    try:
        print("INSTALLING DEPENDENCIES---------------")
        for dep in deps:
            subprocess.check_call([sys.executable, "-m", "pip","install", dep])
        return True
    except Exception as e:
        print(e)
        return False



def execute_script(file_name):

    print("EXECUTING SCRIPT---------------")
    
    file_name = file_name.split('.')[0]
    modl = importlib.import_module(f"Scripts.{file_name}")
    try:
        result = modl.main()
        return result
    except Exception as e:
        print(e)
        return False



'''
Redis payload;
func_id: {
    "dependencies":[],
    "file_name":"string",
    "scraped_data":func_id,
    "linked_func":[array of func_ids],
    "curr_state":"Data Scrapping"
}
'''
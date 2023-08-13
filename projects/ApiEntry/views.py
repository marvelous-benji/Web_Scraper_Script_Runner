

from flask import  request,jsonify
import json
from werkzeug.utils import secure_filename
from threading import Thread
from config import basedir
import os.path
from . import Entry
from projects import db

from ..helpers import get_redis_client
from ..scrapper import run_scrapper
from ..models import  User, DataAction

from flask_jwt_extended import jwt_required


def save_file(file,filename):
    try:
        return file.save(basedir,filename)
    except Exception as e:
        print(e)
        return



@Entry.route("/receive_file",methods=["POST"])
@jwt_required()
def receive_payload():
    try:
        print("-------------",request.files,basedir)
        if 'file' not in request.files:
            return jsonify({"status":"failed","msg":"No file found"}),400
        file = request.files['file']
        if not file.filename:
            return jsonify({"status":"failed","msg":"Filename cannot be empty"}),400
        filename = secure_filename(file.filename)
        cld = get_redis_client()
        if cld is None:
            return jsonify({"status":"failed","msg":"Service currently unavailable"}),502
        file.save(os.path.join(f"{basedir}/Scripts",filename))
        payload = {}
        cld.set(filename,json.dumps(payload))
        print(json.loads(cld.get(filename)))
        return jsonify({"status":"success","msg":"File successfully received"}),200
    except Exception as e:
        print(e)
        return jsonify({"status":"failed","msg":"An unknown error occured"}),422
    



@Entry.route("/receive_protocol/<func_id>", methods=["PUT"])
@jwt_required()
def init_func_protocol(func_id):
    try:
        print("Got here-----------")
        cld = get_redis_client()
        if cld is None:
            return jsonify({"status":"failed","msg":"Service currently unavailable"}),502
        data = request.get_json()
        dependencies = data.get("dependencies",[])
        file_name = data.get("filename",func_id)
        if os.path.isfile(f"{basedir}/Scripts/{file_name}") is False:
            return jsonify({"status":"failed","msg":"File does not exist"}),400
        linked_func = data.get("linked_func",[])
        payload = json.loads(cld.get(func_id))
        if payload is None:
            return jsonify({"status":"failed","msg":"An error occured"}),500
        payload['dependencies'] = dependencies
        payload['linked_func'] = linked_func
        category = data['category']
        is_main = not linked_func
        db_record = DataAction.query.filter_by(func_id=func_id).first()
        if db_record is None:
            db_data = DataAction(func_id=file_name,is_being_processed=True,
                                current_state="DATA_SCRAPPING",is_main=is_main,
                                data_category=category)
            db.session.add(db_data)
            db.session.commit()
        cld.set(func_id,json.dumps(payload))
        return jsonify({"status":"success","msg":"Protocol successfully updated"}),200
    except Exception as e:
        print(e)
        return jsonify({"status":"failed","msg":"An unknown error occured"}),422
        


@Entry.route("/trigger_function/<func_id>", methods=["PUT"])
@jwt_required()
def init_scrapper(func_id):
    try:
        cld = get_redis_client()
        if cld is None:
            return jsonify({"status":"failed","msg":"Service currently unavailable"}),502
        if json.loads(cld.get(func_id)) is None:
            return jsonify({"status":"failed","msg":f"func_id: {func_id} does not exist"}), 404
        thr = Thread(target=run_scrapper, args=[func_id])
        thr.start()
        return jsonify({"status":"success","msg":"Request initiated successfully"}),200
    except Exception as e:
        print(e)
        return jsonify({"status":"failed","msg":"An error occured"}),500
    




@Entry.route("/check_state/<func_id>", methods=["GET"])
@jwt_required()
def get_current_state(func_id):
    try:
        #cld = get_redis_client()
        #if cld is None:
        #    return jsonify({"status":"failed","msg":"Service currently unavailable"}),502
        #payload = json.loads(cld.get(func_id))
        #if payload is None:
        #    return jsonify({"status":"failed","msg":f"func_id: {func_id} does not exist"}), 404
        #state = payload['curr_state']
        data = DataAction.query.filter_by(func_id=func_id).first()
        if data is None:
            return jsonify({"status":"failed","msg":"No record found for the requested func_id"}),404
        return jsonify({"status":"success","current_state":data.current_state,"comment":data.comment}),200
    except Exception as e:
        print(e)
        return jsonify({"status":"failed","msg":"An error occured"}),500
        

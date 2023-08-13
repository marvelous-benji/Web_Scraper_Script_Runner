from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from flask_jwt_extended import JWTManager
from flask_bcrypt import Bcrypt
from celery import Celery

from config import configs


jwt = JWTManager()
db = SQLAlchemy()
bcrypt = Bcrypt()
#celery = Celery()


def create_app(config_name='development'):
    
    app = Flask(__name__)
    app.config.from_object(configs[config_name])
    jwt.init_app(app)
    db.init_app(app)
    bcrypt.init_app(app)
    #celery.config_from_object(app.config)
    from .Auth import auth as auth_blueprint
    from .ApiEntry import Entry as entry_blueprint
    app.register_blueprint(auth_blueprint,url_prefix="/api/v1/auth")
    app.register_blueprint(entry_blueprint, url_prefix="/api/v1/entry")

    return app
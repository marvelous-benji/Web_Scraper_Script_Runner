import os
import json
from datetime import timedelta


basedir = os.path.abspath(os.path.dirname(__name__)) # Sets application's base directory


def get_env(variable):
    '''
    A custom function that provides
    alternative lookup for enviroment variables
    '''

    try:
        return os.environ[variable]
    except KeyError:

        def get_from_file(variable):
            with open("configs.json", "r") as f:
                env_file = json.load(f)
            return env_file[variable]

        return get_from_file(variable)


class BaseConfig:
    '''
    Base configurations common to all
    the development environments
    '''

    SECRET_KEY = get_env("SECRET_KEY")
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    JSON_SORT_KEYS = False
    BROKER_URL='redis://127.0.0.1:6379/0',
    RESULT_BACKEND='redis://127.0.0.1:6379/0'


class DevelopmentConfig(BaseConfig):
    '''
    Convigurations for local environment
    '''

    DEBUG = True
    SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(
        basedir, get_env("DB_DEV_URL")
    )
    JWT_ACCESS_TOKEN_EXPIRES = False
    #JWT_ACCESS_TOKEN_EXPIRES = timedelta(hours=20)
    JWT_CREATE_TOKEN_EXPIRES = timedelta(hours=20)




configs = {
    "development": DevelopmentConfig
}
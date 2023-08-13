from celery import Celery, Task
from projects import create_app, db
from .models import DataAction
from celery import chain
from .aggregator import aggregate_data
from .writer import write_to_excel
from .uploader import upload_to_S3

app = create_app()




def celery_init_app(app=app):
    class FlaskTask(Task):
        def __call__(self, *args: object, **kwargs: object):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery_app = Celery(app.name, task_cls=FlaskTask)
    celery_app.config_from_object(app.config)
    celery_app.set_default()
    app.extensions["celery"] = celery_app
    return celery_app


App = celery_init_app()



App.conf.beat_schedule = {
    'run_jobs': {
        'task': 'projects.worker.check_for_jobs',
        'schedule': 20
    },
}



@App.task
def check_for_jobs():
    try:
        results = DataAction.query.filter_by(is_main=True,is_being_processed=True).all()
        print('GETTING RESULTS--------------',results)
        for result in results:
            if result.current_state == 'DATA_AGGREGATION':
                aggregate_data.delay(result.func_id)
            elif result.current_state == 'WRITING_DATA':
                write_to_excel.delay(result.func_id)
            elif result.current_state == 'UPLOADING_DATA':
                upload_to_S3.delay(result.func_id)
            else:
                print("unkown state") # use logging here
        return True
    except Exception as e:
        print(e)
        return False


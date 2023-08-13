
from projects import create_app,db
from flask_migrate import Migrate
from projects.models import User,DataAction


app = create_app()

migrate = Migrate(app, db)

#db.create_all(app=app)


@app.shell_context_processor
def make_shell_processor():

    return dict(app=app, db=db, User=User, DataAction=DataAction)




if __name__ == '__main__':
    app.run(port=5000)
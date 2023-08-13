
import re
import uuid
from datetime import datetime

from sqlalchemy.orm import validates
from marshmallow import Schema, fields, validate

from projects import db, bcrypt



def hex_id():

    return uuid.uuid4().hex




class User(db.Model):

    __tablename__ = "users"
    id = db.Column(db.String(50), primary_key=True, index=True, default=hex_id)
    email = db.Column(db.String(80), nullable=False, unique=True, index=True)
    password = db.Column(db.String(140), nullable=False)
    date_created = db.Column(db.DateTime, default=datetime.utcnow)

    @validates("email")
    def validate_email(self, key, email):

        if not re.match("[^@]+@[^@]+\.[^@]+", email):
            raise AssertionError("Provided email is not a valid email address")
        return email

    def __repr__(self):

        return f"User('{self.id}','{self.email}')"

    @staticmethod
    def hash_password(password):
        return bcrypt.generate_password_hash(password)

    @staticmethod
    def verify_password_hash(password_hash, password):
        return bcrypt.check_password_hash(password_hash, password)





class UserSchema(Schema):

    class Meta:
        model = User
        sqla_session = db.session
        ordered = True

    id = fields.String(dump_only=True)
    email = fields.Email(required=True)
    password = fields.String(validate=validate.Length(min=7), load_only=True)
    date_created = fields.DateTime(dump_only=True)





class DataAction(db.Model):
    __tablename__="data_action"

    id = db.Column(db.String(50), primary_key=True, index=True, default=hex_id)
    func_id = db.Column(db.String(20), nullable=True, index=True, unique=True)
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    last_modified = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    current_state = db.Column(db.String(20), nullable=True,index=True)
    is_main = db.Column(db.Boolean, default=True)
    data_category = db.Column(db.String(20), nullable=True, index=True)
    comment = db.Column(db.Text)
    is_main_complete = db.Column(db.Boolean, default=False, index=True)
    is_being_processed = db.Column(db.Boolean, default=False, index=True)



    def __repr__(self):
    
        return f"DataAction('{self.id}','{self.func_id}')"


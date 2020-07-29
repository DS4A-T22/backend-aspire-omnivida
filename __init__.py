from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from flask_restful import Api, Resource
from sqlalchemy import Boolean, Column , ForeignKey
from sqlalchemy import DateTime, Integer, String, Text, Float
from sqlalchemy.orm import relationship

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:nxZFTdAzjL0TMwHX8TBT@ds4a-t22-omnivida.ch3i6mn5ftrs.sa-east-1.rds.amazonaws.com/omnivida'
db = SQLAlchemy(app)
ma = Marshmallow(app)
api = Api(app)


class Pacientes(db.Model):
    __tablename__='patients'
    id_patient = Column(Integer, primary_key=True)
    first_name = Column(String(100),nullable=False)
    last_name = Column(String(100),nullable=False)
    gender = Column(String(100),nullable=False)
    birthdate = Column(DateTime,nullable=False)
    civil_status=Column(String(100),nullable=False)

    def __repr__(self):
        #return '<Pacientes %s>' % self.civil_status
        return (u'<{self.__class__.__name__}: {self.id_patient}>'.format(self=self))

class PacientesSchema(ma.Schema):
    class Meta:
        fields = ("id_patient","first_name",
        "last_name", "gender","birthdate","civil_status" )
        ordered=True
        


patient_schema = PacientesSchema()
patients_schema = PacientesSchema(many=True)


class PacientesListResource(Resource):
    def get(self):
        patient = Pacientes.query.all()
        return patients_schema.dump(patient)


class PacientesResource(Resource):
    def get(self, id_patient):
        patient = Pacientes.query.filter_by(id_patient=id_patient)
        return patients_schema.dump(patient)

api.add_resource(PacientesListResource, '/patients')
api.add_resource(PacientesResource, '/patients/<int:id_patient>')


if __name__ == '__main__':
    app.run(debug=True)

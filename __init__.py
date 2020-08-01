from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from flask_restful import Api, Resource
from sqlalchemy import Boolean, Column , ForeignKey
from sqlalchemy import DateTime, Integer, String, Text, Float
from sqlalchemy.sql import select
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from stdata import StData

import pandas as pd
import numpy as np

db_connect = create_engine(    
        "postgresql://postgres:nxZFTdAzjL0TMwHX8TBT@ds4a-t22-omnivida.ch3i6mn5ftrs.sa-east-1.rds.amazonaws.com/omnivida",
        isolation_level="READ UNCOMMITTED") #La ruta depende de donde tengas almacenada la base de datos

conn = db_connect.connect() 

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

class PredictResource(Resource):
    def get(self, id_patient):
        st=StData()
        #BASIC_INFO=  pd.read_sql_query("SELECT * from patients", conn)
        BASIC_INFO = pd.read_sql_query('SELECT * from patients where id_patient='+str(id_patient), conn)
        basic_inf= st.get_basic_info_dataset(BASIC_INFO)
        datab=basic_inf.loc[:,['education','socioeconomic_level']]

        ADHERENCE = pd.read_sql_query('SELECT * from adherence where id_patient='+str(id_patient), conn)
        #ADHERENCE = pd.read_sql_query('SELECT * from adherence ', conn)
        adh= st.get_adherence_dataset(ADHERENCE)
        datah=adh.loc[:, ['days_since_last_control', 'ongoing_adherence_percentage','social_stratum']]

        FAMILY_RECORD = pd.read_sql_query('SELECT * from family_records where id_patient='+str(id_patient), conn)
        #FAMILY_RECORD = pd.read_sql_query('SELECT * from family_records', conn)
        family_rec= st.get_family_records_dataset(FAMILY_RECORD)

        WELLBEING_INDEX = pd.read_sql_query('SELECT * from well_being_records where id_patient='+str(id_patient), conn)
        #WELLBEING_INDEX = pd.read_sql_query('SELECT * from well_being_records', conn)
        
        wellb= st.get_wellbeing_index_dataset(WELLBEING_INDEX)


        #age_at_survey_date=    
        return "{}"

 
api.add_resource(PredictResource, '/patient/predict/<int:id_patient>')
api.add_resource(PacientesListResource, '/patients')
api.add_resource(PacientesResource, '/patients/<int:id_patient>')


if __name__ == '__main__':
    app.run(debug=True)

import sys
import os
sys.path.append("../")
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
import json
import pickle
import omnivida_util as ovu

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
humanistic_model = pickle.load(open('./models/human_xgboost.pkl','rb'))
respiratory_related_diagnosis = ['ASMA MIXTA', 'ASMA NO ALERGICA', 'ASMA, NO ESPECIFICADA', 
                                        'CARCINOMA IN SITU DEL BRONQUIO Y DEL PULMON', 'ENFERMEDAD PULMONAR OBSTRUCTIVA CRONICA, NO ESPECIFICADA']
period = 30
humanistic_covariates = ['days_since_last_control', 'ongoing_adherence_percentage', 'education', 'social_stratum', 'socioeconomic_level', 
                        'age_at_survey_date', 'family_respiratory_related_diagnosis', 'family_non_respiratory_related_diagnosis', 
                        'days_since_last_wb_survey', 'num_wb_reports_last_30_days', 'wb_score', 'gender_F', 'gender_M',
                        'civil_status_CASADO (A)', 'civil_status_SEPARADO (A)', 'civil_status_SIN DEFINIR', 'civil_status_SOLTERO (A)',
                        'civil_status_UNIÓN LIBRE', 'civil_status_VIUDO (A)', 'occupation_AMA DE CASA', 'occupation_DESEMPLEADO',
                        'occupation_EMPLEADO', 'occupation_ESTUDIANTE', 'occupation_INDEPENDIENTE', 'occupation_JUBILADO',
                        'occupation_PENSIONADO', 'occupation_SIN DEFINIR']

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
        if adh.empty:
            return {
                'error': 500,
                'message': 'There is no adherence records available for this patient'
            }
        datah=adh.loc[:, ['days_since_last_control', 'ongoing_adherence_percentage']]

        FAMILY_RECORD = pd.read_sql_query('SELECT * from family_records where id_patient='+str(id_patient), conn)
        #FAMILY_RECORD = pd.read_sql_query('SELECT * from family_records', conn)
        family_rec= st.get_family_records_dataset(FAMILY_RECORD)

        WELLBEING_INDEX = pd.read_sql_query('SELECT * from well_being_records where id_patient='+str(id_patient), conn)
        #WELLBEING_INDEX = pd.read_sql_query('SELECT * from well_being_records', conn)
        
        wellb= st.get_wellbeing_index_dataset(WELLBEING_INDEX)

        predictable_data = self.prepare_data_for_humanistic_model(id_patient, adh, basic_inf, family_rec, wellb)
        
        #age_at_survey_date=    
        prediction = 'ADHERENT' if humanistic_model.predict(predictable_data)[-1] == 0 else 'NON-ADHERENT'
        prediction_proba = 1 - humanistic_model.predict_proba(predictable_data)[-1][1]
        res = {
            'id_patient': id_patient,
            'prediction': prediction, 
            'probability': prediction_proba
        }
        return res
    
    def prepare_data_for_humanistic_model(self, id_patient, adherence_df, basic_info_df, family_records_df, well_being_df):
        select_fields = ['id_patient', 'survey_date', 'qualitative_result', 'qualitative_result_change', 'days_since_last_control', 'ongoing_adherence_percentage', 'num_reports']
        adherence_change_analysis = adherence_df[select_fields]

        bi_adherence = adherence_change_analysis.merge(basic_info_df, how='left', on='id_patient')
        bi_adherence['age_at_survey_date'] = (np.ceil((bi_adherence['survey_date'] - bi_adherence['birthdate']) / np.timedelta64(1, 'Y'))).astype(int)
        bi_adherence.drop(columns=['city', 'department'], axis=1, inplace=True)

        consolidated_family_record = pd.DataFrame()
        row = {}
        if not family_records_df.empty:
            consolidated_family_record = pd.DataFrame()
            temp_df = family_records_df.copy()
            row = {}
            row['id_patient'] = int(id_patient)
            row['family_history_reported'] = 1
            row['num_family_records'] = temp_df.shape[0]
            row['family_respiratory_related_diagnosis'] = len(temp_df[temp_df.diagnosis.isin(respiratory_related_diagnosis)])
            row['family_non_respiratory_related_diagnosis'] = len(temp_df[~temp_df.diagnosis.isin(respiratory_related_diagnosis)])
            consolidated_family_record = consolidated_family_record.append(row, ignore_index=True)
        else:
            row = {}
            row['id_patient'] = int(id_patient)
            row['family_history_reported'] = 0
            row['num_family_records'] = 0
            row['family_respiratory_related_diagnosis'] = 0
            row['family_non_respiratory_related_diagnosis'] = 0
            consolidated_family_record = consolidated_family_record.append(row, ignore_index=True)

        ## Rearrange columns
        consolidated_family_record = consolidated_family_record[
            ['id_patient', 'family_respiratory_related_diagnosis', 'family_non_respiratory_related_diagnosis']
        ]
        # Merge Adherence, Basic info data and Family records data

        bi_fr_adherence = bi_adherence.merge(consolidated_family_record, how='left', on='id_patient')
        bi_fr_adherence['family_respiratory_related_diagnosis'] = bi_fr_adherence['family_respiratory_related_diagnosis'].fillna(0)
        bi_fr_adherence['family_non_respiratory_related_diagnosis'] = bi_fr_adherence['family_non_respiratory_related_diagnosis'].fillna(0)

        aclq_timely_summary = pd.DataFrame()
        if not family_records_df.empty:
            pivoted_life_quality = well_being_df.pivot(index='creation_date', columns='dimension', values='score')
            pivoted_life_quality.columns = pivoted_life_quality.columns.categories
            pivoted_life_quality.reset_index(inplace=True)
            pivoted_life_quality['id_patient'] = id_patient
            cols = [list(pivoted_life_quality.columns)[-1]] + list(pivoted_life_quality.columns)[:-1]
            pivoted_life_quality = pivoted_life_quality[cols]
            # print(pivoted_life_quality)

            pivoted_life_quality.columns = ['id_patient', 'creation_date', 'personal_environment', 'psychological_health', 'interpersonal_relationships', 'physical_health']
            pivoted_life_quality['wb_score'] = (pivoted_life_quality['personal_environment'] + \
                    pivoted_life_quality['psychological_health'] + \
                    pivoted_life_quality['interpersonal_relationships'] + \
                    pivoted_life_quality['physical_health']) / 4.

            adherence_change_life_quality = ovu.merge_on_closest_date(df1=adherence_change_analysis, df2=pivoted_life_quality, date_field_df1='survey_date', date_field_df2='creation_date', merge_on='id_patient')
            adherence_change_life_quality.rename(columns={'days_since_creation_date': 'days_since_last_well_being_survey'}, inplace=True)

            aclq_timely = adherence_change_life_quality[adherence_change_life_quality.days_since_last_well_being_survey <= period]
            # aclq_late = adherence_change_life_quality[adherence_change_life_quality.days_since_last_well_being_survey > period]

            for survey_date, df in aclq_timely.groupby('survey_date'):
                aclq_timely_summary = aclq_timely_summary.append({
                    'id_patient': id_patient,
                    'survey_date': survey_date,
                    'num_wb_reports_last_30_days': df.shape[0],
                    'wb_score': df.iloc[-1]['wb_score'],
                    'days_since_last_wb_survey': df.iloc[-1]['days_since_last_well_being_survey']
                }, ignore_index=True)
        else:
            for survey_date, df in bi_fr_adherence.groupby('survey_date'):
                aclq_timely_summary = aclq_timely_summary.append({
                    'id_patient': id_patient,
                    'survey_date': survey_date,
                    'num_wb_reports_last_30_days': np.nan,
                    'wb_score': np.nan,
                    'days_since_last_wb_survey': np.nan
                }, ignore_index=True)

        # Merge Adherence, Basic info data, Family records and Well-being data

        bi_fr_wb_adherence = bi_fr_adherence.merge(aclq_timely_summary, how='left', on=['id_patient', 'survey_date'])
        bi_fr_wb_adherence['days_since_last_wb_survey'] = bi_fr_wb_adherence['days_since_last_wb_survey'].fillna(0)
        bi_fr_wb_adherence['days_since_last_control'] = bi_fr_wb_adherence['days_since_last_control'].fillna(0)
        bi_fr_wb_adherence['num_wb_reports_last_30_days'] = bi_fr_wb_adherence['num_wb_reports_last_30_days'].fillna(0)
        bi_fr_wb_adherence['wb_score'] = bi_fr_wb_adherence['wb_score'].fillna(0)
        bi_fr_wb_adherence.drop(columns=['qualitative_result_change', 'social_security_regime', 'zone', 'social_security_affiliation_type', 'employment_type'], axis=1, inplace=True)

        bi_fr_wb_adherence_modelable = bi_fr_wb_adherence.copy()
        bi_fr_wb_adherence_modelable = pd.get_dummies(bi_fr_wb_adherence_modelable, columns=['gender', 'civil_status', 'occupation'])
        bi_fr_wb_adherence_modelable = bi_fr_wb_adherence_modelable[humanistic_covariates]
        return bi_fr_wb_adherence_modelable


 
api.add_resource(PredictResource, '/patient/predict/<int:id_patient>')
api.add_resource(PacientesListResource, '/patients')
api.add_resource(PacientesResource, '/patients/<int:id_patient>')


if __name__ == '__main__':
    app.run(debug=True)

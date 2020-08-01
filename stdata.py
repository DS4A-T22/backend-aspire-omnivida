# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

class StData():
    @classmethod
    def get_family_records_dataset(self,familiar_records):
        # set datatype and explicit category sorting for categorical data
        familiar_records['health_provider'] = familiar_records['health_provider'].astype('category')
        familiar_records['diagnosis'] = familiar_records['diagnosis'].astype('category')
        familiar_records['diagnosis_code'] = familiar_records['diagnosis_code'].astype('category')
        familiar_records['relationship'] = familiar_records['relationship'].astype('category')
        familiar_records['creation_date'] = pd.to_datetime(familiar_records['creation_date'])
        print("familiar records")
        return familiar_records
    @classmethod
    def get_wellbeing_index_dataset(self,life_quality):
        # set datatype and explicit category sorting for categorical data
        life_quality['dimension'] = life_quality['dimension'].astype('category')
        life_quality['creation_date'] = pd.to_datetime(life_quality['creation_date'])
        return life_quality
    
    @classmethod
    def get_adherence_dataset(self,adherence ):
        adherence['survey_date'] = pd.to_datetime(adherence['survey_date'])
        adherence.loc[adherence['quantitative_result']=='<30%', 'quantitative_result'] = 0
        adherence.loc[adherence['quantitative_result']=='30-65%', 'quantitative_result'] = 1
        adherence.loc[adherence['quantitative_result']=='64-84%', 'quantitative_result'] = 2
        adherence.loc[adherence['quantitative_result']=='85-94%', 'quantitative_result'] = 3
        adherence.loc[adherence['quantitative_result']=='95-100%', 'quantitative_result'] = 4
        adherence.loc[adherence['smaq2']=='<30%', 'smaq2'] = 0
        adherence.loc[adherence['smaq2']=='30-65%', 'smaq2'] = 1
        adherence.loc[adherence['smaq2']=='64-84%', 'smaq2'] = 2
        adherence.loc[adherence['smaq2']=='85-94%', 'smaq2'] = 3
        adherence.loc[adherence['smaq2']=='95-100%', 'smaq2'] = 4
        adherence.loc[adherence['morisky_green']=='si', 'morisky_green'] = 1
        adherence.loc[adherence['morisky_green']=='no', 'morisky_green'] = 0
        adherence.loc[adherence['smaq1']=='si', 'smaq1'] = 1
        adherence.loc[adherence['smaq1']=='no', 'smaq1'] = 0
        adherence.loc[adherence['espa']=='si', 'espa'] = 1
        adherence.loc[adherence['espa']=='no', 'espa'] = 0
        adherence.loc[adherence['qualitative_result']=='si', 'qualitative_result'] = 1
        adherence.loc[adherence['qualitative_result']=='no', 'qualitative_result'] = 0
        adherence['smaq1'] = adherence['smaq1'].astype('float') 
        adherence['smaq2'] = adherence['smaq2'].astype('float') 
        adherence['morisky_green'] = adherence['morisky_green'].astype('float') 
        adherence['espa'] = adherence['espa'].astype('float') 
        adherence['nm_espa'] = adherence['nm_espa'].astype('float') 
        adherence['qualitative_result'] = adherence['qualitative_result'].astype('float') 
        adherence['quantitative_result'] = adherence['quantitative_result'].astype('float') 
        
#         adherence_s= adherence[adherence.quantitative_result < 4]
#         adherence_n= adherence[adherence.quantitative_result == 4]
#         Todos=adherence['id_patient'].unique()                  #List of all of the ids.
#         id_list_s_not=adherence_s['id_patient'].unique() #List of id's that are not 'siempre'
#         ads = adherence[~adherence['id_patient'].isin(id_list_s_not)].copy()  #ads : Dataframe of id's that are always adherent.
#         id_list_n_not=adherence_n['id_patient'].unique() #List of id's that are not 'nunca'
#         adn = adherence[~adherence['id_patient'].isin(id_list_n_not)].copy()  #adn : Dataframe of id's that never were adherent.

#         Siempre_ad=ads['id_patient'].unique()            #List of id's that are 'siempre'
#         Nunca_ad=adn['id_patient'].unique()              #List of id's that are 'nunca'

#         Intermitentes = [x for x in Todos if x not in Siempre_ad]
#         Intermitentes = [x for x in Intermitentes if x not in Nunca_ad]
#         Intermitentes = np.array(Intermitentes)  #Intermitentes: list of id's that are 'Intermitent'

#         Todosmenosint=[x for x in Todos if x not in Intermitentes] 
#         Todosmenosint = np.array(Todosmenosint)  #List of id's that are 'Intermitent'

#         adi = adherence[~adherence['id_patient'].isin(Todosmenosint)].copy()  #adi: Dataframe of id's that has intermitent adherence.
#         adi['quantitative_result'] = adi['quantitative_result'].astype('int64')
#         middle_group_scores = adi.groupby('id_patient').quantitative_result.mean().reset_index()  #Sortearlo por el promedio.
#         intermediate_adherence_thresholds = adi.groupby('id_patient').quantitative_result.mean().quantile([0.25, 0.75])
#         scores_t75 = intermediate_adherence_thresholds[0.75]
#         scores_t25 = intermediate_adherence_thresholds[0.25]
#         middle_group_scores["category"] = np.where(middle_group_scores['quantitative_result'] > scores_t75, 'A-', 
#         (np.where(middle_group_scores['quantitative_result'] < scores_t25, 'N+', 'M')))
#         adi = adi.merge(middle_group_scores[['id_patient', 'category']], how='left')
#         ads['category']='A'
#         adn['category']='N'
#         adherence = pd.concat([ads, adi, adn], ignore_index=True).sort_values(by=['id_patient', 'survey_date']).reset_index(drop=True)
    
#         adherence['category'] = adherence['category'].astype('category')
#         adherence['category'].cat.reorder_categories(['N', 'N+', 'M', 'A-', 'A'], ordered=True, inplace=True)
        
        adherence_change = pd.DataFrame()
        for paciente, df in adherence.groupby('id_patient'):
            temp_df = df.copy().reset_index(drop=True)
            temp_df['morisky_change'] = temp_df['morisky_green'].diff()
            temp_df['smaq1_change'] = temp_df['smaq1'].diff()
            temp_df['smaq2_change'] = temp_df['smaq2'].diff()
            temp_df['espa_change'] = temp_df['espa'].diff()
            temp_df['nm_espa_change'] = temp_df['nm_espa'].diff()
            temp_df['qualitative_result_change'] = temp_df['qualitative_result'].diff()
            temp_df['quantitative_result_change'] = temp_df['quantitative_result'].diff()
            temp_df['days_since_last_control'] = temp_df['survey_date'].diff() / np.timedelta64(1, 'D')
            temp_df['num_reports'] = temp_df.index + 1
            temp_df['ongoing_adherence_percentage'] = 100*(temp_df['qualitative_result'].cumsum()/(temp_df.index+1))
            adherence_change = adherence_change.append(temp_df, ignore_index=True)
        
        return adherence_change



    @classmethod 
    def get_basic_info_dataset(self,basic_info):
        education_codes = {
        'ANALFABETA': 0,
        'EDAD PREESCOLAR': 1,
        'PRIMARIA': 2,
        'SECUNDARIA': 3,
        'TECNICO': 4,
        'TECNOLOGO': 5,
        'UNIVERSITARIO': 6,
        'POSGRADO': 7
        }
        socioeconomic_level_codes = {
        'NIVEL 0 DEL SISBEN': 0,
        'NIVEL 1 DEL SISBEN': 1,
        'NIVEL 2 DEL SISBEN': 2,
        'A': 3,
        'B': 4,
        'C': 5
        }
        
#         gender_codes={
#             'M':0,
#             'F':1
#         }

#         if basic_info['gender'][0] in gender_codes:
#             a=basic_info['gender'][0]
#             basic_info.at[0,'gender'] =gender_codes[a]
#             print (basic_info['gender'])
#         else:
#             print ('No hay traducción para el género')

        if basic_info['education'][0] in education_codes:
            a=basic_info['education'][0]
            basic_info.at[0,'education'] =education_codes[a]
            print("educación")
            print (basic_info['education'])
        else:
            basic_info['education'] = 3
            print ('No hay traducción para la educación')

        if basic_info['socioeconomic_level'][0] in socioeconomic_level_codes:
            a=basic_info['socioeconomic_level'][0]
            basic_info.at[0,'socioeconomic_level'] =socioeconomic_level_codes[a]
            print("socioeconomic_level")
            print (basic_info['socioeconomic_level'])
        else:
            basic_info['socioeconomic_level'] = 3
            print ('No hay traducción para socioeconomic_level_codes')
    # set datatype and explicit category sorting for categorical data
        basic_info['education'] = basic_info['education'].astype(int)
        basic_info['socioeconomic_level'] = basic_info['socioeconomic_level'].astype(int)
        basic_info['gender'] = basic_info['civil_status'].astype('category')
        basic_info['gender'] = basic_info['gender'].cat.set_categories(['M', 'F'])
        basic_info['civil_status'] = basic_info['civil_status'].astype('category')
        basic_info['civil_status'] = basic_info['civil_status'].cat.set_categories(['CASADO (A)', \
                                        'SEPARADO (A)', 'SIN DEFINIR', 'SOLTERO (A)', 'UNIÓN LIBRE', \
                                        'VIUDO (A)'])
#         basic_info['zone'] = basic_info['zone'].astype('category')
        basic_info['occupation'] = basic_info['occupation'].astype('category')
        basic_info['occupation'] = basic_info['occupation'].cat.set_categories(['AMA DE CASA', \
                                        'DESEMPLEADO', 'EMPLEADO', 'ESTUDIANTE', 'INDEPENDIENTE', \
                                        'JUBILADO', 'PENSIONADO', 'SIN DEFINIR'])
        
#         basic_info['social_security_regime'] = basic_info['social_security_regime'].astype('category')
#         basic_info['social_security_affiliation_type'] = basic_info['social_security_affiliation_type'].astype('category')
#         basic_info['employment_type'] = basic_info['employment_type'].astype('category')
        basic_info['birthdate'] = pd.to_datetime(basic_info['birthdate'])
        print("creó información básica")
        return basic_info

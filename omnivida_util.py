import pandas as pd
import numpy as np

def find_closest_date(df1_row, date_field_df1, df2, date_field_df2, filter_on='id_patient', add_time_delta_column=True):
    filter_cond = (df2[filter_on]==df1_row[filter_on]) & (df2[date_field_df2] <= df1_row[date_field_df1])
    deltas = ((df1_row[date_field_df1] - df2[filter_cond][date_field_df2]) / np.timedelta64(1, 'D')).reset_index(drop=True)
#     deltas_abs = np.abs(deltas)
#     deltas = deltas[deltas>=0]
#     print(f"\n\nADHERENCE ROW: \n{df1_row}")
#     print(f"\nDELTAS: \n{deltas}")
    if not deltas.empty:
        idx_closest_date = np.argmin(deltas)
#         print(f"\nCLOSEST VALUE: \n{df2[filter_cond].iloc[idx_closest_date]}")
        closest_date = df2[filter_cond].iloc[idx_closest_date][date_field_df2]
        res = {"closest_date": closest_date}
        idx = ['closest_date']
        if add_time_delta_column:
            res["closest_delta"] = deltas[idx_closest_date] * (-1.0 if closest_date > df1_row[date_field_df1] else 1.0)
            idx.append('closest_delta')
    else: 
        res = {"closest_date": np.nan, "closest_delta": np.nan}
        idx = ['closest_date', 'closest_delta']
    return pd.Series(res, index=idx)

def merge_on_closest_date(df1, df2, date_field_df1, date_field_df2, merge_on='id_patient'):
    temp_df1 = df1.copy()
    # if df2[date_field_df2].dtype == 'object':
    df2[date_field_df2]=pd.to_datetime(df2[date_field_df2])
    temp_df1[date_field_df1]=pd.to_datetime(df1[date_field_df1])
    temp_df1[[f'closest_{date_field_df2}', f'days_since_{date_field_df2}']] = temp_df1.apply(
                                          find_closest_date, args=[date_field_df1, df2, date_field_df2, merge_on], axis=1)
    
    result_df = pd.merge(temp_df1, df2, left_on=[merge_on, f'closest_{date_field_df2}'], right_on=[merge_on, date_field_df2])
    return result_df.drop(columns=f'closest_{date_field_df2}', axis=1)

def compute_transition_probabilities(df):
    pos_transition = df[df['qualitative_result_change']==1]
    neg_transition = df[df['qualitative_result_change']==-1]
    num_pos_transitions = len(pos_transition)
    num_neg_transitions = len(neg_transition)
    num_no_trans_no_adherent = len(df[(df['qualitative_result'] == 0) & (df['qualitative_result_change'] == 0)])
    num_no_trans_adherent = len(df[(df['qualitative_result'] == 1) & (df['qualitative_result_change'] == 0)])
    pat = num_pos_transitions / (num_pos_transitions + num_no_trans_no_adherent)
    pnt = num_neg_transitions / (num_neg_transitions + num_no_trans_adherent)
    pt = (num_pos_transitions + num_neg_transitions) / (num_neg_transitions + num_no_trans_adherent + num_pos_transitions + num_no_trans_no_adherent)
    return [pat, pnt, pt]

def compute_transition_probabilitie4DFs(df_dict):
    return pd.DataFrame(index=df_dict.keys(), columns=['P(N-A)', 'P(A-N)', 'P(Transition)'], \
                        data=[compute_transition_probabilities(df) for df in df_dict.values()])

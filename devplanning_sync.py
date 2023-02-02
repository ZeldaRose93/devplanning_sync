# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 14:47:58 2022

@author: zrose
"""

# Third party packages
import matplotlib.pyplot as plt
import numpy as np
import openpyxl
import pandas as pd
import pyodbc

# Zelda's Written functions
from devplanning_sync_functions import compare_columns
from devplanning_sync_functions import compare_numeric_columns
from devplanning_sync_functions import connect_to_snowflake
from devplanning_sync_functions import hierarchical_select
from devplanning_sync_functions import match


# Supresses error messages for valid pandas operations.
pd.options.mode.chained_assignment = None


conn = connect_to_snowflake()

snowflake_string = """
SELECT
*
FROM
SOURCE.GIS.DEV_PLANNING AS DP
WHERE (DP.BUSINESS_UNIT = 'BRAZOS VALLEY' OR DP.BUSINESS_UNIT = 'SOUTH TEXAS') AND DP.SCENARIO IN ('A', 'MDV') AND DP.DEV_STATUS IN ('PRIMARY', 'DEVELOPMENT')
"""

dev_planning = pd.read_sql(snowflake_string, conn)
conn.close()

#####     End connect to snowflake database     #####


#####     Connect to Aries Working District     #####

conn = pyodbc.connect(r'DRIVER={ODBC Driver 17 for SQL Server}; uid={zdrane}; server={Aries-prod}; Database={Working_District}; Trusted_Connection=yes')

# Write Query string from SQL
aries_string = """
SELECT
	M.ARIES_CODE,
    M.RSV_CAT,
    M.PROP_NUM,
    M.PRESPUDWELLID,
	M.USER3,
    M.LEASE,
    M.SPUDDER_DATE,
    M.FIRST_PROD,
    M.PAD_NAME,
    M.LAT_SURFACE,
    M.LONG_SURFACE,
    M.LAT_TARGET,
    M.LONG_TARGET,
    M.LAT_BH,
    M.LONG_BH,
    B.PLANNED_SH_LAT,
    B.PLANNED_SH_LONG,
    B.PLANNED_TARGET_LAT,
    B.PLANNED_TARGET_LONG,
    B.PLANNED_BH_LAT,
    B.PLANNED_BH_LONG,
    M.LATERAL_LEN,
    B.PLANNED_LL,
    B.PROJECT_NAME,
    M.MDA,
    M.RESV_ENG,
    M.RESERVOIR,
    M.TYPECURVE,
    M.TYPECURVE_SHORT,
    M.TD_DATE,
    B.AFE_DATE
FROM [Working_District].[AriesAdmin].[AC_PROPERTY] AS M
INNER JOIN [Working_District].[AriesAdmin].[AC_BUDGET] AS B ON M.PROPNUM = B.PROPNUM
WHERE (M.BUSINESS_UNIT ='SOUTH TEXAS') AND M.RSV_CAT IN ('5PUD','5PUDX','6PROB','7POSS');
"""


aries = pd.read_sql(aries_string, conn)
conn.close()

#####     End Connect to Aries Working District     #####


#####     Check for active cases in Aries that aren't in DP and active DP that aren't in Aries     #####

dp_st = dev_planning.loc[dev_planning.BUSINESS_UNIT == 'SOUTH TEXAS']

combined_df = dp_st.merge(
    aries,
    left_on='ARIES_ID',
    right_on='ARIES_CODE',
    how='left',
    suffixes=['_DP', '_AR']
)
in_dp_not_aries = combined_df.loc[pd.isna(combined_df.ARIES_CODE)]
in_dp_not_aries = in_dp_not_aries[['ARIES_CODE', 'ARIES_ID',
                                   'WELL_NAME', 'LEASE',
                                   'RSV_CAT_DP', 'RSV_CAT_AR']]

combined_df = aries.merge(
    dp_st,
    left_on='ARIES_CODE',
    right_on='ARIES_ID',
    how='left',
    suffixes=['_AR', '_DP']
)

in_aries_not_dp = combined_df.loc[pd.isna(combined_df.ARIES_ID)]
in_aries_not_dp = in_aries_not_dp[['ARIES_CODE', 'ARIES_ID', 'WELL_NAME',
                                   'LEASE', 'RSV_CAT_DP', 'RSV_CAT_AR',
                                   'TD_DATE']]

in_aries_not_dp.loc[pd.isna(in_aries_not_dp.TD_DATE)]


#####     END Active cases check.     #####
combined_df.dropna(subset=['ARIES_ID'], inplace=True)

# Start checking individual columns.

psid_list = []
for index, row in combined_df.iterrows():
    psid_list.append(hierarchical_select(row.USER3,
                                         row.PRESPUDWELLID,
                                         type_to_coerce=int))

combined_df['PSID_AR'] = psid_list
combined_df.PSID_AR = combined_df.PSID_AR.astype('float64')

combined_df.rename(columns={'PSID': 'PSID_DP'}, inplace=True)

psid_check = compare_numeric_columns("PSID_DP",
                                     "PSID_AR",
                                     dataframe=combined_df,
                                     round_to=0)

pn_check = compare_columns('PROP_NUM_DP',
                           'PROP_NUM_AR',
                           dataframe=combined_df)
pn_check[['PROP_NUM_DP', 'PROP_NUM_AR', 'MATCH']]
print(pn_check.value_counts(['MATCH']))


lease_check = compare_columns('WELL_NAME', 'LEASE', dataframe=combined_df)
print(lease_check[['WELL_NAME', 'LEASE', 'MATCH']])
print(lease_check.MATCH.value_counts())

proj_name_check = compare_columns('PROJECT_NAME_DP',
                                  'PROJECT_NAME_AR',
                                  dataframe=combined_df)
print(proj_name_check[['PROJECT_NAME_DP', 'PROJECT_NAME_AR', 'MATCH']])
print(proj_name_check.MATCH.value_counts())

pad_name_check = compare_columns('PAD_NAME_DP',
                                 'PAD_NAME_AR',
                                 dataframe=combined_df)
print(pad_name_check[['PAD_NAME_DP', 'PAD_NAME_AR', 'MATCH']])
print(pad_name_check.MATCH.value_counts())

mda_check = compare_columns('MKT_DEDICATION_AREA',
                            'MDA',
                            dataframe=combined_df)
print(mda_check[['MKT_DEDICATION_AREA', 'MDA', 'MATCH']])
print(mda_check.MATCH.value_counts())


#            Start Checking Lat Longs          #
#            Start Checking Lat Longs          #
#            Start Checking Lat Longs          #


sl_lat_check = compare_numeric_columns('SL_LAT',
                                       'LAT_SURFACE',
                                       dataframe=combined_df,
                                       round_to=4)
print(sl_lat_check[['SL_LAT', 'LAT_SURFACE', 'DELTA', 'MATCH']])
print(sl_lat_check.value_counts('MATCH'))
print(sl_lat_check[sl_lat_check.MATCH == "UPDATE ARIES"])

sl_long_check = compare_numeric_columns('SL_LONG',
                                        'LONG_SURFACE',
                                        dataframe=combined_df,
                                        round_to=4)
print(sl_long_check.value_counts('MATCH'))

#    Start Target Hole checks     #

# In South Texas we used the first waypoint values if they existed and if not
# we would use the Landing point. This may be different in different BUs
dp_tp_lat_list = []
dp_tp_long_list = []

for index, row in combined_df.iterrows():
    dp_tp_lat_list.append(
        hierarchical_select(row.WAYPOINT1_LAT,
                            row.LP_LAT,
                            float
                            )
    )
    dp_tp_long_list.append(
        hierarchical_select(row.WAYPOINT1_LONG,
                            row.LP_LONG,
                            float
                            )
    )
dp_tp_lat_list
combined_df['TP_LAT'] = dp_tp_lat_list
combined_df['TP_LONG'] = dp_tp_long_list

print(combined_df[['WAYPOINT1_LAT', 'LP_LAT', 'TP_LAT']])
print(combined_df[['WAYPOINT1_LONG', 'LP_LONG', 'TP_LONG']])


th_lat_check = compare_numeric_columns('TP_LAT',
                                       'LAT_TARGET',
                                       dataframe=combined_df,
                                       round_to=4)
print(th_lat_check[['TP_LAT', 'LAT_TARGET', 'DELTA', 'MATCH']])
print(th_lat_check.MATCH.value_counts())

th_long_check = compare_numeric_columns('TP_LONG',
                                        'LONG_TARGET',
                                        dataframe=combined_df,
                                        round_to=4)
print(th_long_check[['TP_LONG', 'LONG_TARGET', 'DELTA', 'MATCH']])
print(th_long_check.MATCH.value_counts())

bh_lat_check = compare_numeric_columns('BHL_LAT',
                                       'LAT_BH',
                                       dataframe=combined_df,
                                       round_to=4)
print(bh_lat_check[['BHL_LAT', 'LAT_BH', 'DELTA', 'MATCH']])
print(bh_lat_check.MATCH.value_counts())

bh_long_check = compare_numeric_columns('BHL_LONG',
                                        'LONG_BH',
                                        dataframe=combined_df,
                                        round_to=4)
print(bh_long_check[['BHL_LONG', 'LONG_BH', 'DELTA', 'MATCH']])
print(bh_long_check.MATCH.value_counts())

# End Checking lat longs  #

m_lateral_len_check = compare_numeric_columns('COMPLETABLE_LL',
                                              'LATERAL_LEN',
                                              dataframe=combined_df,
                                              round_to=0)
b_lateral_len_check = compare_numeric_columns('COMPLETABLE_LL',
                                              'PLANNED_LL',
                                              dataframe=combined_df,
                                              round_to=0)
lat_len_check = m_lateral_len_check.merge(b_lateral_len_check,
                                          how='inner',
                                          on=['ARIES_CODE',
                                              'LEASE',
                                              'COMPLETABLE_LL'],
                                          suffixes=['_m', '_b']
                                          )

# Output backups to an excel file
with pd.ExcelWriter(r'.\output_spreadsheets\backups.xlsx') as writer:
    dp_st.to_excel(writer, sheet_name='DevPlanning_backup')
    aries.to_excel(writer, sheet_name='Aries_Backup')

with pd.ExcelWriter(r'.\output_spreadsheets\changes.xlsx') as writer:
    in_aries_not_dp.to_excel(writer, sheet_name='in_aries_not_dp')
    in_dp_not_aries.to_excel(writer, sheet_name='in_dp_not_aries')
    psid_check.to_excel(writer, sheet_name='PSID')
    pn_check.to_excel(writer, sheet_name='PROP_NUM')
    lease_check.to_excel(writer, sheet_name='LEASE')
    proj_name_check.to_excel(writer, sheet_name="PROJECT NAME")
    pad_name_check.to_excel(writer, sheet_name='PAD_NAME')
    sl_lat_check.to_excel(writer, sheet_name="SH_LAT")
    sl_long_check.to_excel(writer, sheet_name="SH_LONG")
    th_lat_check.to_excel(writer, sheet_name='TH_LAT')
    th_long_check.to_excel(writer, sheet_name='TH_LONG')
    bh_lat_check.to_excel(writer, sheet_name='BH_LAT')
    bh_long_check.to_excel(writer, sheet_name='BH_LONG')
    lat_len_check.to_excel(writer, sheet_name='Lateral Lengths')

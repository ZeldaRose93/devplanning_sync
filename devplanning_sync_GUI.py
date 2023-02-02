# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 07:59:45 2023

@author: zrose
"""

# Build a GUI for the devplanning sync script.
import os

# Import third party packages
import pandas as pd
import pyodbc
import sqlalchemy
import tkinter as tk

# Import Zelda written packages
# from devplanning_syn_GUI_functions import pull_data
from devplanning_syn_GUI_functions import _load_aries
from devplanning_syn_GUI_functions import _load_snowflake
from devplanning_sync_functions import compare_columns
from devplanning_sync_functions import compare_numeric_columns
from devplanning_sync_functions import hierarchical_select
from devplanning_sync_functions import update_table


def pull_data(business_unit):
    """
    This function will take the two functions from the devplanning_sync_GUI\
        functions.py script and combine them into one. It is necessary to
        build this function in this script to ensure the dataframes are
        properly scoped to this section's global scope and namespace.

    Args:
        business_unit: The name of the business unit you are updating.

    Returns: None
    """
    global dev_planning
    global aries
    try:
        dev_planning = _load_snowflake(business_unit)
        aries = _load_aries(business_unit)

        aries_len = len(aries)
        dp_len = len(dev_planning)
        tk.messagebox.showinfo("Connection Status", f"""
                               Connection Success!
                               {aries_len} rows imported from Working District
                               {dp_len} rows imported from Dev Planning
                               """)
    except:
        tk.messagebox.showinfo("Connection Status", "Connection Failed")


def run_checks():
    """
    This procedure runs all the checks and comparisons between the Aries
    and DevPlanning columns.

    It starts by setting up a combined dataframe and checking which values
    are not in both dataframes.

    From here it goes column by column and compares them if the boxes are
    checked for that particular field.
    """
    try:
        global combined_df
        combined_df = dev_planning.merge(
            aries,
            left_on='ARIES_ID',
            right_on='ARIES_CODE',
            how='left',
            suffixes=['_DP', '_AR']
        )
        global in_dp_not_aries
        in_dp_not_aries = combined_df.loc[pd.isna(combined_df.ARIES_CODE)]
        in_dp_not_aries = in_dp_not_aries[['ARIES_CODE', 'ARIES_ID',
                                           'WELL_NAME', 'LEASE',
                                           'RSV_CAT_DP', 'RSV_CAT_AR']]

        combined_df = aries.merge(
            dev_planning,
            left_on='ARIES_CODE',
            right_on='ARIES_ID',
            how='left',
            suffixes=['_AR', '_DP']
        )

        global in_aries_not_dp
        in_aries_not_dp = combined_df.loc[pd.isna(combined_df.ARIES_ID)]
        in_aries_not_dp = in_aries_not_dp[['ARIES_CODE', 'ARIES_ID',
                                           'WELL_NAME', 'LEASE', 'RSV_CAT_DP',
                                           'RSV_CAT_AR', 'TD_DATE']]

        in_aries_not_dp.loc[pd.isna(in_aries_not_dp.TD_DATE)]

        combined_df.dropna(subset=['ARIES_ID'], inplace=True)

        psid_list = []
        for index, row in combined_df.iterrows():
            psid_list.append(hierarchical_select(row.USER3,
                                                 row.PRESPUDWELLID,
                                                 type_to_coerce=int))

        combined_df['PSID_AR'] = psid_list
        combined_df.PSID_AR = combined_df.PSID_AR.astype('float64')

        combined_df.rename(columns={'PSID': 'PSID_DP'}, inplace=True)

        if psid_bool.get() == 1:
            global psid_check
            psid_check = compare_numeric_columns("PSID_DP",
                                                 "PSID_AR",
                                                 dataframe=combined_df,
                                                 round_to=0)

        if pn_bool.get() == 1:
            global pn_check
            pn_check = compare_columns('PROP_NUM_DP',
                                       'PROP_NUM_AR',
                                       dataframe=combined_df)
            pn_check[['PROP_NUM_DP', 'PROP_NUM_AR', 'MATCH']]

        if lease_bool.get() == 1:
            global lease_check
            lease_check = compare_columns('WELL_NAME', 'LEASE',
                                          dataframe=combined_df)

        if projnm_bool.get() == 1:
            global proj_name_check
            proj_name_check = compare_columns('PROJECT_NAME_DP',
                                              'PROJECT_NAME_AR',
                                              dataframe=combined_df)

        if padnm_bool.get() == 1:
            global pad_name_check
            pad_name_check = compare_columns('PAD_NAME_DP',
                                             'PAD_NAME_AR',
                                             dataframe=combined_df)

        if mda_bool.get() == 1:
            global mda_check
            mda_check = compare_columns('MKT_DEDICATION_AREA',
                                        'MDA',
                                        dataframe=combined_df)

        #            Start Checking Master table Lat Longs          #
        #            Start Checking Master table Lat Longs          #
        #            Start Checking Master table Lat Longs          #

        if msh_lat_bool.get() == 1:
            global sl_lat_check
            sl_lat_check = compare_numeric_columns('SL_LAT',
                                                   'LAT_SURFACE',
                                                   dataframe=combined_df,
                                                   round_to=4)

        if msh_long_bool.get() == 1:
            global sl_long_check
            sl_long_check = compare_numeric_columns('SL_LONG',
                                                    'LONG_SURFACE',
                                                    dataframe=combined_df,
                                                    round_to=4)

        #    Start Target Hole checks     #

        # In South Texas we used the first waypoint values if they existed
        # and if not we would use the Landing point.
        # This may be different in different BUs
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
        combined_df['TP_LAT'] = dp_tp_lat_list
        combined_df['TP_LONG'] = dp_tp_long_list

        if mth_lat_bool.get() == 1:
            global th_lat_check
            th_lat_check = compare_numeric_columns('TP_LAT',
                                                   'LAT_TARGET',
                                                   dataframe=combined_df,
                                                   round_to=4)

        if mth_long_bool.get() == 1:
            global th_long_check
            th_long_check = compare_numeric_columns('TP_LONG',
                                                    'LONG_TARGET',
                                                    dataframe=combined_df,
                                                    round_to=4)

        if mbh_lat_bool.get() == 1:
            global bh_lat_check
            bh_lat_check = compare_numeric_columns('BHL_LAT',
                                                   'LAT_BH',
                                                   dataframe=combined_df,
                                                   round_to=4)

        if mbh_long_bool.get() == 1:
            global bh_long_check
            bh_long_check = compare_numeric_columns('BHL_LONG',
                                                    'LONG_BH',
                                                    dataframe=combined_df,
                                                    round_to=4)

        #          Check Budget table Lat Longs          #
        #          Check Budget table Lat Longs          #
        #          Check Budget table Lat Longs          #

        if bsh_lat_bool.get() == 1:
            global b_sl_lat_check
            b_sl_lat_check = compare_numeric_columns('SL_LAT',
                                                     'PLANNED_SH_LAT',
                                                     dataframe=combined_df,
                                                     round_to=4)

        if bsh_long_bool.get() == 1:
            global b_sl_long_check
            b_sl_long_check = compare_numeric_columns('SL_LONG',
                                                      'PLANNED_SH_LONG',
                                                      dataframe=combined_df,
                                                      round_to=4)

        #    Start Target Hole checks     #

        if bth_lat_bool.get() == 1:
            global b_th_lat_check
            b_th_lat_check = compare_numeric_columns('TP_LAT',
                                                     'PLANNED_TARGET_LAT',
                                                     dataframe=combined_df,
                                                     round_to=4)

        if bth_long_bool.get() == 1:
            global b_th_long_check
            b_th_long_check = compare_numeric_columns('TP_LONG',
                                                      'PLANNED_TARGET_LONG',
                                                      dataframe=combined_df,
                                                      round_to=4)

        if bbh_lat_bool.get() == 1:
            global b_bh_lat_check
            b_bh_lat_check = compare_numeric_columns('BHL_LAT',
                                                     'PLANNED_BH_LAT',
                                                     dataframe=combined_df,
                                                     round_to=4)

        if bbh_long_bool.get() == 1:
            global b_bh_long_check
            b_bh_long_check = compare_numeric_columns('BHL_LONG',
                                                      'PLANNED_BH_LONG',
                                                      dataframe=combined_df,
                                                      round_to=4)

        # End Checking lat longs #

        if m_ll_bool.get() == 1:
            global m_lateral_len_check
            m_lateral_len_check = compare_numeric_columns('COMPLETABLE_LL',
                                                          'LATERAL_LEN',
                                                          dataframe=combined_df,
                                                          round_to=0)

        if b_ll_bool.get() == 1:
            global b_lateral_len_check
            b_lateral_len_check = compare_numeric_columns('COMPLETABLE_LL',
                                                          'PLANNED_LL',
                                                          dataframe=combined_df,
                                                          round_to=0)
    except NameError as e:
        tk.messagebox.showinfo("run_checks error",
                               f"Make sure you pull the data first:\n{e}")


def write_backups(path_to_folder):
    try:
        with pd.ExcelWriter(r'{path_to_folder}\backups.xlsx'
                            .format(path_to_folder=path_to_folder)) as writer:
            dev_planning.to_excel(writer, sheet_name='DevPlanning_backup')
            aries.to_excel(writer, sheet_name='Aries_Backup')

        with pd.ExcelWriter(r'{path_to_folder}\changes.xlsx'
                            .format(path_to_folder=path_to_folder)) as writer:
            in_aries_not_dp.to_excel(writer, sheet_name='in_aries_not_dp')
            in_dp_not_aries.to_excel(writer, sheet_name='in_dp_not_aries')
            if psid_bool.get() == 1:
                psid_check.to_excel(writer, sheet_name='PSID')
            if pn_bool.get() == 1:
                pn_check.to_excel(writer, sheet_name='PROP_NUM')
            if lease_bool.get() == 1:
                lease_check.to_excel(writer, sheet_name='LEASE')
            if projnm_bool.get() == 1:
                proj_name_check.to_excel(writer, sheet_name="PROJECT NAME")
            if padnm_bool.get() == 1:
                pad_name_check.to_excel(writer, sheet_name='PAD_NAME')
            if msh_lat_bool.get() == 1:
                sl_lat_check.to_excel(writer, sheet_name="SH_LAT")
            if msh_long_bool.get() == 1:
                sl_long_check.to_excel(writer, sheet_name="SH_LONG")
            if mth_lat_bool.get() == 1:
                th_lat_check.to_excel(writer, sheet_name='TH_LAT')
            if mth_long_bool.get() == 1:
                th_long_check.to_excel(writer, sheet_name='TH_LONG')
            if mbh_lat_bool.get() == 1:
                bh_lat_check.to_excel(writer, sheet_name='BH_LAT')
            if mbh_long_bool.get() == 1:
                bh_long_check.to_excel(writer, sheet_name='BH_LONG')
            if m_ll_bool.get() == 1:
                m_lateral_len_check.to_excel(writer, sheet_name='LATERAL_LEN')
            if bsh_lat_bool.get() == 1:
                b_sl_lat_check.to_excel(writer, sheet_name="PLANNED_SH_LAT")
            if bsh_long_bool.get() == 1:
                b_sl_long_check.to_excel(writer, sheet_name="PLANNED_SH_LONG")
            if bth_lat_bool.get() == 1:
                b_th_lat_check.to_excel(writer,
                                        sheet_name='PLANNED_TARGET_LAT')
            if bth_long_bool.get() == 1:
                b_th_long_check.to_excel(writer,
                                         sheet_name='PLANNED_TARGET_LONG')
            if bbh_lat_bool.get() == 1:
                b_bh_lat_check.to_excel(writer, sheet_name='PLANNED_BH_LAT')
            if bbh_long_bool.get() == 1:
                b_bh_long_check.to_excel(writer, sheet_name='PLANNED_BH_LONG')
            if b_ll_bool.get() == 1:
                b_lateral_len_check.to_excel(writer, sheet_name='PLANNED_LL')
        tk.messagebox.showinfo("Backup Status",
                               "Backup and Checks successful")
    except NameError:
        tk.messagebox.showinfo("Backup And Check Error",
                               """Backup and Checks failed.
                               Please connect to the database first.""")
    except:
        tk.messagebox.showinfo("Backup And Check Error",
                               """Backup and Checks failed.
                               please check path""")


def write_qc(path_to_folder):
    try:
        with pd.ExcelWriter(r'{path_to_folder}\post_update.xlsx'
                            .format(path_to_folder=path_to_folder)) as writer:
            in_aries_not_dp.to_excel(writer, sheet_name='in_aries_not_dp')
            in_dp_not_aries.to_excel(writer, sheet_name='in_dp_not_aries')
            if psid_bool.get() == 1:
                psid_check.to_excel(writer, sheet_name='PSID')
                if 'UPDATE ARIES' in psid_check.MATCH.value_counts():
                    writer.PSID.set_tab_color('gold')
            if pn_bool.get() == 1:
                pn_check.to_excel(writer, sheet_name='PROP_NUM')
            if lease_bool.get() == 1:
                lease_check.to_excel(writer, sheet_name='LEASE')
                if 'UPDATE ARIES' in lease_check.MATCH.value_counts():
                    writer.LEASE.set_tab_color('gold')
            if projnm_bool.get() == 1:
                proj_name_check.to_excel(writer, sheet_name="PROJECT NAME")
            if padnm_bool.get() == 1:
                pad_name_check.to_excel(writer, sheet_name='PAD_NAME')
            if msh_lat_bool.get() == 1:
                sl_lat_check.to_excel(writer, sheet_name="SH_LAT")
            if msh_long_bool.get() == 1:
                sl_long_check.to_excel(writer, sheet_name="SH_LONG")
            if mth_lat_bool.get() == 1:
                th_lat_check.to_excel(writer, sheet_name='TH_LAT')
            if mth_long_bool.get() == 1:
                th_long_check.to_excel(writer, sheet_name='TH_LONG')
            if mbh_lat_bool.get() == 1:
                bh_lat_check.to_excel(writer, sheet_name='BH_LAT')
            if mbh_long_bool.get() == 1:
                bh_long_check.to_excel(writer, sheet_name='BH_LONG')
            if m_ll_bool.get() == 1:
                m_lateral_len_check.to_excel(writer, sheet_name='LATERAL_LEN')
            if bsh_lat_bool.get() == 1:
                b_sl_lat_check.to_excel(writer, sheet_name="PLANNED_SH_LAT")
            if bsh_long_bool.get() == 1:
                b_sl_long_check.to_excel(writer, sheet_name="PLANNED_SH_LONG")
            if bth_lat_bool.get() == 1:
                b_th_lat_check.to_excel(writer,
                                        sheet_name='PLANNED_TARGET_LAT')
            if bth_long_bool.get() == 1:
                b_th_long_check.to_excel(writer,
                                         sheet_name='PLANNED_TARGET_LONG')
            if bbh_lat_bool.get() == 1:
                b_bh_lat_check.to_excel(writer, sheet_name='PLANNED_BH_LAT')
            if bbh_long_bool.get() == 1:
                b_bh_long_check.to_excel(writer, sheet_name='PLANNED_BH_LONG')
            if b_ll_bool.get() == 1:
                b_lateral_len_check.to_excel(writer, sheet_name='PLANNED_LL')
        tk.messagebox.showinfo("Backup Status",
                               "Backup and Checks successful")
    except NameError as e:
        tk.messagebox.showinfo("Write QC Error",
                               f"""Backup and Checks failed.
                               Please connect to the database first.
                               {e}""")
    except Exception as e:
        tk.messagebox.showinfo("Write QC Error",
                               f"""Backup and Checks failed.
                               please check file path
                               {e}""")


def update_aries():
    Server = 'Aries-prod'
    Database = 'Working_District'
    Driver = 'ODBC Driver 17 for SQL Server'
    database_url = f"mssql://@{Server}/{Database}?driver={Driver}"

    engine = sqlalchemy.create_engine(database_url)


    #         Push updates to Master Table        #
    #         Push updates to Master Table        #
    #         Push updates to Master Table        #


    # Push updates to the PROP_NUM column into WD Property table.
    try:
        if pn_bool.get() == 1:
            update_table('ac_property_base',
                         pn_check,
                         engine,
                         'PROP_NUM_DP',
                         'PROP_NUM',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'PROP_NUM_DP': sqlalchemy.VARCHAR(10)
                             })

        # Update Pre Spud ID
        # South Texas uses USER3 to house PSID to avoid unwanted syncing
        # between Aries and GPlat.
        if psid_bool.get() == 1:
            if bu_select.get().upper() == "SOUTH TEXAS":
                update_table('ac_property_base',
                             psid_check,
                             engine,
                             'PSID_DP',
                             'USER3',
                             {
                                 "ARIES_CODE": sqlalchemy.VARCHAR(255),
                                 'PSID_DP': sqlalchemy.VARCHAR(255)
                                 })
            else:
                update_table('ac_property_base',
                             psid_check,
                             engine,
                             'PSID_DP',
                             'PRESPUDWELLID',
                             {
                                 "ARIES_CODE": sqlalchemy.VARCHAR(255),
                                 'PSID_DP': sqlalchemy.VARCHAR(255)
                                 })

        # Update LEASE
        if lease_bool.get() == 1:
            update_table('ac_property_base',
                         lease_check,
                         engine,
                         'WELL_NAME',
                         'LEASE',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'WELL_NAME': sqlalchemy.VARCHAR(36)
                             })

        # Update PAD_NAME
        if padnm_bool.get() == 1:
            update_table('ac_property_base',
                         pad_name_check,
                         engine,
                         'PAD_NAME_DP',
                         'PAD_NAME',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'PAD_NAME_DP': sqlalchemy.VARCHAR(36)
                             })

        # Update M.LAT_SURFACE
        if msh_lat_bool.get() == 1:
            update_table('ac_property_base',
                         sl_lat_check,
                         engine,
                         'SL_LAT',
                         'LAT_SURFACE',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'SL_LAT': sqlalchemy.FLOAT()
                             })

        # Update M.LONG_SURFACE
        if msh_long_bool.get() == 1:
            update_table('ac_property_base',
                         sl_long_check,
                         engine,
                         'SL_LONG',
                         'LONG_SURFACE',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'SL_LONG': sqlalchemy.FLOAT()
                             })

        # Update M.LAT_TARGET
        if mth_lat_bool.get() == 1:
            update_table('ac_property_base',
                         th_lat_check,
                         engine,
                         'TP_LAT',
                         'LAT_TARGET',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'TP_LAT': sqlalchemy.FLOAT()
                             })

        # Update M.LONG_TARGET
        if mth_long_bool.get() == 1:
            update_table('ac_property_base',
                         th_long_check,
                         engine,
                         'TP_LONG',
                         'LONG_TARGET',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'TP_LONG': sqlalchemy.FLOAT()
                             })

        # Update M.LAT_BH
        if mbh_lat_bool.get() == 1:
            update_table('ac_property_base',
                         bh_lat_check,
                         engine,
                         'BHL_LAT',
                         'LAT_BH',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'BHL_LAT': sqlalchemy.FLOAT()
                             })

        # Update M.LONG_BH
        if mbh_long_bool.get() == 1:
            update_table('ac_property_base',
                         bh_long_check,
                         engine,
                         'BHL_LONG',
                         'LONG_BH',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'BHL_LONG': sqlalchemy.FLOAT()
                             })

        # Update M.LATERAL_LEN
        if m_ll_bool.get() == 1:
            update_table('ac_property_base',
                         m_lateral_len_check,
                         engine,
                         'COMPLETABLE_LL',
                         'LATERAL_LEN',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'COMPLETABLE_LL': sqlalchemy.INTEGER()
                             },  # The extra_condition is to ensure reserves
                         #  cases arent deleted in ST.
                         extra_condition=r"AND U.TEXT16 <> 'RESERVES CASE'")


        #          Push updates to Budget table          #
        #          Push updates to Budget table          #
        #          Push updates to Budget table          #

        # Update B.PROJECT_NAME
        if projnm_bool.get() == 1:
            update_table('ac_budget_base',
                         proj_name_check,
                         engine,
                         'PROJECT_NAME_DP',
                         'PROJECT_NAME',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'PROJECT_NAME_DP': sqlalchemy.VARCHAR(75)
                             })

        # Update B.PLANNED_SH_LAT
        if bsh_lat_bool.get() == 1:
            update_table('ac_budget_base',
                         b_sl_lat_check,
                         engine,
                         'SL_LAT',
                         'PLANNED_SH_LAT',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'SL_LAT': sqlalchemy.FLOAT()
                             })

        # Update B.PLANNED_SH_LONG
        if bsh_long_bool.get() == 1:
            update_table('ac_budget_base',
                         b_sl_long_check,
                         engine,
                         'SL_LONG',
                         'PLANNED_SH_LONG',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'SL_LONG': sqlalchemy.FLOAT()
                             })

        # Update B.PLANNED_TARGET_LAT
        if bth_lat_bool.get() == 1:
            update_table('ac_budget_base',
                         b_th_lat_check,
                         engine,
                         'TP_LAT',
                         'PLANNED_TARGET_LAT',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'TP_LAT': sqlalchemy.FLOAT()
                             })

        # Update B.PLANNED_TARGET_LONG
        if bth_long_bool.get() == 1:
            update_table('ac_budget_base',
                         b_th_long_check,
                         engine,
                         'TP_LONG',
                         'PLANNED_TARGET_LONG',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'TP_LONG': sqlalchemy.FLOAT()
                             })

        # Update B.PLANNED_BH_LAT
        if bbh_lat_bool.get() == 1:
            update_table('ac_budget_base',
                         b_bh_lat_check,
                         engine,
                         'BHL_LAT',
                         'PLANNED_BH_LAT',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'BHL_LAT': sqlalchemy.FLOAT()
                             })

        # Update B.PLANNED_BH_LONG
        if bbh_long_bool.get() == 1:
            update_table('ac_budget_base',
                         b_bh_long_check,
                         engine,
                         'BHL_LONG',
                         'PLANNED_BH_LONG',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'BHL_LONG': sqlalchemy.FLOAT()
                             })

        # Update B.PLANNED_LL
        if b_ll_bool.get() == 1:
            update_table('ac_budget_base',
                         b_lateral_len_check,
                         engine,
                         'COMPLETABLE_LL',
                         'PLANNED_LL',
                         {
                             "ARIES_CODE": sqlalchemy.VARCHAR(255),
                             'COMPLETABLE_LL': sqlalchemy.INTEGER()
                             })

        tk.messagebox.showinfo("Aries Update", "Push Successful")
    except:
        tk.messagebox.showinfo("Aries Update", "Push Failed")

#%%


window = tk.Tk()
window.title("Dev Planning Sync program")

bu_select_label = tk.Label(text='Please enter the BU you would like to sync:')
bu_select_label.grid(row=10, column=0)

bu_select = tk.Entry(width=60)
bu_select.grid(row=20, column=0)

connect_button = tk.Button(text="Connect", width=15,
                           command=lambda: pull_data(bu_select.get().upper())
                           )
connect_button.grid(row=20, column=10, pady=5, padx=5)


#           Check Box Frame          #
#           Check Box Frame          #
#           Check Box Frame          #


check_box_label = tk.Label(text="Which columns would you like to check or \
update?")
check_box_label.grid(row=30, column=0)

checkbox_frame = tk.Frame(master=window)
checkbox_frame.grid(row=40, column=0)

psid_bool = tk.IntVar(checkbox_frame, value=1)
psid_cb = tk.Checkbutton(checkbox_frame, text='PSID', variable=psid_bool)
psid_cb.grid(row=0, column=0)

pn_bool = tk.IntVar(checkbox_frame, value=1)
pn_cb = tk.Checkbutton(checkbox_frame, text='PROP_NUM', variable=pn_bool)
pn_cb.grid(row=00, column=10)

lease_bool = tk.IntVar(checkbox_frame, value=1)
lease_cb = tk.Checkbutton(checkbox_frame, text='LEASE', variable=lease_bool)
lease_cb.grid(row=10, column=0)

padnm_bool = tk.IntVar(checkbox_frame, value=1)
padnm_cb = tk.Checkbutton(checkbox_frame, text='PAD_NAME', variable=padnm_bool)
padnm_cb.grid(row=10, column=10)

projnm_bool = tk.IntVar(checkbox_frame, value=1)
projnm_cb = tk.Checkbutton(checkbox_frame, text='PROJECT_NAME',
                           variable=projnm_bool)
projnm_cb.grid(row=20, column=0)

mda_bool = tk.IntVar(checkbox_frame, value=1)
mda_cb = tk.Checkbutton(checkbox_frame, text='MDA', variable=mda_bool)
mda_cb.grid(row=20, column=10)

m_ll_bool = tk.IntVar(checkbox_frame, value=1)
m_ll_cb = tk.Checkbutton(checkbox_frame, text='LATERAL_LEN',
                         variable=m_ll_bool)
m_ll_cb .grid(row=25, column=0)

b_ll_bool = tk.IntVar(checkbox_frame, value=1)
b_ll_cb = tk.Checkbutton(checkbox_frame, text='PLANNED_LL', variable=b_ll_bool)
b_ll_cb.grid(row=25, column=10)


# Add label for master table start dates
m_lat_long_label = tk.Label(checkbox_frame, text="Master table Lat Longs:")
m_lat_long_label.grid(row=30, column=5, columnspan=4, sticky=tk.EW)

msh_lat_bool = tk.IntVar(checkbox_frame, value=1)
msh_lat_cb = tk.Checkbutton(checkbox_frame, text="LAT_SURFACE",
                            variable=msh_lat_bool)
msh_lat_cb.grid(row=40, column=0)

msh_long_bool = tk.IntVar(checkbox_frame, value=1)
msh_long_cb = tk.Checkbutton(checkbox_frame, text="LONG_SURFACE",
                             variable=msh_long_bool)
msh_long_cb.grid(row=40, column=10)

mth_lat_bool = tk.IntVar(checkbox_frame, value=1)
mth_lat_cb = tk.Checkbutton(checkbox_frame, text="LAT_TARGET",
                            variable=mth_lat_bool)
mth_lat_cb.grid(row=50, column=0)

mth_long_bool = tk.IntVar(checkbox_frame, value=1)
mth_long_cb = tk.Checkbutton(checkbox_frame, text="LONG_TARGET",
                             variable=mth_long_bool)
mth_long_cb.grid(row=50, column=10)

mbh_lat_bool = tk.IntVar(checkbox_frame, value=1)
mbh_lat_cb = tk.Checkbutton(checkbox_frame, text="LAT_BH",
                            variable=mbh_lat_bool)
mbh_lat_cb.grid(row=60, column=0)

mbh_long_bool = tk.IntVar(checkbox_frame, value=1)
mbh_long_cb = tk.Checkbutton(checkbox_frame, text="LONG_BH",
                             variable=mbh_long_bool)
mbh_long_cb.grid(row=60, column=10)

# Add label for budget table start dates
b_lat_long_label = tk.Label(checkbox_frame, text="Budget table Lat Longs:")
b_lat_long_label.grid(row=70, column=5, columnspan=4, sticky=tk.EW)

bsh_lat_bool = tk.IntVar(checkbox_frame, value=1)
bsh_lat_cb = tk.Checkbutton(checkbox_frame, text="PLANNED_SH_LAT",
                            variable=bsh_lat_bool)
bsh_lat_cb.grid(row=80, column=0)

bsh_long_bool = tk.IntVar(checkbox_frame, value=1)
bsh_long_cb = tk.Checkbutton(checkbox_frame, text="PLANNED_SH_LONG",
                             variable=bsh_long_bool)
bsh_long_cb.grid(row=80, column=10)

bth_lat_bool = tk.IntVar(checkbox_frame, value=1)
bth_lat_cb = tk.Checkbutton(checkbox_frame, text="PLANNED_TARGET_LAT",
                            variable=bth_lat_bool)
bth_lat_cb.grid(row=90, column=0)

bth_long_bool = tk.IntVar(checkbox_frame, value=1)
bth_long_cb = tk.Checkbutton(checkbox_frame, text="PLANNED_TARGET_LONG",
                             variable=bth_long_bool)
bth_long_cb.grid(row=90, column=10)

bbh_lat_bool = tk.IntVar(checkbox_frame, value=1)
bbh_lat_cb = tk.Checkbutton(checkbox_frame, text="PLANNED_BH_LAT",
                            variable=bbh_lat_bool)
bbh_lat_cb.grid(row=100, column=0)


bbh_long_bool = tk.IntVar(checkbox_frame, value=1)
bbh_long_cb = tk.Checkbutton(checkbox_frame, text="PLANNED_BH_LONG",
                             variable=bbh_long_bool)
bbh_long_cb.grid(row=100, column=10)


#           End Check Box Frame          #
#           End Check Box Frame          #
#           End Check Box Frame          #


backup_folder_lab = tk.Label(text="Input the path to the output folder:")
backup_folder_lab.grid(row=50, column=0)

backup_fold_ent = tk.Entry(width=60)
backup_fold_ent.grid(row=60, column=0, padx=15)

backup_btn = tk.Button(text="Backup Aries and DP",
                       command=lambda: [run_checks(),
                                        write_backups(backup_fold_ent.get())])
backup_btn.grid(row=60, column=10)


open_str = r"start EXCEL.EXE {folder}\{file}.xlsx"
open_bkup_btn = tk.Button(text="Open Backup", width=15,
                          command=lambda: os.system(open_str.format(
                              folder=backup_fold_ent.get(),
                              file='backups')))
open_bkup_btn.grid(row=64, column=10)

open_change_btn = tk.Button(text="Open Changes", width=15,
                            command=lambda: os.system(open_str.format(
                              folder=backup_fold_ent.get(),
                              file='changes')))
open_change_btn.grid(row=66, column=10)


update_button = tk.Button(text="UPDATE ARIES", background='Red',
                          width=70, height=5, command=lambda: update_aries())
update_button.grid(row=70, column=0, columnspan=40, pady=7)

qc_btn = tk.Button(text="Post Update QC",
                   width=15,
                   command=lambda: [run_checks(),
                                    write_qc(backup_fold_ent.get())])
qc_btn.grid(row=80, column=10)

open_change_btn = tk.Button(text="Open Update QC", width=15,
                            command=lambda: os.system(open_str.format(
                              folder=backup_fold_ent.get(),
                              file='post_update')))
open_change_btn.grid(row=85, column=10)

close_btn = tk.Button(text='Exit', width=15, height=3, pady=10,
                      command=lambda: window.destroy())
close_btn.grid(row=90, column=10)

window.mainloop()
#%%

if __name__ == '__main__':
    pass
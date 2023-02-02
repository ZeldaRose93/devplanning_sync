# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 16:21:22 2023

@author: zrose
"""

# This file houses the functions used for the GUI version of the DP sync.
import os

import pandas as pd
import pyodbc
import tkinter as tk

from devplanning_sync_functions import connect_to_snowflake


def _load_snowflake(business_unit):
    sf_conn = connect_to_snowflake()

    snowflake_string = r"""
    SELECT
        *
    FROM
        SOURCE.GIS.DEV_PLANNING AS DP
    WHERE
        (DP.BUSINESS_UNIT = '{business_unit}')
        AND DP.SCENARIO IN ('A', 'MDV')
        AND DP.DEV_STATUS IN ('PRIMARY', 'DEVELOPMENT')
    """.format(business_unit=business_unit)

    dev_planning = pd.read_sql(snowflake_string, sf_conn)
    sf_conn.close()
    return dev_planning


def _load_aries(business_unit):
    uid = os.getlogin()
    conn = pyodbc.connect(
        r'DRIVER={ODBC Driver 17 for SQL Server};' + r' uid={' + uid + r'};' +\
            r'server={Aries-prod}; Database={Working_District}; Trusted_Connection=yes')

    # Write Query string from SQL
    aries_string = r"""
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
    INNER JOIN [Working_District].[AriesAdmin].[AC_BUDGET] AS B
        ON M.PROPNUM = B.PROPNUM
    WHERE
        (M.BUSINESS_UNIT ='{business_unit}')
        AND M.RSV_CAT IN ('5PUD','5PUDX','6PROB','7POSS');
    """.format(business_unit=business_unit)

    aries = pd.read_sql(aries_string, conn)
    conn.close()

    return aries


# This function will fail to set the global variables aries and dev_planning
# in our other scripts. This is because each module or file that we use has
# its own global scope.

# def pull_data(business_unit):
#     global dev_planning
#     global aries
#     try:
#         dev_planning = _load_snowflake(business_unit)
#         aries = _load_aries(business_unit)
#         tk.messagebox.showinfo("Connection Success")
#     except:
#         tk.messagebox.showinfo("Connection Failed")
#     return dev_planning, aries





if __name__ == "__main__":
    pass

import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyodbc
import sqlalchemy



def connect_to_snowflake():
    '''
    This function exists to connect to snowflake while obscuring your password. It will still display your password as you type, but everything will be deleted after
    the function is run so you won't have to worry about people seeing your password after you've connected.
    '''
    uid = os.getlogin()
    connection_string = f'DRIVER={{SnowflakeDSIIDriver}}; SERVER=chk-energy.snowflakecomputing.com;DSN=Snowflake_Azure_Prod; AUTHENTICATOR=EXTERNALBROWSER; UID={uid};'

    conn = pyodbc.connect(connection_string)
    return conn


def hierarchical_select(primary, secondary, type_to_coerce):
    """
    Takes two inputs and returns the first if it is not none
    returns the second if it isn't none, and
    returns 0 when both inputs are none.

    Params:
        primary (any): the first variable to check
        secondary (any): the second variable to check

    Returns:
        The primary value as an integer if it exists and is not null,
        the secondary value as an integer if it exists and is not null,
        0 if both values are None or NaN.
    """
    try:
        type_to_coerce(primary)
        if pd.isna(primary):
            pass
        else:
            return type_to_coerce(primary)
    except TypeError:  # This is necessary to handle None types
        pass
    except ValueError:  # This is necessary to handle NaN float types.
        pass
    try:
        type_to_coerce(secondary)
        if pd.isna(secondary):
            pass
        else:
            return type_to_coerce(secondary)
    except TypeError:
        pass
    except ValueError:
        pass
    try:
        return 0
    finally:
        pass


def match(devp,
          aries,
          msg1='MATCH',
          msg2='UPDATE ARIES',
          msg3='UPDATE ARIES',
          msg4='UPDATE DEVPLANNING',
          msg5='NOT ASSIGNED'):
    """
    Takes one value from the dev_planning DataFrame and one value from the aries
    DataFrame and returns a new column with one of five messages depending on
    how the two values compare.

    This was written to work while iterating over the fields and is mainly
    a helper function for compare_columns and compare_numeric_column.

    Args:
        devp: The value from the Dev Planning dataframe
        aries: The value from the Aries Dataframe
        msg1: Message to output when the values from DP and Aries are equal
        msg2: Message to output when dp and aries don't match but are the same
              type and are not null.
        msg3: Message to output when DP value is not null, but Aries value is
              null.
        msg4: Message to output when DP is null, but aries is not.
        msg5: Message to output when both DP and Aries values are null.

    Returns one of five messages based on how the values compare.
    """
    if (type(devp) is None or pd.isna(devp)) and (type(aries) is None or pd.isna(aries)):
        return msg5
    elif type(devp) is None or pd.isna(devp):
        return msg4
    elif type(aries) is None or pd.isna(aries):
        return msg3
    elif devp != aries:
        return msg2
    elif devp == aries:
        return msg1


def compare_columns(col_dp,
                    col_ar,
                    dataframe,  # default value causes errors
                    msg1='MATCH',
                    msg2='UPDATE ARIES',
                    msg3='UPDATE ARIES',
                    msg4='UPDATE DEVPLANNING',
                    msg5='NOT ASSIGNED'):
    """
    Takes column names from a dataframe containing values from both Aries and
    Dev Planning and compares the values.

    Args:
        col_dp (str): the column in dataframe that contains Dev Planning data.
        col_ar (str): the column in dataframe that contains Aries data.
        dataframe (pd.DataFrame): DataFrame to use for the comparison.
        msg1 (str): Message to output when the values from DP and Aries are
                    equal
        msg2 (str): Message to output when dp and aries don't match but are the
                    same type and are not null.
        msg3 (str): Message to output when DP value is not null, but Aries
                    value is null.
        msg4 (str): Message to output when DP is null, but aries is not.
        msg5 (str): Message to output when both DP and Aries values are null.

    Returns
        pd.DataFrame with columns for Aries Code, Lease, the two columns that
        were compared and the output Match column.
    """
    if col_ar == "LEASE":
        tmp_df = dataframe[['ARIES_CODE', col_dp, col_ar]]
    else:
        tmp_df = dataframe[['ARIES_CODE', 'LEASE', col_dp, col_ar]]
    tmp_df['MATCH'] = 'initial'
    tmp_df.reset_index(inplace=True)
    for index, row in tmp_df.iterrows():
        tmp_df['MATCH'].iloc[index] = match(row[col_dp],
                                            row[col_ar],
                                            msg1=msg1,
                                            msg2=msg2,
                                            msg3=msg3,
                                            msg4=msg4,
                                            msg5=msg5)

    tmp_df[[col_dp, col_ar, 'MATCH']]
    return tmp_df


def compare_numeric_columns(col_dp: str,
                            col_ar: str,
                            dataframe: pd.DataFrame,
                            round_to: int = 0):
    """
    Compares two columns of numeric data.

    Args:
        col_dp: The column name that contains Dev Planning data.
        col_ar: The column name that contains Aries data.
        dataframe: The name of the dataframe to compare.
        round_to: The precision to consider a match.
                  It will round to 10eround_to

    Returns:
        DataFrame containing the Aries Code, lease, DP column, Aries column,
        the difference between the two columns, and a column that shows if the
        values match or not.
    """
    tmp_df = dataframe[['ARIES_CODE', 'LEASE', col_dp, col_ar]]
    tmp_df['DELTA'] = 0
    tmp_df['MATCH'] = 'initial'
    tmp_df[col_dp] = pd.to_numeric(tmp_df[col_dp])
    tmp_df[col_ar] = pd.to_numeric(tmp_df[col_ar])
    tmp_df.reset_index(inplace=True)
    for index, row in tmp_df.iterrows():
        tmp_df['DELTA'].iloc[index] = round(row[col_dp] - row[col_ar], round_to)
        tmp_df['MATCH'].iloc[index] = ''
        # Check for the condition where both aries and DP are blank
        if ((pd.isna(tmp_df[col_dp].iloc[index])
                or tmp_df[col_dp].iloc[index] == 0.)
            and ((pd.isna(tmp_df[col_ar].iloc[index])
                 or tmp_df[col_ar].iloc[index] == 0.))):
            tmp_df['MATCH'].iloc[index] = 'BOTH VALUES NULL'
        # Check for the condition where DP is null, but aries isn't
        elif (pd.isna(tmp_df[col_dp].iloc[index]) \
                or tmp_df[col_dp].iloc[index] == 0.):
            tmp_df['MATCH'].iloc[index] = 'DP EMPTY'
        # Check for the condition where Aries is null, but DP isn't
        elif (pd.isna(tmp_df[col_ar].iloc[index])
              or tmp_df[col_ar].iloc[index] == 0.):
            tmp_df['MATCH'].iloc[index] = "UPDATE ARIES"
        # Check for both Aries and DP being non-zero but non-equal
        elif abs(tmp_df['DELTA'].iloc[index]) > 0.0:
            tmp_df['MATCH'].iloc[index] = "UPDATE ARIES"
        else:
            tmp_df['MATCH'].iloc[index] = "MATCH"
    return tmp_df


def update_table(table_to_update,
                 check_table,
                 engine,
                 devplanning_column,
                 aries_column,
                 dtype_dict,
                 extra_condition=""):

    connection = engine.connect()

    check_table.to_sql('#check_table', connection, if_exists='replace',
                       dtype=dtype_dict)

    # Check to see if we're using the only table with an Aries Code.
    if table_to_update.lower() == 'ac_property_base':

        # This will setup the query to join only the Master table and the
        # temporary table that we uploaded.
        sql_string = (r"""
                      UPDATE
                          M
                      SET
                          M.{aries_column} = ct.{devplanning_column}
                      FROM
                          [WORKING_DISTRICT].[AriesAdmin].[AC_PROPERTY_BASE] M
                          INNER JOIN [#check_table] ct
                          ON ct.ARIES_CODE = M.ARIES_CODE
                          INNER JOIN [WORKING_DISTRICT].[AriesAdmin].[AC_USER]
                          U ON U.PROPNUM = M.PROPNUM
                      WHERE
                          ct.MATCH = 'UPDATE ARIES' {extra_condition}
                      ;""".format(aries_column=aries_column,
                                  devplanning_column=devplanning_column,
                                  extra_condition=extra_condition))

    else:
        # In order for queries to work as they are setup, we need to join in
        # the master table so we can connect with an Aries Code.
        sql_string = (r"""
                      UPDATE
                          ttu
                      SET
                          ttu.{aries_column} = ct.{devplanning_column}
                      FROM
                          ([WORKING_DISTRICT].[AriesAdmin].[AC_PROPERTY_BASE] M
                           INNER JOIN [Working_District].[AriesAdmin].
                           [{table_to_update}] ttu ON M.PROPNUM = ttu.PROPNUM)
                          INNER JOIN [#check_table] ct
                          ON ct.ARIES_CODE = M.ARIES_CODE
                     WHERE ct.MATCH = 'UPDATE ARIES' {extra_condition}
                      ;""".format(table_to_update=table_to_update,
                                  aries_column=aries_column,
                                  devplanning_column=devplanning_column,
                                  extra_condition=extra_condition))

    connection.execute(sqlalchemy.text(sql_string))

    # connection.commit()
    connection.close()


if __name__ == "__main__":
    pass

import pandas as pd
import pytest

from devplanning_sync_functions import compare_columns
from devplanning_sync_functions import compare_numeric_columns
from devplanning_sync_functions import hierarchical_select
from devplanning_sync_functions import match


class TestHierarchicalSelect:
    def test_hierarchical_select_none_none(self):
        actual_value = hierarchical_select(None, None, int)
        expected_value = 0
        assert actual_value == expected_value, f"The output value was {actual_value}, but we expected {expected_value}."

    def test_hierarchical_select_properstr_none(self):
        actual_value = hierarchical_select('43720', None, int)
        expected_value = 43720
        assert actual_value == expected_value, f"The output value was {actual_value}, but we expected {expected_value}."

    def test_hierarchical_select_none_properstr(self):
        actual_value = hierarchical_select(None, '43720', int)
        expected_value = 43720
        assert actual_value == expected_value, f"The output value was {actual_value}, but we expected {expected_value}."

    def test_hierarchical_select_nan_properstr(self):
        actual_value = hierarchical_select(float("nan"), '43720', int)
        expected_value = 43720
        assert actual_value == expected_value, f"The output value was {actual_value}, but we expected {expected_value}."

    def test_hierarchical_select_both_int(self):
        actual_value = hierarchical_select('76221', '43720', int)
        expected_value = 76221
        assert actual_value == expected_value, f"The output value was {actual_value}, but we expected {expected_value}."

    def test_hierarchical_select_both_float(self):
        actual_value = hierarchical_select('76221.498573119875', '43720.8793', float)
        expected_value = 76221.498573119875
        assert actual_value == pytest.approx(expected_value, abs=1e-6), f"The output value was {actual_value}, but we expected {expected_value}."


class TestMatch:
    def test_match_both_null(self):
        actual = match(None, None, msg5="test_pass")
        expected = "test_pass"
        assert actual == expected,\
            "function returned {} instead of msg5".format(actual)

    def test_match_dp_null_aries_valid(self):
        actual = match(None, 'valid', msg4="test_pass")
        expected = "test_pass"
        assert actual == expected,\
            "function returned {} instead of msg4".format(actual)

    def test_match_dp_valid_aries_null(self):
        actual = match('valid', None, msg3="test_pass")
        expected = "test_pass"
        assert actual == expected,\
            "function returned {} instead of msg3".format(actual)

    def test_match_both_valid_but_dont_match(self):
        actual = match('valid', 'Also valid', msg2="test_pass")
        expected = "test_pass"
        assert actual == expected,\
            "function returned {} instead of msg2".format(actual)

    def test_match_both_valid(self):
        actual = match("We're valid!", "We're valid!", msg1="test_pass")
        expected = "test_pass"
        assert actual == expected,\
            "function returned {} instead of msg1".format(actual)


class TestCompareColumns:
    setup_dict = {
        'ARIES_CODE': ['TEST001', 'TEST002', 'TEST003', 'TEST004', 'TEST005'],
        'LEASE': ['WELL1', 'WELL2', 'WELL3', 'WELL4', 'WELL5'],
        'DP_COL': ['ImEqual', 'NotEqualSameType', 'NotNull', None, None],
        'AR_COL': ['ImEqual', 'SameTypeNotEqual', None, 'NotNull', None]
    }
    combined_df = pd.DataFrame(setup_dict)

    def test_compare_columns(self):
        actual_df = compare_columns('DP_COL',
                                    'AR_COL',
                                    dataframe=self.combined_df,
                                    msg1='msg1',
                                    msg2='msg2',
                                    msg3='msg3',
                                    msg4='msg4',
                                    msg5='msg5')
        actual = list(actual_df.MATCH.values)
        expected = ['msg1', 'msg2', 'msg3', 'msg4', 'msg5']
        assert actual == expected, "One or more of the values are not equal."


class TestCompareNumericColumns:
    setup_dict = {
        'ARIES_CODE': ['TEST001', 'TEST002', 'TEST003', 'TEST004', 'TEST005',
                       'TEST006', 'TEST007', 'TEST008', 'TEST009', 'TEST010'],
        'LEASE': ['WELL1', 'WELL2', 'WELL3', 'WELL4', 'WELL5',
                  'WELL6', 'WELL7', 'WELL8', 'WELL9', 'WELL10'],
        'DP_COL': ['5.12345678', '-90.4682467',
                   '7.98765432', '-32.123456789',
                   '1.0000', '1.0000',
                   None, float('nan'),
                   None, float('nan')],
        'AR_COL': ['5.12345678', '-90.4682467',
                   '7.98123456', '-30.98756263',
                   None, float('nan'),
                   '1.0000', '1.0000',
                   None, float('nan')]
    }
    combined_df = pd.DataFrame(setup_dict)

    def test_compare_numeric_columns_int(self):
        actual_df = compare_numeric_columns('DP_COL',
                                            'AR_COL',
                                            dataframe=self.combined_df,
                                            round_to=0)
        actual = list(actual_df.MATCH.values)
        expected = ["MATCH", "MATCH",
                    'MATCH', 'UPDATE ARIES',
                    "UPDATE ARIES", 'UPDATE ARIES',
                    'DP EMPTY', 'DP EMPTY',
                    'BOTH VALUES NULL', 'BOTH VALUES NULL']
        assert actual == expected, "One or more of the values are not equal."

    def test_compare_numeric_columns_float(self):
        actual_df = compare_numeric_columns('DP_COL',
                                            'AR_COL',
                                            dataframe=self.combined_df,
                                            round_to=6)
        actual = list(actual_df.MATCH.values)
        expected = ["MATCH", "MATCH",
                    'UPDATE ARIES', 'UPDATE ARIES',
                    "UPDATE ARIES", 'UPDATE ARIES',
                    'DP EMPTY', 'DP EMPTY',
                    'BOTH VALUES NULL', 'BOTH VALUES NULL']
        assert actual == expected, "One or more of the values are not equal."


class TestUpdateTable:
    check_table_dict = {
        'aries_code': ["TEST001", "TEST002", 'TEST003', 'TEST004', 'TEST005'],
        'lease': ['Well 1', 'Well 2', 'Well 3', 'Well 4', 'Well 5'],
        'prop_num_dp': ['000001', '000002', '000003', '', '0'],
        'prop_num_ar': ['', '', '', '', '123456'],
        'match': ['UPDATE ARIES', 'UPDATE ARIES', 'UPDATE ARIES',
                  'NOT ASSIGNED', 'UPDATE DEVPLANNING']
        }

    mock_aries_dict = {
        'aries_code': ["TEST001", "TEST002", 'TEST003', 'TEST004', 'TEST005'],
        'lease': ['Well 1', 'Well 2', 'Well 3', 'Well 4', 'Well 5'],
        'prop_num_ar': ['', '', '', '', '123456']
        }

    check_df = pd.DataFrame(check_table_dict)
    aries_df = pd.DataFrame(mock_aries_dict)

    # The following lines setup a connection to the MSSQL working district
    # database.
    Server = 'Aries-prod'
    Database = 'Working_District'
    Driver = 'ODBC Driver 17 for SQL Server'
    database_url = f"mssql://@{Server}/{Database}?driver={Driver}"

    engine = sqlalchemy.create_engine(database_url)
    engine.table_names()

    update_wd()

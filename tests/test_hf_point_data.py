import sys
import os
import pytest
import sqlite3
import pandas as pd
import numpy as np

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
)

from hf_point_data import hf_point_data, utils
from hydrodata.mock_classes.mock_hydrodata import (create_mock_hydrodata, create_mock_observations_database,
                                                   cleanup_mock_hydrodata)

HYDRODATA = 'test_data/hydrodata'


def test_check_inputs_failure1():
    '''Parameter all_attributes cannot be True if return_metadata is False'''
    with pytest.raises(Exception):
        utils.check_inputs(data_source='usgs_nwis', variable='streamflow', temporal_resolution='daily',
                           aggregation='average', return_metadata=False, all_attributes=True)


def test_check_inputs_failure2():
    '''Parameter provided for variable not in supported list (typo).'''
    with pytest.raises(Exception):
        utils.check_inputs(data_source='usgs_nwis', variable='steamflow',
                           temporal_resolution='daily', aggregation='average')


def test_check_inputs_failure3():
    '''Parameter provided for temporal_resolution not in supported list.'''
    with pytest.raises(Exception):
        utils.check_inputs(data_source='usgs_nwis', variable='streamflow',
                           temporal_resolution='monthly', aggregation='average')


def test_get_var_id():
    create_mock_hydrodata(HYDRODATA)
    create_mock_observations_database(HYDRODATA)
    conn = sqlite3.connect(f'{HYDRODATA}/national_obs/point_obs.sqlite')

    # Build SQL connection to mock HydroData database
    assert utils.get_var_id(conn, data_source='usgs_nwis', variable='streamflow',
                            temporal_resolution='hourly', aggregation='average') == 1
    assert utils.get_var_id(conn, data_source='usgs_nwis', variable='streamflow',
                            temporal_resolution='daily', aggregation='average') == 2
    assert utils.get_var_id(conn, data_source='usgs_nwis', variable='wtd',
                            temporal_resolution='hourly', aggregation='average') == 3
    assert utils.get_var_id(conn, data_source='usgs_nwis', variable='wtd',
                            temporal_resolution='daily', aggregation='average') == 4
    assert utils.get_var_id(conn, data_source='usgs_nwis', variable='wtd',
                            temporal_resolution='instantaneous', aggregation='instantaneous') == 5
    assert utils.get_var_id(conn, data_source='usda_nrcs', variable='swe',
                            temporal_resolution='daily', aggregation='start-of-day') == 6
    assert utils.get_var_id(conn, data_source='usda_nrcs', variable='precipitation',
                            temporal_resolution='daily', aggregation='accumulated') == 7
    assert utils.get_var_id(conn, data_source='usda_nrcs', variable='precipitation',
                            temporal_resolution='daily', aggregation='total') == 8
    assert utils.get_var_id(conn, data_source='usda_nrcs', variable='precipitation', temporal_resolution='daily',
                            aggregation='total, snow-adjusted') == 9
    assert utils.get_var_id(conn, data_source='usda_nrcs', variable='temperature',
                            temporal_resolution='daily', aggregation='minimum') == 10
    assert utils.get_var_id(conn, data_source='usda_nrcs', variable='temperature',
                            temporal_resolution='daily', aggregation='maximum') == 11
    assert utils.get_var_id(conn, data_source='usda_nrcs', variable='temperature',
                            temporal_resolution='daily', aggregation='average') == 12
    assert utils.get_var_id(conn, data_source='usda_nrcs', variable='soil moisture', temporal_resolution='daily',
                            aggregation='start-of-day', depth_level=2) == 13
    assert utils.get_var_id(conn, data_source='usda_nrcs', variable='soil moisture', temporal_resolution='daily',
                            aggregation='start-of-day', depth_level=4) == 14
    assert utils.get_var_id(conn, data_source='usda_nrcs', variable='soil moisture', temporal_resolution='daily',
                            aggregation='start-of-day', depth_level=8) == 15
    assert utils.get_var_id(conn, data_source='usda_nrcs', variable='soil moisture', temporal_resolution='daily',
                            aggregation='start-of-day', depth_level=20) == 16
    assert utils.get_var_id(conn, data_source='usda_nrcs', variable='soil moisture', temporal_resolution='daily',
                            aggregation='start-of-day', depth_level=40) == 17
    cleanup_mock_hydrodata(HYDRODATA)


def test_filter_min_num_obs():
    df = pd.DataFrame({'site_id': ['101', '102', '103', '104', '105'],
                       'date1': [1, 5, 3, 4, 8], 'date2': [np.nan, 4, 2, 9, 4],
                       'date3': [np.nan, 9, 2, np.nan, 9]})

    assert len(utils.filter_min_num_obs(df, 1)) == 5
    assert len(utils.filter_min_num_obs(df, 2)) == 4
    assert len(utils.filter_min_num_obs(df, 3)) == 3


if __name__ == "__main__":
    pytest.main()

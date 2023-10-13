import sys
import os
import io
import pytest
import sqlite3
from unittest import mock
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from hf_point_data import hf_point_data, utils

# from hydrodata.mock_classes.mock_hydrodata import (create_mock_hydrodata, create_mock_observations_database,
# cleanup_mock_hydrodata)

HYDRODATA = "test_data/hydrodata"


class MockResponse:
    """Mock the flask.request response."""

    def __init__(self):
        data = {
            "headers": ["site_id", "2020-01-01", "2020-01-02"],
            "0": ["01019000", "18.39500", "18.36670"],
            "1": ["01027200", "4.92420", "4.64120"],
            "2": ["01029500", "35.09200", "33.67700"],
        }

        # Create a DataFrame with specified column names
        df = pd.DataFrame(data)
        print("The dataframe is:", df)
        buffer = io.BytesIO()
        df.to_pickle(buffer)
        data_bytes = buffer.getvalue()

        self.headers = {}
        self.status_code = 200
        self.content = data_bytes
        self.text = None
        self.checksum = ""


class MockResponseSecurity:
    """Mock the flask.request response."""

    def __init__(self):
        data = b'{"email":"dummy@email.com","expires":"2023/10/14 18:31:11 GMT-0000","groups":["demo"],"jwt_token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkdW1teSIsImVtYWlsIjoiZHVtbXlAZW1haWwuY29tIiwiZ3JvdXBzIjpbImRlbW8iXSwiZXhwIjoxNjk3MzA4MjcxfQ.Z6YJHZOlo3OdzdmuLHAqdaRIraH1Z-WzoKtXQSbh92w","user_id":"dummy"}'

        self.headers = {}
        self.status_code = 200
        self.content = data
        self.text = None
        self.checksum = ""


def mock_requests_get(point_data_url, headers, timeout=180):
    """Create a mock csv response."""

    if headers is None:
        response = MockResponseSecurity()
    else:
        response = MockResponse()

    return response


def test_get_dataframe():
    """Test ability to retreive vegp file."""

    with mock.patch(
        "requests.get",
        new=mock_requests_get,
    ):
        hf_point_data.HYDRODATA = "/empty"
        data_df = hf_point_data.get_data(
            "usgs_nwis",
            "streamflow",
            "daily",
            "average",
            date_start="2020-01-01",
            date_end="2020-01-03",
            latitude_range=(45, 46),
            longitude_range=(-110, -108),
        )

        assert (data_df.loc[0, "0"]) == "01019000"


def xxtest_check_inputs_failure1():
    """Parameter all_attributes cannot be True if return_metadata is False"""
    with pytest.raises(Exception):
        utils.check_inputs(
            data_source="usgs_nwis",
            variable="streamflow",
            temporal_resolution="daily",
            aggregation="average",
            return_metadata=False,
            all_attributes=True,
        )


def xxtest_check_inputs_failure2():
    """Parameter provided for variable not in supported list (typo)."""
    with pytest.raises(Exception):
        utils.check_inputs(
            data_source="usgs_nwis",
            variable="steamflow",
            temporal_resolution="daily",
            aggregation="average",
        )


def xxtest_check_inputs_failure3():
    """Parameter provided for temporal_resolution not in supported list."""
    with pytest.raises(Exception):
        utils.check_inputs(
            data_source="usgs_nwis",
            variable="streamflow",
            temporal_resolution="monthly",
            aggregation="average",
        )


def _get_var_id():
    create_mock_hydrodata(HYDRODATA)
    create_mock_observations_database(HYDRODATA)
    conn = sqlite3.connect(f"{HYDRODATA}/national_obs/point_obs.sqlite")

    # Build SQL connection to mock HydroData database
    assert (
        utils.get_var_id(
            conn,
            data_source="usgs_nwis",
            variable="streamflow",
            temporal_resolution="hourly",
            aggregation="average",
        )
        == 1
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usgs_nwis",
            variable="streamflow",
            temporal_resolution="daily",
            aggregation="average",
        )
        == 2
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usgs_nwis",
            variable="wtd",
            temporal_resolution="hourly",
            aggregation="average",
        )
        == 3
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usgs_nwis",
            variable="wtd",
            temporal_resolution="daily",
            aggregation="average",
        )
        == 4
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usgs_nwis",
            variable="wtd",
            temporal_resolution="instantaneous",
            aggregation="instantaneous",
        )
        == 5
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usda_nrcs",
            variable="swe",
            temporal_resolution="daily",
            aggregation="start-of-day",
        )
        == 6
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usda_nrcs",
            variable="precipitation",
            temporal_resolution="daily",
            aggregation="accumulated",
        )
        == 7
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usda_nrcs",
            variable="precipitation",
            temporal_resolution="daily",
            aggregation="total",
        )
        == 8
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usda_nrcs",
            variable="precipitation",
            temporal_resolution="daily",
            aggregation="total, snow-adjusted",
        )
        == 9
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usda_nrcs",
            variable="temperature",
            temporal_resolution="daily",
            aggregation="minimum",
        )
        == 10
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usda_nrcs",
            variable="temperature",
            temporal_resolution="daily",
            aggregation="maximum",
        )
        == 11
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usda_nrcs",
            variable="temperature",
            temporal_resolution="daily",
            aggregation="average",
        )
        == 12
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usda_nrcs",
            variable="soil moisture",
            temporal_resolution="daily",
            aggregation="start-of-day",
            depth_level=2,
        )
        == 13
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usda_nrcs",
            variable="soil moisture",
            temporal_resolution="daily",
            aggregation="start-of-day",
            depth_level=4,
        )
        == 14
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usda_nrcs",
            variable="soil moisture",
            temporal_resolution="daily",
            aggregation="start-of-day",
            depth_level=8,
        )
        == 15
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usda_nrcs",
            variable="soil moisture",
            temporal_resolution="daily",
            aggregation="start-of-day",
            depth_level=20,
        )
        == 16
    )
    assert (
        utils.get_var_id(
            conn,
            data_source="usda_nrcs",
            variable="soil moisture",
            temporal_resolution="daily",
            aggregation="start-of-day",
            depth_level=40,
        )
        == 17
    )
    cleanup_mock_hydrodata(HYDRODATA)


def xxtest_filter_min_num_obs():
    df = pd.DataFrame(
        {
            "site_id": ["101", "102", "103", "104", "105"],
            "date1": [1, 5, 3, 4, 8],
            "date2": [np.nan, 4, 2, 9, 4],
            "date3": [np.nan, 9, 2, np.nan, 9],
        }
    )

    assert len(utils.filter_min_num_obs(df, 1)) == 5
    assert len(utils.filter_min_num_obs(df, 2)) == 4
    assert len(utils.filter_min_num_obs(df, 3)) == 3


if __name__ == "__main__":
    pytest.main()

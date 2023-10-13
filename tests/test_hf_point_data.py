import sys
import os
import io
import pytest
import sqlite3
from unittest import mock
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from hf_point_data import hf_point_data, utils  # noqa

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


def test_check_inputs():
    """Confirm utils.check_inputs fails for expected cases."""
    # Parameter provided for variable not in supported list (typo).
    with pytest.raises(Exception):
        utils.check_inputs(
            data_source="usgs_nwis",
            variable="steamflow",
            temporal_resolution="daily",
            aggregation="average",
        )

    # Parameter provided for temporal_resolution not in supported list.
    with pytest.raises(Exception):
        utils.check_inputs(
            data_source="usgs_nwis",
            variable="streamflow",
            temporal_resolution="monthly",
            aggregation="average",
        )

    # Variable requested is soil moisture but no depth level provided.
    with pytest.raises(Exception):
        utils.check_inputs(
            data_source="usda_nrcs",
            variable="soil moisture",
            temporal_resolution="daily",
            aggregation="start-of-day",
        )

    # Variable requested is soil moisture with unsupported depth level provided.
    with pytest.raises(Exception):
        utils.check_inputs(
            data_source="usda_nrcs",
            variable="soil moisture",
            temporal_resolution="daily",
            aggregation="start-of-day",
            depth_level=6
        )


def test_filter_min_num_obs():
    """Test functionality for filtering DataFrame on minimum non-NaN values."""
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


def test_get_network_site_list():
    """Confirm expected number of sites in each network list."""
    assert len(utils.get_network_site_list('usgs_nwis', 'streamflow', ['gagesii'])) == 9067
    assert len(utils.get_network_site_list('usgs_nwis', 'streamflow', ['gagesii_reference'])) == 1947
    assert len(utils.get_network_site_list('usgs_nwis', 'streamflow', ['hcdn2009'])) == 704
    assert len(utils.get_network_site_list('usgs_nwis', 'streamflow', ['camels'])) == 671
    assert len(utils.get_network_site_list('usgs_nwis', 'wtd', ['climate_response_network'])) == 718


def test_get_network_site_list_intersect():
    """Confirm function doesn't return duplicate site IDs if multiple networks listed."""
    assert len(utils.get_network_site_list('usgs_nwis', 'streamflow', ['gagesii', 'gagesii_reference'])) == 1947


if __name__ == "__main__":
    pytest.main()

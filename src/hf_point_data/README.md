
This module provides functionality to compile point observations data for a variety of hydrologic variables. All source data comes from public sources that has been downloaded locally to `/hydrodata` on Verde.

Note that raw hourly data is saved in UTC while raw daily data is saved with respect to the local site time zone. This is what currently gets returned with each of the `hourly` and `daily` aggregation parameters. Coming soon: the ability for a user to specify whether data gets returned in UTC or local time, regardless of how the raw data is structured.

## Data Sources

Data sources that can be accessed with this function currently include:

<table>
    <thead>
        <tr>
            <th>data_source</th>
            <th>variable</th>
            <th>temporal_resolution</th>
            <th>aggregation</th>
            <th>depth_level</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td rowspan=5>'usgs_nwis'</td>
            <td rowspan=2>'streamflow'</td>
            <td>'hourly'</td>
            <td>'average'</td>
        </tr>
        <tr>
            <td>'daily'</td>
            <td>'average'</td>
        </tr>
        <tr>
            <td rowspan=3>'wtd'</td>
            <td>'hourly'</td>
            <td>'average'</td>
        </tr>
        <tr>
            <td>'daily'</td>
            <td>'average'</td>
        </tr>
        <tr>
            <td>'instantaneous'</td>
            <td>'instantaneous'</td>
        </tr>
        <tr>
            <td rowspan=8>'usda_nrcs'</td>
            <td rowspan=1>'swe'</td>
            <td>'daily'</td>
            <td>'start-of-day'</td>
        </tr>
        <tr>
            <td rowspan=3>'precipitation'</td>
            <td>'daily'</td>
            <td>'accumulated'</td>
        </tr>
        <tr>
            <td>'daily'</td>
            <td>'total'</td>
        </tr>
        <tr>
            <td>'daily'</td>
            <td>'total, snow-adjusted'</td>
        </tr>
        <tr>
            <td rowspan=3>'temperature'</td>
            <td>'daily'</td>
            <td>'minimum'</td>
        </tr>
        <tr>
            <td>'daily'</td>
            <td>'maximum'</td>
        </tr>
        <tr>
            <td>'daily'</td>
            <td>'average'</td>
        </tr>
        <tr>
            <td rowspan=1>'soil moisture'</td>
            <td>'daily'</td>
            <td>'start-of-day'</td>
            <td>2, 4, 8, 20, or 40 (inches)</td>
        </tr>
        <tr>
            <td rowspan=8>'ameriflux'</td>
            <tr>
                <td rowspan=1>'latent heat flux'</td>
                <td>'hourly'</td>
                <td>'total'</td>
            </tr>
            <tr>
                <td rowspan=1>'sensible heat flux'</td>
                <td>'hourly'</td>
                <td>'total'</td>
            </tr>
            <tr>
                <td rowspan=1>'shortwave radiation'</td>
                <td>'hourly'</td>
                <td>'average'</td>
            </tr>
            <tr>
                <td rowspan=1>'longwave radiation'</td>
                <td>'hourly'</td>
                <td>'average'</td>
            </tr>
            <tr>
                <td rowspan=1>'vapor pressure deficit'</td>
                <td>'hourly'</td>
                <td>'average'</td>
            </tr>
            <tr>
                <td rowspan=1>'temperature'</td>
                <td>'hourly'</td>
                <td>'average'</td>
            </tr>
            <tr>
                <td rowspan=1>'wind speed'</td>
                <td>'hourly'</td>
                <td>'average'</td>
            </tr>
        </tr>
    </tbody>
</table>

Note that the field, *depth_level*, needs only be provided when querying soil moisture data.

We are under active development and anticipate regularly incorporating additional sources.

## Examples

The following code examples need to be run from a location with access to `/hydrodata`. For most users, this means these scripts must be run from Verde.

### **See what observation types are available**

To see what types of observations are currently available for use in HydroData, call the utility method `list_available_sources()`. This will produce a printout of the variable description and units. In addition, the input parameters of data_source, variable, temporal_resolution and aggregation are listed for each.

```
import hydrodata.point_observations.utilities
hydrodata.point_observations.utilities.list_available_sources()
```

### **Get observations data and (optional) associated site metadata**

The function `get_pandas_observations` returns the requested point observations data as a pandas DataFrame. Users must include the `data_source`, `variable`, `temporal_resoultion`, and `aggregation` parameters. Please see the top of this README document for a table of available options for these fields. Optional parameters allow for filtering observations to those within a specific date range (as 'YYYY-MM-DD') and/or geographic region (via latitude and/or longitude bounds, state, and/or site ID).

Optionally, users can request to return a DataFrame that contains metadata about the sites that fulfill the request. If the parameter `return_metadata` is set to `True`, users have the additional option of whether to return the complete set of attributes available or whether to return a select subset via the `all_attributes` parameter. If `all_attributes=False`, only the following site attributes are included in the metadata DataFrame: site_id, site_name, site_type, agency, state, latitude, longitude, var_id, first_date_data_available, last_date_data_available, record_count, file_path. Note that here record_count refers to the overall count of records available for this type of observation for that site. It does not account for filtering to a specific time range. 

Such filtering - to only include observations that have a minimum number of observations within a specified time range - can be accomplished via the `min_num_obs` parameter.

```
import hydrodata.point_observations.pandas.collect_observations
hydrodata.point_observations.pandas.collect_observations.get_pandas_observations(
    data_source, variable, temporal_resolution,
    aggregation,
    date_start=None, date_end=None,
    latitude_range=None, longitude_range=None,
    site_id=None, state=None, min_num_obs=1,
    return_metadata=False, all_attributes=False)
```

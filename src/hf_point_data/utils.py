'''
Point observations utility functions.

Note that these functions are not intended to be used stand-alone; they act as sub-processes
within the collect_observations.get_pandas_observations method.
'''
import pandas as pd
import sqlite3
import datetime as dt
import numpy as np
import xarray as xr


def check_inputs(data_source, variable, temporal_resolution, aggregation, **kwargs):
    """
    Checks on inputs to get_observations function.

    Parameters
    ----------
    data_source : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'usda_nrcs', 
        'ameriflux'.
    variable : str
        Description of type of data requested. Currently supported: 'streamflow', 'wtd', 'swe', 
        'precipitation', 'temperature', 'soil moisture', 'latent heat flux', 'sensible heat flux', 
        'shortwave radiation', 'longwave radiation', 'vapor pressure deficit', 'wind speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned. 
        Options include descriptors such as 'average' and 'total'. Please see the README documentation
        for allowable combinations with `variable`.
    depth_level : int
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil moisture'.

    Returns
    -------
    None
    """
    assert temporal_resolution in ['daily', 'hourly', 'instantaneous']
    assert variable in ['streamflow', 'wtd', 'swe', 'precipitation', 'temperature', 'soil moisture',
                        'latent heat flux', 'sensible heat flux', 'shortwave radiation', 'longwave radiation',
                        'vapor pressure deficit', 'wind speed']
    assert aggregation in ['average', 'instantaneous', 'total', 'total, snow-adjusted',
                           'start-of-day', 'accumulated', 'minimum', 'maximum']
    assert data_source in ['usgs_nwis', 'usda_nrcs', 'ameriflux']

    if variable == 'soil moisture':
        assert 'depth_level' in kwargs
        assert kwargs['depth_level'] in [2, 4, 8, 20, 40]


def get_var_id(conn, data_source, variable, temporal_resolution, aggregation, **kwargs):
    """
    Return mapped var_id.

    Parameters
    ----------
    conn : Connection object
        The Connection object associated with the SQLite database to 
        query from. 
    data_source : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'usda_nrcs', 
        'ameriflux'.    
    variable : str
        Description of type of data requested. Currently supported: 'streamflow', 'wtd', 'swe', 
        'precipitation', 'temperature', 'soil moisture', 'latent heat flux', 'sensible heat flux', 
        'shortwave radiation', 'longwave radiation', 'vapor pressure deficit', 'wind speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned. 
        Options include descriptors such as 'average' and 'total'. Please see the README documentation
        for allowable combinations with `variable`.
    depth_level : int
        Depth level in inches at which the measurement is taken. Necessary for variable='soil moisture'.

    Returns
    -------
    var_id : int
        Integer variable ID associated with combination of `data_source`, `variable`, `temporal_resolution`,
        and `aggregation`.
    """
    if variable == 'soil moisture':
        query = """
                SELECT var_id 
                FROM variables
                WHERE data_source = ?
                    AND variable = ?
                    AND temporal_resolution = ?
                    AND aggregation = ?
                    AND depth_level = ?
                """
        param_list = [data_source, variable, temporal_resolution, aggregation, kwargs['depth_level']]

    else:
        query = """
                SELECT var_id 
                FROM variables
                WHERE data_source = ?
                    AND variable = ?
                    AND temporal_resolution = ?
                    AND aggregation = ?
                """
        param_list = [data_source, variable, temporal_resolution, aggregation]

    try:
        result = pd.read_sql_query(query, conn, params=param_list)
        return int(result['var_id'][0])
    except:
        raise ValueError(
            'The provided combination of data_source, variable, temporal_resolution, and aggregation is not currently supported.')


def get_dirpath(var_id):
    """
    Map variable with location of data on /hydrodata.

    Parameters
    ----------
    var_id : int
        Integer variable ID associated with combination of `data_source`, 
        `variable`, `temporal_resolution`, and `aggregation`.

    Returns
    -------
    dirpath : str
        Directory path for observation data location.
    """
    dirpath_map = {1: '/hydrodata/national_obs/streamflow/data/hourly',
                   2: '/hydrodata/national_obs/streamflow/data/daily',
                   3: '/hydrodata/national_obs/groundwater/data/hourly',
                   4: '/hydrodata/national_obs/groundwater/data/daily',
                   5: '',
                   6: '/hydrodata/national_obs/swe/data/daily',
                   7: '/hydrodata/national_obs/point_meteorology/NRCS_precipitation/data/daily',
                   8: '/hydrodata/national_obs/point_meteorology/NRCS_precipitation/data/daily',
                   9: '/hydrodata/national_obs/point_meteorology/NRCS_precipitation/data/daily',
                   10: '/hydrodata/national_obs/point_meteorology/NRCS_temperature/data/daily',
                   11: '/hydrodata/national_obs/point_meteorology/NRCS_temperature/data/daily',
                   12: '/hydrodata/national_obs/point_meteorology/NRCS_temperature/data/daily',
                   13: '/hydrodata/national_obs/soil_moisture/data/daily',
                   14: '/hydrodata/national_obs/soil_moisture/data/daily',
                   15: '/hydrodata/national_obs/soil_moisture/data/daily',
                   16: '/hydrodata/national_obs/soil_moisture/data/daily',
                   17: '/hydrodata/national_obs/soil_moisture/data/daily',
                   18: '/hydrodata/national_obs/ameriflux/data/hourly',
                   19: '/hydrodata/national_obs/ameriflux/data/hourly',
                   20: '/hydrodata/national_obs/ameriflux/data/hourly',
                   21: '/hydrodata/national_obs/ameriflux/data/hourly',
                   22: '/hydrodata/national_obs/ameriflux/data/hourly',
                   23: '/hydrodata/national_obs/ameriflux/data/hourly',
                   24: '/hydrodata/national_obs/ameriflux/data/hourly'}

    return dirpath_map[var_id]


def get_sites(conn, data_source, variable, temporal_resolution, aggregation, **kwargs):
    """
    Build DataFrame with site attribute metadata information.

    Parameters
    ----------
    conn : Connection object
        The Connection object associated with the SQLite database to 
        query from. 
    data_source : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'usda_nrcs', 
        'ameriflux'.   
    variable : str
        Description of type of data requested. Currently supported: 'streamflow', 'wtd', 'swe', 
        'precipitation', 'temperature', 'soil moisture', 'latent heat flux', 'sensible heat flux', 
        'shortwave radiation', 'longwave radiation', 'vapor pressure deficit', 'wind speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
        Please see the README documentation for allowable combinations with `variable`.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned. 
        Options include descriptors such as 'average' and 'total'. Please see the README documentation
        for allowable combinations with `variable`.
    **depth_level : int
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil moisture'.
    **date_start : str; default=None
        'YYYY-MM-DD' format date indicating beginning of time range.
    **date_end : str; default=None
        'YYYY-MM-DD' format date indicating end of time range.
    **latitude_range : tuple; default=None
        Latitude range bounds for the geographic domain; lesser value is provided first.
    **longitude_range : tuple; default=None
        Longitude range bounds for the geographic domain; lesser value is provided first.
    **site_ids : list; default=None
        List of desired (string) site identifiers.
    **state : str; default=None
        Two-letter postal code state abbreviation.
    **site_networks: list
        List of names of site networks. Can be a list with a single network name.
        Each network must have matching .csv file with a list of site ID values that comprise
        the network. This .csv file must be located under network_lists/{data_source}/{variable}
        in the package directory and named as 'network_name'.csv. Eg: `site_networks=['gagesii']`

    Returns
    -------
    DataFrame
        Site-level DataFrame of attribute metadata information.

    Notes
    -----
    The returned field 'record_count' is OVERALL record count. Filtering of metadata 
    only applies at the site level, so only sites within the provided bounds 
    (space and time) are included. The record count does not reflect any filtering 
    at the data/observation level.
    """

    # Get associated variable IDs for requested data types and time periods
    var_id = get_var_id(conn, data_source, variable, temporal_resolution, aggregation, **kwargs)

    param_list = [var_id]

    # Date start
    if 'date_start' in kwargs:
        date_start_query = """ AND last_date_data_available >= ?"""
        param_list.append(kwargs['date_start'])
    else:
        date_start_query = """"""

    # Date end
    if 'date_end' in kwargs:
        date_end_query = """ AND first_date_data_available <= ?"""
        param_list.append(kwargs['date_end'])
    else:
        date_end_query = """"""

    # Latitude
    if 'latitude_range' in kwargs:
        lat_query = """ AND latitude BETWEEN ? AND ?"""
        param_list.append(kwargs['latitude_range'][0])
        param_list.append(kwargs['latitude_range'][1])
    else:
        lat_query = """"""

    # Longitude
    if 'longitude_range' in kwargs:
        lon_query = """ AND longitude BETWEEN ? AND ?"""
        param_list.append(kwargs['longitude_range'][0])
        param_list.append(kwargs['longitude_range'][1])
    else:
        lon_query = """"""

    # Site ID
    if 'site_ids' in kwargs:
        site_query = """ AND s.site_id IN (%s)""" % ','.join('?'*len(kwargs['site_ids']))
        for s in kwargs['site_ids']:
            param_list.append(s)
    else:
        site_query = """"""

    # State
    if 'state' in kwargs:
        state_query = """ AND state == ?"""
        param_list.append(kwargs['state'])
    else:
        state_query = """"""

    # Site Networks
    if 'site_networks' in kwargs:
        network_site_list = get_network_site_list(data_source, variable, kwargs['site_networks'])
        network_query = """ AND s.site_id IN (%s)""" % ','.join('?'*len(network_site_list))
        for s in network_site_list:
            param_list.append(s)
    else:
        network_query = """"""

    query = """
            SELECT s.site_id, s.site_name, s.site_type, s.agency, s.state,
                   s.latitude, s.longitude, s.huc, o.first_date_data_available,
                   o.last_date_data_available, o.record_count, s.site_query_url,
                   s.date_metadata_last_updated, s.tz_cd, s.doi
            FROM sites s
            INNER JOIN observations o
            ON s.site_id = o.site_id AND o.var_id == ?
            WHERE first_date_data_available <> 'None'
            """ + date_start_query + date_end_query + lat_query + lon_query + site_query + state_query + network_query

    df = pd.read_sql_query(query, conn, params=param_list)

    return df


def get_network_site_list(data_source, variable, site_networks):
    """
    Return list of site IDs for desired network of observation sites.

    Parameters
    ----------
    data_source : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'usda_nrcs', 
        'ameriflux'.   
    variable : str
        Description of type of data requested. Currently supported: 'streamflow', 'wtd', 'swe', 
        'precipitation', 'temperature', 'soil moisture', 'latent heat flux', 'sensible heat flux', 
        'shortwave radiation', 'longwave radiation', 'vapor pressure deficit', 'wind speed'.
    site_networks: list
        List of names of site networks. Can be a list with a single network name.
        Each network must have matching .csv file with a list of site ID values that comprise
        the network. This .csv file must be located under network_lists/{data_source}/{variable}
        in the package directory and named as 'network_name'.csv.

    Returns
    -------
    site_list: list
        List of site ID strings for sites belonging to named network.
    """
    network_options = {'usgs_nwis': {'streamflow': ['camels', 'gagesii_reference', 'gagesii', 'hcdn2009'],
                                     'wtd': ['climate_response_network']}}

    # Initialize final site list
    site_list = []

    # Append sites from desired network(s)
    for network in site_networks:
        try:
            assert network in network_options[data_source][variable]
            df = pd.read_csv(f'network_lists/{data_source}/{variable}/{network}.csv',
                             dtype=str, header=None, names=['site_id'])
            site_list += list(df['site_id'])
        except:
            raise ValueError(
                f'Network option {network} is not recognized. Please make sure the .csv network_lists/{data_source}/{variable}/{network}.csv exists.')

    # Make sure only list of unique site IDs is returned (in case multiple, overlapping networks provided)
    # Note: calling 'set' can change the order of the IDs, but for this workflow that does not matter
    return list(set(site_list))


def clean_huc(huc):
    """
    Clean up and standardize HUC8 values.

    Parameters
    ----------
    huc : str
        Single string value representing a HUC code.

    Returns
    -------
    cleaned_huc : str
        HUC8 code or '' if not enough information available.
    """
    # Clean out HUC values that are fewer than 7 digits
    huc_length = len(huc)
    if huc_length < 7:
        cleaned_huc = ''

    # If 7 or 11 digits, add a leading 0
    elif len(huc) in (7, 11):
        huc = '0' + huc

    # Truncate to HUC8 for 'least common denominator' level
    if len(huc) >= 8:
        cleaned_huc = huc[0:8]

    return cleaned_huc


def convert_to_pandas(ds):
    """
    Convert xarray DataSet to pandas DataFrame.

    Parameters
    ----------
    ds : DataSet
        xarray DataSet containing stacked observations data for a 
        single variable. 
    var_id : int
        Integer variable ID associated with combination of `variable`, `temporal_resolution`,
        and `aggregation`.

    Returns
    -------
    DataFrame
        Stacked observations data for a single variable.
    """
    sites = pd.Series(ds['site'].to_numpy())
    dates = pd.Series(ds['date'].to_numpy()).astype(str)
    data = ds.to_numpy()

    df = pd.DataFrame(data.T, columns=sites)
    df['date'] = dates

    # Reorder columns to put site_id first
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]

    return df


def filter_min_num_obs(df, min_num_obs):
    """
    Filter to only sites which have a minimum number of observations.

    This filtering is done after the observations are subset by time, so these
    observation counts will only filter out sites if the number of observations *within 
    that time range* is not satisfied.

    Parameters
    ----------
    df : DataFrame
        Stacked observations data for a single variable.
    min_num_obs : int
        Value for the minimum number of observations desired for a site to have.

    Returns
    -------
    DataFrame
        Stacked observations data for a single variable, filtered to only sites that
        have the minimum number of observations specified.
    """
    dfc = df.copy()

    # drop columns with too many NaN values
    df_filtered = dfc.dropna(thresh=min_num_obs, axis=1)

    return df_filtered


def get_data_nc(site_list, var_id, **kwargs):
    """
    Get observations data for data that is stored in NetCDF files.

    Parameters
    ----------
    site_list : list
        List of site IDs to query observations data for.
    var_id : int
        Integer variable ID associated with combination of `data_source`,
        `variable`, `temporal_resolution`, and `aggregation`.
    date_start : str; default=None
        'YYYY-MM-DD' format date indicating beginning of time range.
    date_end : str; default=None
        'YYYY-MM-DD' format date indicating end of time range.
    min_num_obs : int
        Value for the minimum number of observations desired for a site to have.

    Returns
    -------
    DataFrame
        Stacked observations data for a single variable, filtered to only sites that
        have the minimum number of observations specified.
    """
    dirpath = get_dirpath(var_id)
    file_list = [f'{dirpath}/{site}.nc' for site in site_list]

    varname_map = {'1': 'streamflow', '2': 'streamflow', '3': 'wtd', '4': 'wtd', '5': 'wtd',
                   '6': 'swe', '7': 'precip_acc', '8': 'precip_inc', '9': 'precip_inc_sa',
                   '10': 'temp_min', '11': 'temp_max', '12': 'temp_avg',
                   '13': 'sms_2in', '14': 'sms_4in', '15': 'sms_8in', '16': 'sms_20in', '17': 'sms_40in',
                   '18': 'latent heat flux', '19': 'sensible heat flux', '20': 'shortwave radiation',
                   '21': 'longwave radiation', '22': 'vapor pressure deficit', '23': 'air temperature',
                   '24': 'wind speed'}

    varname = varname_map[str(var_id)]

    if 'date_start' in kwargs:
        date_start_dt = np.datetime64(kwargs['date_start'])
    if 'date_end' in kwargs:
        date_end_dt = np.datetime64(kwargs['date_end'])

    print('collecting data...')

    for i in range(len(site_list)):

        # open single site file
        temp = xr.open_dataset(file_list[i])[varname]

        # make date variable name consistent
        date_var = list(temp.coords)[0]
        temp = temp.rename({date_var: 'datetime'})

        # convert date string to datetime values
        temp['datetime'] = pd.DatetimeIndex(temp['datetime'].values)

        # subset to only observations within desired time range
        if ('date_start' not in kwargs) and ('date_end' not in kwargs):
            temp_wy = temp
        elif ('date_start' not in kwargs) and ('date_end' in kwargs):
            temp_wy = temp.sel(datetime=(temp.datetime <= date_end_dt))
        elif ('date_start' in kwargs) and ('date_end' not in kwargs):
            temp_wy = temp.sel(datetime=(temp.datetime >= date_start_dt))
        elif ('date_start' in kwargs) and ('date_end' in kwargs):
            temp_wy = temp.sel(datetime=(temp.datetime >= date_start_dt) & (temp.datetime <= date_end_dt))

        if i == 0:
            ds = temp_wy
        else:
            ds = xr.concat([ds, temp_wy], dim='site')

    if len(site_list) == 1:
        ds = ds.expand_dims(dim='site')

    ds = ds.assign_coords({'site': (site_list)})
    ds = ds.rename({'datetime': 'date'})

    print('data collected.')

    data_df = convert_to_pandas(ds)
    if 'min_num_obs' in kwargs:
        return filter_min_num_obs(data_df, kwargs['min_num_obs'])
    else:
        return data_df


def get_data_sql(conn, var_id, **kwargs):
    """
    Get observations data for data that is stored in a SQL table.

    Parameters
    ----------
    conn : Connection object
        The Connection object associated with the SQLite database to 
        query from. 
    var_id : int
        Integer variable ID associated with combination of `data_source`, 
        `variable`, `temporal_resolution`, and `aggregation`.
    date_start : str; default=None
        'YYYY-MM-DD' format date indicating beginning of time range.
    date_end : str; default=None
        'YYYY-MM-DD' format date indicating end of time range.
    min_num_obs : int
        Value for the minimum number of observations desired for a site to have.

    Returns
    -------
    DataFrame
        Stacked observations data for a single variable, filtered to only sites that
        have the minimum number of observations specified.
    """
    assert var_id == 5

    # Note:
    #   pumping_status == '1' --> Static (not pumping)
    #   pumping_status == 'P' --> Pumping
    #   pumping_status == '' --> unknown (not reported)
    if 'min_num_obs' not in kwargs:
        min_num_obs = 1
    else:
        min_num_obs = kwargs['min_num_obs']

    if ('date_start' not in kwargs) and ('date_end' not in kwargs):
        date_query = """"""
        param_list = [min_num_obs]
    elif ('date_start' not in kwargs) and ('date_end' in kwargs):
        date_query = """ WHERE w.date <= ?"""
        param_list = [kwargs['date_end'], min_num_obs, kwargs['date_end']]
    elif ('date_start' in kwargs) and ('date_end' not in kwargs):
        date_query = """ WHERE w.date >= ?"""
        param_list = [kwargs['date_start'], min_num_obs, kwargs['date_start']]
    elif ('date_start' in kwargs) and ('date_end' in kwargs):
        date_query = """ WHERE w.date >= ? AND w.date <= ?"""
        param_list = [kwargs['date_start'], kwargs['date_end'], min_num_obs, kwargs['date_start'], kwargs['date_end']]

    query = """
            SELECT w.site_id, w.date, w.wtd, w.pumping_status
            FROM wtd_discrete_data AS w
            INNER JOIN (SELECT w.site_id, COUNT(*) AS num_obs
                FROM wtd_discrete_data AS w
                """ + date_query + """
                GROUP BY site_id
                HAVING num_obs >= ?) AS c
            ON w.site_id = c.site_id
            """ + date_query

    df = pd.read_sql_query(query, conn, params=param_list)

    return df

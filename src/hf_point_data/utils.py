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


def check_inputs(data_source, variable, temporal_resolution, aggregation, depth_level):
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
    assert depth_level in [2, 4, 8, 20, 40, None]

    if variable == 'soil moisture':
        assert depth_level is not None


def get_var_id(conn, data_source, variable, temporal_resolution, aggregation, depth_level=None):
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
        param_list = [data_source, variable, temporal_resolution, aggregation, depth_level]

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

    df = pd.DataFrame(data, columns=dates)
    df['site_id'] = sites

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

    # count number of non-na observations
    dfc['num_obs'] = dfc.count(axis=1) - 1

    # filter based on this field
    df_filtered = dfc[dfc['num_obs'] >= min_num_obs]

    return df_filtered


def get_data_nc(site_list, var_id, date_start, date_end, min_num_obs):
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

    if date_start != None:
        date_start_dt = np.datetime64(date_start)
    if date_end != None:
        date_end_dt = np.datetime64(date_end)

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
        if (date_start == None) and (date_end == None):
            temp_wy = temp
        elif (date_start == None) and (date_end != None):
            temp_wy = temp.sel(datetime=(temp.datetime <= date_end_dt))
        elif (date_start != None) and (date_end == None):
            temp_wy = temp.sel(datetime=(temp.datetime >= date_start_dt))
        elif (date_start != None) and (date_end != None):
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
    final_data_df = filter_min_num_obs(data_df, min_num_obs)

    return final_data_df


def get_data_sql(conn, var_id, date_start, date_end, min_num_obs):
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

    if (date_start == None) and (date_end == None):
        date_query = """"""
        param_list = [min_num_obs]
    elif (date_start == None) and (date_end != None):
        date_query = """ WHERE w.date <= ?"""
        param_list = [date_end, min_num_obs, date_end]
    elif (date_start != None) and (date_end == None):
        date_query = """ WHERE w.date >= ?"""
        param_list = [date_start, min_num_obs, date_start]
    elif (date_start != None) and (date_end != None):
        date_query = """ WHERE w.date >= ? AND w.date <= ?"""
        param_list = [date_start, date_end, min_num_obs, date_start, date_end]

    query = """
            SELECT w.site_id, w.date, w.wtd, w.pumping_status, num_obs
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

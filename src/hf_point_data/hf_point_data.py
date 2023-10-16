import pandas as pd
import sqlite3
import datetime as dt
import numpy as np
import xarray as xr

import hf_point_data.utils as utils

HYDRODATA = '/hydrodata'
DB_PATH = f'{HYDRODATA}/national_obs/point_obs.sqlite'


def get_data(data_source, variable, temporal_resolution, aggregation, **kwargs):
    """
    Collect observations data into a Pandas DataFrame.

    Observations collected from HydroData for the specified data source, variable, temporal 
    resolution, and aggregation. Optional arguments can be supplied for filters such as
    date bounds, geography bounds, and/or the minimum number of per-site observations allowed.

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
        Please see the README documentation for allowable combinations with `variable`.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned. 
        Options include descriptors such as 'average' and 'total'. Please see the README documentation
        for allowable combinations with `variable`.
    **depth_level : int
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil moisture'.
    **date_start : str; default=None
        'YYYY-MM-DD' date indicating beginning of time range.
    **date_end : str; default=None
        'YYYY-MM-DD' date indicating end of time range.
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
    **min_num_obs : int; default=1
        Value for the minimum number of observations desired for a site to have.

    Returns
    -------
    data_df : DataFrame
        Stacked observations data for a single variable, filtered to only sites that
        (optionally) have the minimum number of observations specified, within the 
        defined geographic and/or date range.
    """

    # Create database connection
    conn = sqlite3.connect(DB_PATH)

    # Validation checks on inputs
    utils.check_inputs(data_source, variable, temporal_resolution, aggregation, **kwargs)

    # Get associated variable IDs for requested data types and time periods
    var_id = utils.get_var_id(conn, data_source, variable, temporal_resolution, aggregation, **kwargs)

    # Get site list
    sites_df = utils.get_sites(conn, data_source, variable,
                               temporal_resolution, aggregation, **kwargs)

    if len(sites_df) == 0:
        raise ValueError('There are zero sites that satisfy the given parameters.')

    # Get data
    site_list = list(sites_df['site_id'])

    if (var_id in (1, 2, 3, 4)) | (var_id in range(6, 25)):
        data_df = utils.get_data_nc(site_list, var_id, **kwargs)

    elif var_id == 5:
        data_df = utils.get_data_sql(conn, var_id, **kwargs)

    conn.close()

    return data_df.reset_index().drop('index', axis=1)


def get_metadata(data_source, variable, temporal_resolution, aggregation, **kwargs):
    """
    Return DataFrame with site metadata for the requested site IDs.

    Parameters
    ----------
    site_ids : list; default=None
        List of desired (string) site identifiers.

    Returns
    -------
    DataFrame
        Site-level DataFrame of site-level metadata.
    """
    # Create database connection
    conn = sqlite3.connect(DB_PATH)

    metadata_df = utils.get_sites(conn, data_source, variable,
                                  temporal_resolution, aggregation, **kwargs)

    # Clean up HUC to string of appropriate length
    metadata_df['huc8'] = metadata_df['huc'].apply(lambda x: utils.clean_huc(x))
    metadata_df.drop(columns=['huc'], inplace=True)

    # Merge on additional metadata attribute tables as needed
    site_ids = list(metadata_df['site_id'])

    if 'stream gauge' in metadata_df['site_type'].unique():
        attributes_df = pd.read_sql_query(
            """SELECT * FROM streamgauge_attributes WHERE site_id IN (%s)""" % ','.join('?' * len(site_ids)),
            conn, params=site_ids)
        metadata_df = pd.merge(metadata_df, attributes_df, how='left', on='site_id')

    if 'groundwater well' in metadata_df['site_type'].unique():
        attributes_df = pd.read_sql_query(
            """SELECT * FROM well_attributes WHERE site_id IN (%s)""" % ','.join('?' * len(site_ids)),
            conn, params=site_ids)
        metadata_df = pd.merge(metadata_df, attributes_df, how='left', on='site_id')

    if 'SNOTEL station' or 'SCAN station' in metadata_df['site_type'].unique():
        attributes_df = pd.read_sql_query(
            """SELECT * FROM snotel_station_attributes WHERE site_id IN (%s)""" % ','.join('?' * len(site_ids)),
            conn, params=site_ids)
        metadata_df = pd.merge(metadata_df, attributes_df, how='left', on='site_id')

    if 'flux tower' in metadata_df['site_type'].unique():
        attributes_df = pd.read_sql_query(
            """SELECT * FROM flux_tower_attributes WHERE site_id IN (%s)""" % ','.join('?' * len(site_ids)),
            conn, params=site_ids)
        metadata_df = pd.merge(metadata_df, attributes_df, how='left', on='site_id')

    conn.close()
    return metadata_df


def get_citations(data_source, site_ids=None):
    """
    Print and/or return specific citation information for requested data source.

    Parameters
    ----------
    data_source : str
        Source from which data originates. Options include: 'usgs_nwis', 'usda_nrcs', and 
        'ameriflux'.
    site_ids : list; default None
        If provided, the specific list of sites to return data DOIs for. This is only
        supported if `data_source` == 'ameriflux'.

    Returns
    -------
    None or DataFrame of site-specific DOIs
        Nothing returned unless data_source == `ameriflux` and the parameter `site_ids` is provided.
    """
    try:
        assert data_source in ['usgs_nwis', 'usda_nrcs', 'ameriflux']
    except:
        raise ValueError(
            f"Unexpected value of data_source, {data_source}. Supported values include 'usgs_nwis', 'usda_nrcs', and 'ameriflux'")

    if data_source == 'usgs_nwis':
        print('''Most U.S. Geological Survey (USGS) information resides in Public Domain 
              and may be used without restriction, though they do ask that proper credit be given.
              An example credit statement would be: "(Product or data name) courtesy of the U.S. Geological Survey"
              Source: https://www.usgs.gov/information-policies-and-instructions/acknowledging-or-crediting-usgs''')

    elif data_source == 'usda_nrcs':
        print('''Most information presented on the USDA Web site is considered public domain information. 
                Public domain information may be freely distributed or copied, but use of appropriate
                byline/photo/image credits is requested. 
                Attribution may be cited as follows: "U.S. Department of Agriculture"
                Source: https://www.usda.gov/policies-and-links''')

    elif data_source == 'ameriflux':
        print('''All AmeriFlux sites provided by the HydroData service follow the CC-BY-4.0 License.
                The CC-BY-4.0 license specifies that the data user is free to Share (copy and redistribute 
                the material in any medium or format) and/or Adapt (remix, transform, and build upon the 
                material) for any purpose.
            
                Users of this data must acknowledge the AmeriFlux data resource with the following statement:
                "Funding for the AmeriFlux data portal was provided by the U.S. Department of Energy Office 
                of Science."
            
                Additionally, for each AmeriFlux site used, you must provide a citation to the site's 
                data product that includes the data product DOI. The DOI for each site is included in the 
                full metadata query. Alternately, a site list can be provided to this get_citation_information
                function to return each site-specific DOI.
            
                Source: https://ameriflux.lbl.gov/data/data-policy/''')

    if site_ids is not None:
        # Create database connection
        conn = sqlite3.connect(DB_PATH)

        query = """
                SELECT site_id, doi 
                FROM sites
                WHERE site_id IN (%s)
                """ % ','.join('?'*len(site_ids))

        df = pd.read_sql_query(query, conn, params=site_ids)
        return df

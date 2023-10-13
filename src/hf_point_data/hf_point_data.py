# pylint: disable=C0301
import json
import io
import sqlite3
import os
import datetime
from typing import Tuple
import ast
import requests
import pandas as pd
import hf_point_data.utils as utils

HYDRODATA = "/hydrodata"
DB_PATH = f"{HYDRODATA}/national_obs/point_obs.sqlite"
HYDRODATA_URL = os.getenv("HYDRODATA_URL", "https://hydro-dev-aj.princeton.edu")


# Need to convert these inputs to options
def get_data(
    data_source,
    variable,
    temporal_resolution,
    aggregation,
    depth_level=None,
    date_start=None,
    date_end=None,
    latitude_range=None,
    longitude_range=None,
    site_ids=None,
    state=None,
    min_num_obs=1,
    return_metadata=False,
    all_attributes=False,
):
    """
    Collect observations data into a Pandas DataFrame.

    Observations collected from HydroData for the specified data source, variable, temporal
    resolution, and aggregation. Optional arguments can be supplied for date bounds, geography bounds,
    the minimum number of per-site observations allowed, and/or whether site metadata
    should also be returned (in a separate DataFrame).

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
    depth_level : int
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil moisture'.
    date_start : str; default=None
        'YYYY-MM-DD' date indicating beginning of time range.
    date_end : str; default=None
        'YYYY-MM-DD' date indicating end of time range.
    latitude_range : tuple; default=None
        Latitude range bounds for the geographic domain; lesser value is provided first.
    longitude_range : tuple; default=None
        Longitude range bounds for the geographic domain; lesser value is provided first.
    site_ids : list; default=None
        List of desired (string) site identifiers.
    state : str; default=None
        Two-letter postal code state abbreviation.
    min_num_obs : int; default=1
        Value for the minimum number of observations desired for a site to have.
    return_metadata : bool; default=False
        Whether to additionally return a DataFrame containing site metadata.
    all_attributes : bool; default=False
        Whether to include all available attributes on returned metadata DataFrame.
    db_path : str
        Full path to location of point observations database.

    Returns
    -------
    data_df : DataFrame
        Stacked observations data for a single variable, filtered to only sites that
        (optionally) have the minimum number of observations specified, within the
        defined geographic and/or date range.
    metadata_df : DataFrame; optional
        Metadata about the sites present in `data_df` for the desired variable.
    """

    run_remote = not os.path.exists(HYDRODATA)

    if run_remote:
        data_df = _get_data_from_api(
            data_source=data_source,
            variable=variable,
            temporal_resolution=temporal_resolution,
            aggregation=aggregation,
            depth_level=depth_level,
            date_start=date_start,
            date_end=date_end,
            latitude_range=latitude_range,
            longitude_range=longitude_range,
            site_ids=site_ids,
            state=state,
            min_num_obs=min_num_obs,
            return_metadata=return_metadata,
            all_attributes=all_attributes,
        )

        return data_df

    (
        depth_level,
        latitude_range,
        longitude_range,
        site_ids,
        min_num_obs,
        return_metadata,
        all_attributes,
    ) = _convert_strings_to_type(
        depth_level,
        latitude_range,
        longitude_range,
        site_ids,
        min_num_obs,
        return_metadata,
        all_attributes,
    )
    # Create database connection
    conn = sqlite3.connect(DB_PATH)

    # Validation checks on inputs
    utils.check_inputs(
        data_source,
        variable,
        temporal_resolution,
        aggregation,
        depth_level,
        return_metadata,
        all_attributes,
    )

    # Get associated variable IDs for requested data types and time periods
    var_id = utils.get_var_id(
        conn, data_source, variable, temporal_resolution, aggregation, depth_level
    )

    # Get site metadata
    metadata_temp = utils.get_observations_metadata(
        conn,
        var_id,
        date_start=date_start,
        date_end=date_end,
        latitude_range=latitude_range,
        longitude_range=longitude_range,
        site_ids=site_ids,
        state=state,
        all_attributes=all_attributes,
    )

    if len(metadata_temp) == 0:
        raise ValueError("There are zero sites that satisfy the given parameters.")

    # Get data
    site_list = list(metadata_temp["site_id"])

    if (var_id in (1, 2, 3, 4)) | (var_id in range(6, 25)):
        data_df = utils.get_data_nc(
            site_list, var_id, date_start, date_end, min_num_obs
        )

    elif var_id == 5:
        data_df = utils.get_data_sql(conn, var_id, date_start, date_end, min_num_obs)

    # Return data
    if return_metadata == True:
        # Filter metadata down to only sites with actual data for the specific date range
        # and/or minimum observation count
        metadata_df = metadata_temp.merge(data_df["site_id"], how="right", on="site_id")
        return (
            data_df.reset_index().drop("index", axis=1),
            metadata_df.reset_index().drop("index", axis=1),
        )

    else:
        return data_df.reset_index().drop("index", axis=1)


def get_citation_information(data_source, site_ids=None):
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
        Nothing returned unless data_source == `ameriflux` and the parameter `site_list` is provided.
    """
    try:
        assert data_source in ["usgs_nwis", "usda_nrcs", "ameriflux"]
    except:
        raise ValueError(
            f"Unexpected value of data_source, {data_source}. Supported values include 'usgs_nwis', 'usda_nrcs', and 'ameriflux'"
        )

    if data_source == "usgs_nwis":
        print(
            """Most U.S. Geological Survey (USGS) information resides in Public Domain 
              and may be used without restriction, though they do ask that proper credit be given.
              An example credit statement would be: "(Product or data name) courtesy of the U.S. Geological Survey"
              Source: https://www.usgs.gov/information-policies-and-instructions/acknowledging-or-crediting-usgs"""
        )

    elif data_source == "usda_nrcs":
        print(
            """Most information presented on the USDA Web site is considered public domain information. 
                Public domain information may be freely distributed or copied, but use of appropriate
                byline/photo/image credits is requested. 
                Attribution may be cited as follows: "U.S. Department of Agriculture"
                Source: https://www.usda.gov/policies-and-links"""
        )

    elif data_source == "ameriflux":
        print(
            """All AmeriFlux sites provided by the HydroData service follow the CC-BY-4.0 License.
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
            
                Source: https://ameriflux.lbl.gov/data/data-policy/"""
        )

    if site_ids is not None:
        # Create database connection
        conn = sqlite3.connect(DB_PATH)

        query = """
                SELECT site_id, doi 
                FROM sites
                WHERE site_id IN (%s)
                """ % ",".join(
            "?" * len(site_ids)
        )

        df = pd.read_sql_query(query, conn, params=site_ids)
        return df


def _get_data_from_api(**kwargs):
    options = kwargs
    options = _convert_params_to_string_dict(options)

    q_params = _construct_string_from_qparams(options)

    point_data_url = f"{HYDRODATA_URL}/api/point-data-app?{q_params}"

    try:
        headers = _validate_user()
        response = requests.get(point_data_url, headers=headers, timeout=180)
        if response.status_code != 200:
            raise ValueError(
                f"The  {point_data_url} returned error code {response.status_code}."
            )

    except requests.exceptions.Timeout as e:
        raise ValueError(f"The point_data_url {point_data_url} has timed out.") from e

    data_df = pd.read_pickle(io.BytesIO(response.content))
    return data_df


def _convert_params_to_string_dict(options):
    """
    Converts types other than strings to strings.

    Parameters
    ----------
    options : dictionary
        request options.
    """

    for key, value in options.items():
        if key == "depth_level":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "latitude_range":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "longitude_range":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "site_ids":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "min_num_obs":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "return_metadata":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "all_attributes":
            if not isinstance(value, str):
                options[key] = str(value)
    return options


def _convert_strings_to_type(
    depth_level,
    latitude_range,
    longitude_range,
    site_ids,
    min_num_obs,
    return_metadata,
    all_attributes,
):
    """
    Converts strings to relevant types.

    Parameters
    ----------
    options : dictionary
        request options.
    """

    if isinstance(depth_level, str):
        depth_level = int(depth_level)
    if isinstance(latitude_range, str):
        latitude_range = ast.literal_eval(latitude_range)
    if isinstance(longitude_range, str):
        longitude_range = ast.literal_eval(longitude_range)
    if isinstance(site_ids, str):
        site_ids = ast.literal_eval(site_ids)
    if isinstance(min_num_obs, str):
        min_num_obs = int(min_num_obs)
    if isinstance(return_metadata, str):
        return_metadata = bool(return_metadata)
    if isinstance(all_attributes, str):
        all_attributes = bool(all_attributes)

    return (
        depth_level,
        latitude_range,
        longitude_range,
        site_ids,
        min_num_obs,
        return_metadata,
        all_attributes,
    )


def _construct_string_from_qparams(options):
    """
    Constructs the query parameters from the entry and options provided.

    Parameters
    ----------
    entry : hydroframe.data_catalog.data_model_access.ModelTableRow
        variable to be downloaded.
    options : dictionary
        datast to which the variable belongs.

    Returns
    -------
    data : numpy array
        the requested data.
    """
    print("The options are:", options)
    string_parts = [
        f"{name}={value}" for name, value in options.items() if value is not None
    ]
    result_string = "&".join(string_parts)
    return result_string


def _validate_user():
    email, pin = get_registered_api_pin()
    url_security = f"{HYDRODATA_URL}/api/api_pins?pin={pin}&email={email}"
    response = requests.get(url_security, headers=None, timeout=15)
    if not response.status_code == 200:
        raise ValueError(
            f"No registered PIN for email '{email}' and PIN {pin}. See documentation to register with a URL."
        )
    json_string = response.content.decode("utf-8")
    jwt_json = json.loads(json_string)
    expires_string = jwt_json.get("expires")
    if expires_string:
        expires = datetime.datetime.strptime(
            expires_string, "%Y/%m/%d %H:%M:%S GMT-0000"
        )
        now = datetime.datetime.now()
        if now > expires:
            raise ValueError(
                "PIN has expired. Please re-register it from https://hydrogen.princeton.edu/pin"
            )
    jwt_token = jwt_json["jwt_token"]
    headers = {}
    headers["Authorization"] = f"Bearer {jwt_token}"
    return headers


def get_registered_api_pin() -> Tuple[str, str]:
    """
    Get the email and pin registered by the current user.

    Returns:
        A tuple (email, pin)
    Raises:
        ValueError if no email/pin was registered
    """

    pin_dir = os.path.expanduser("~/.hydrodata")
    pin_path = f"{pin_dir}/pin.json"
    if not os.path.exists(pin_path):
        raise ValueError(
            "No email/pin was registered. Use the register_api() method to register the pin you created at the website."
        )
    try:
        with open(pin_path, "r") as stream:
            contents = stream.read()
            parsed_contents = json.loads(contents)
            email = parsed_contents.get("email")
            pin = parsed_contents.get("pin")
            return (email, pin)
    except Exception as e:
        raise ValueError(
            "No email/pin was registered. Use the register_api() method to register the pin you created at the website."
        ) from e

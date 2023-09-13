import sqlite3
import pandas as pd

HYDRODATA = '/hydrodata'
DB_PATH = f'{HYDRODATA}/national_obs/point_obs.sqlite'


def list_available_sources():
    """
    List in Shell the data sources that are available on HydroData.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    # Create database connection
    conn = sqlite3.connect(DB_PATH)

    query = """
            SELECT *
            FROM variables
            """
    df = pd.read_sql_query(query, conn)
    print(df[['variable_name', 'units', 'data_source', 'variable', 'temporal_resolution', 'aggregation']])
    conn.close()

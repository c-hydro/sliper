"""
Library Features:

Name:          lib_utils_db
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260605'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import psycopg2
import pandas as pd

from psycopg2 import sql
from netrc import netrc

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# helper to get db credential
def get_credentials(machine="liguria_landdefend_db", user=None, password=None, **kwargs):

    # info start
    log_stream.info(" -----> DB credentials ... ")

    # check credentials
    if user and password:
        # info credentials type
        log_stream.info(" ::: Set by user")
        pass
    else:
        # get auth from .netrc
        auth = netrc().authenticators(machine)

        # if auth is not defined
        if auth is None:

            log_stream.error(' ===> Database credentials are not defined. Exit.')
            log_stream.info(" -----> DB credentials ... FAILED")
            raise RuntimeError(
                f"Credentials for database '{machine}' not found in ~/.netrc"
            )

        # extract login and password
        user, _, password = auth

        # info credentials type
        log_stream.info(" ::: Set by .netrc")

    # check user and password
    if not user or not password:
        log_stream.warning(' ===> User and/or password is/are not defined. Check your settings.')

    # info end
    log_stream.info(" -----> DB credentials ... DONE")

    return user, password
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to connect database
def get_connection(
    host="10.24.194.56", port=5432, db="liguria_landdefend",
    user=None, password=None, **kwargs):

    # info start
    log_stream.info(" -----> DB connection ... ")

    # check user and password
    if (not user) or (not password):
        # info end (failed)
        log_stream.warning(" ===> User or password is/are not defined")
        log_stream.info(" -----> DB connection ... FAILED")

    try:
        # db connection
        conn = psycopg2.connect(
            host=host, port=port,dbname=db,
            user=user, password=password,
            connect_timeout=10
        )
        # info end (done)
        log_stream.info(" -----> DB connection ... DONE")

        return conn

    except Exception as e:

        # info end (failed)
        log_stream.warning(f" ===> Error connecting to database: {e}")
        log_stream.info(" -----> DB connection ... FAILED")

        return None
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to fetch data
def fetch_data(conn, limit=None, fields=None, result_format="dataframe"):

    # info start
    log_stream.info(" -----> Fetch data ... ")

    # sanitize mapping
    fields_clean = {}
    for dst_name, src_name in fields.items():
        if dst_name is None:
            log_stream.warning(
                f" ===> Destination field is None for source field '{src_name}'. "
                f"Using 'NA' as destination name."
            )
            dst_name = "NA"

        if src_name is None:
            log_stream.warning(
                f" ===> Source field for destination field '{dst_name}' is None. "
                f"Using 'NA' as source name."
            )
            src_name = "NA"

        fields_clean[dst_name] = src_name

    fields = fields_clean

    if conn is None:
        log_stream.error(" ===> Database connection is undefined. Exit.")
        log_stream.info(" -----> Fetch data ... FAILED")
        raise ConnectionError("Connection is not established.")

    try:
        with conn.cursor() as cur:

            if limit is None:
                query = sql.SQL("""
                    SELECT *
                    FROM landslides_full_view
                """)
                cur.execute(query)
            else:
                query = sql.SQL("""
                    SELECT *
                    FROM landslides_full_view
                    LIMIT %s
                """)
                cur.execute(query, (limit,))

            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

        conn.close()

        # create dataframe
        df = pd.DataFrame(rows, columns=colnames)

        # apply field filtering and remapping
        if fields is not None and fields:

            # warn for missing source fields
            available_cols = set(df.columns)
            missing_fields = []
            for dst_name, src_name in fields.items():
                if src_name not in available_cols:
                    missing_fields.append((dst_name, src_name))
                    log_stream.warning(
                        f" ===> Field '{src_name}' mapped to '{dst_name}' "
                        f"is not available in the database view."
                    )

            # print available columns once
            if missing_fields:
                log_stream.warning(
                    f" ===> Available columns are: {', '.join(sorted(df.columns.tolist()))}"
                )

            # keep only available fields
            map_fields = {
                dst_name: src_name
                for dst_name, src_name in fields.items()
                if src_name in available_cols
            }

            if map_fields:
                df = df[list(map_fields.values())]

                map_rename = {
                    src_name: dst_name
                    for dst_name, src_name in map_fields.items()
                }

                df = df.rename(columns=map_rename)
            else:
                log_stream.warning(
                    " ===> None of the requested fields are available."
                )
                df = pd.DataFrame()

        # create output object
        if result_format == "dataframe":
            obj = df

        elif result_format == "dictionary":
            obj = df.to_dict()

        else:
            raise ValueError(
                "result_format must be either 'dataframe' or 'dictionary'"
            )

        log_stream.info(" -----> Fetch data ... DONE")

        return obj

    except Exception as e:
        log_stream.warning(f"Query error: {e}. Return None")
        log_stream.info(" -----> Fetch data ... FAILED")
        return None
# ----------------------------------------------------------------------------------------------------------------------

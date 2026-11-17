"""
SLIPER UTILS - CONNECT DB - Soil Landslide Information and Prediction & Early Response

__date__ = '20260914'
__version__ = '1.1.0'
__author__ = Fabio Delogu (fabio.delogu@cimafoundation.org
__library__ = 'sliper'

General command line:
python sliper_utils_connect_db.py

Version(s):
20260914 (1.1.0) --> Update for testing
20260422 (1.0.0) --> First release
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
import psycopg2
import pandas as pd

from psycopg2 import sql
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to connect database
def connect_db(host="10.24.194.56", port=5432, dbname="liguria_landdefend",
               user="rekab", password="baker"):

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=10
        )
        print(" ----> Connection established successfully")
        return conn

    except Exception as e:
        print(f" ===> Error connecting to database: {e}")
        return None
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to fetch data
def fetch_data():

    # connect database
    conn = connect_db()

    # check connection
    if conn is None:
        return None
    try:
        # prepare query
        cur = conn.cursor()

        query = sql.SQL("""
            SELECT *
            FROM landslides_full_view
        """)

        print(f" ----> QUERY: {query}")

        # execute query
        cur.execute(query)

        # get rows and cols
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]

        # close connection
        cur.close()
        conn.close()

        return cols, rows

    except Exception as e:

        print(f" ===> Query error: {e}")
        return None
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to create db
def create_db(data_db, cols_db):
    dframe_db = pd.DataFrame(data_db, columns=cols_db)
    return dframe_db
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to organize db
def organize_db(dframe_db):

    # convert datetime columns
    dframe_db["timestamp_utc"] = pd.to_datetime(dframe_db["timestamp_utc"], format="%Y-%m-%d %H:%M:%S")
    dframe_db["data_trigger"] = pd.to_datetime(dframe_db["data_trigger"],format="%Y-%m-%d")
    dframe_db["data"] = pd.to_datetime(dframe_db["data"],format="%Y-%m-%d")

    # convert time column
    dframe_db["orario_locale"] = pd.to_datetime(dframe_db["orario_locale"],format="%H:%M:%S").dt.time

    # convert string columns
    dframe_db["nome_trigger"] = dframe_db["nome_trigger"].astype("string")

    # normalize località as a single string
    dframe_db["località"] = (
        dframe_db["località"]
        .astype(str)
        .str.strip()
        .str.replace(r"[\s,]+", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )

    # order dataframe
    dframe_db = dframe_db.sort_values(by="data").reset_index(drop=True)

    return dframe_db
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure folder (if defined)
def ensure_folder(path_name):
    # check folder
    folder_name = os.path.dirname(path_name)
    # create folder
    if folder_name:
        os.makedirs(folder_name, exist_ok=True)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to dump database to csv
def dump_db2csv(file_name, dframe_data, delimiter=';', date_format="%Y-%m-%d"):

    # dump dataframe
    dframe_data.to_csv(
        file_name,
        sep=delimiter, index=False, date_format=date_format
    )
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# example of main
def main(view_db=15, file_name_db='soilslips_db.csv'):

    # info start
    print(" ============================================================================ ")
    print(" ==> PROCESS DATA FROM DB ...")
    print(" ============================================================================ ")

    # method to fetch data from db
    print(" ---> Fetching data from database ... ")
    cols_db, data_db = fetch_data()
    print(f" ----> Columns found: {len(cols_db)}")
    print(f" ----> Rows found: {len(data_db)}")
    print(" ---> Fetching data from database ... DONE ")

    # check elements to verify dimensions
    print(" ---> Checking database dimensions ...")
    if data_db and len(cols_db) != len(data_db[0]):
        raise ValueError(f"Columns ({len(cols_db)}) and row values ({len(data_db[0])}) do not match.")
    print(" ---> Checking database dimensions ... DONE")

    # create dataframe
    print(" ---> Creating dataframe ...")
    dframe_db_raw = create_db(data_db, cols_db)
    print(f" ----> Dataframe created: {dframe_db_raw.shape[0]} rows x {dframe_db_raw.shape[1]} columns")
    print(" ---> Creating dataframe ... DONE")

    # organize dataframe
    print(" ---> Organizing dataframe ...")
    dframe_db_processed = organize_db(dframe_db_raw)
    print(" ---> Organizing dataframe ... DONE")

    # check dataframe
    if not dframe_db_processed.empty:
        print(
            f" ----> Date range: "
            f"{dframe_db_processed['data'].min():%Y-%m-%d} -> "
            f"{dframe_db_processed['data'].max():%Y-%m-%d}"
        )

    # analyse dataframe
    if view_db is not None:
        # print data
        print(f" ---> Showing first {view_db} rows ...")
        print(dframe_db_processed.head(view_db).to_string(index=False))
        print(f" ---> Showing first {view_db} rows ... DONE")

    # ensure folder
    print(f" ---> Dumping dataframe to file CSV {file_name_db}...")
    ensure_folder(file_name_db)

    # dump dataframe to csv file
    dump_db2csv(file_name=file_name_db, dframe_data=dframe_db_processed)
    print(f" ---> Dumping dataframe to file CSV {file_name_db}... DONE")

    # info end
    print(" ============================================================================ ")
    print(" ==> PROCESS DATA FROM DB ... DONE")
    print(" ============================================================================ ")

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# Call script from external library
if __name__ == '__main__':

    # define db views (int or None)
    view_rows = 10
    # define db csv filename (string)
    file_name = '/home/admin/soilslips-ws/data_static/db_soil_slips/soil_slips_db_20260915.csv'
    # call main with arguments
    main(view_db=view_rows, file_name_db=file_name)
# ----------------------------------------------------------------------------------------------------------------------

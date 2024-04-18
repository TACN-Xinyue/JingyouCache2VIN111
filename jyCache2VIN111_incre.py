"""_summary_ This Script is to transform Jingyou Cache result to VIN1-11

Input:
-daily VIN(1-17) & JingyouId from vinDB_Aliyun.min_api
-JingyouId & Ktypes from vinDB_Aliyun.min_api

Returns:
    VIN(1-11) & Ktype into VIN111(only daily Increment)
    _name_:VIN111_yyyy-mm-dd.csv & VIN111_EXTRA_yyyy-mm-dd.csv
"""

# @auther: X.Wang
# @last update: Apr./18/2024

# %%
# Importing ...
import pandas as pd
import pymysql
import logging
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import datetime
from datetime import timedelta
import tqdm
from tqdm import tqdm

# --------Retrieve VIN&Jingyou ID from MySql use SSH Tunel--------#
# ----------------------------------------------------------------#
ssh_host = "139.196.113.12"
ssh_username = "xinyue"
ssh_password = "68PnGoEes7tXpg"
database_username = "xinyue"
database_password = "hK958MSmqaYFMz"
database_name = "min_api"
localhost = "127.0.0.1"


def open_ssh_tunnel(verbose=False):
    """
    Open an SSH tunnel and connect using a username and password.

    :param verbose: Set to True to show logging
    :return tunnel: Global SSH tunnel connection
    """

    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

    global tunnel
    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        remote_bind_address=("127.0.0.1", 3306),
    )

    tunnel.start()


def mysql_connect():
    """
    Connect to a MySQL server using the SSH tunnel connection

    :return connection: Global MySQL database connection
    """
    global connection

    connection = pymysql.connect(
        host="127.0.0.1",
        user=database_username,
        passwd=database_password,
        db=database_name,
        port=tunnel.local_bind_port,
    )


def run_query(mysqlquery):
    """Runs a given SQL query via the global database connection.

    :param sql: MySQL query
    :return: Pandas dataframe containing results
    """

    return pd.read_sql_query(mysqlquery, connection)


def mysql_disconnect():
    """Closes the MySQL database connection."""

    connection.close()


def close_ssh_tunnel():
    """Closes the SSH tunnel connection."""

    tunnel.close


open_ssh_tunnel()
mysql_connect()

# Get the time range for query condition
now = datetime.datetime.now()
now = now - timedelta(days=1)
today_range_left = now.replace(hour=0, minute=0, second=0, microsecond=0)
today_range_right = now.replace(hour=23, minute=59, second=59, microsecond=59)
nowString = now.strftime("%Y-%m-%d %H:%M:%S")
leftString = today_range_left.strftime("%Y-%m-%d %H:%M:%S")
rightString = today_range_right.strftime("%Y-%m-%d %H:%M:%S")
print(f"Quering data from {leftString} to {rightString}")

# Call the defined function
daily_query = f"SELECT vin, vehicleIds FROM min_api.jingyou_cache WHERE LENGTH(vehicleIds)>1 AND timestamp BETWEEN '{leftString}' AND '{rightString}';"
df_jyCache = run_query(daily_query)  # Get the daily Increment from JY Cache
df_jy = run_query(
    "SELECT ktype, vehicleId FROM min_api.jingyou;"
)  # Get the JY Id to Ktype

mysql_disconnect()
close_ssh_tunnel()

# ----------------------------------------------------------------#
# --------------------Begin Processing Increment------------------#
# Transform the data type to String
df_jy = df_jy.astype(str)

# Split multiple vehicleIds in one cell into rows
df_jyCache = (
    df_jyCache.assign(vehicleIds=df_jyCache["vehicleIds"].str.split("\n"))
    .explode("vehicleIds")
    .reset_index(drop=True)
)

# Left join to get Ktype
df_vin111 = df_jyCache.merge(
    df_jy, how="left", left_on="vehicleIds", right_on="vehicleId"
)

# Remove other column "vin" and "Ktype"
df_vin111 = df_vin111[["vin", "ktype"]]

# Get the first 11 digits from VIN
df_vin111["vin"] = df_vin111["vin"].str[:11]

# Remove the duplicates
df_vin111 = df_vin111.drop_duplicates()

# Mark the case one VIN to multiple ktype as in column "count" > 1
# Remove the rows "count" > 1
df_vin111["count"] = df_vin111.groupby("vin")["ktype"].transform("nunique")
df_vin111 = df_vin111.loc[df_vin111["count"] < 2]

# Cleansing the data to wanted form
df_vin111 = df_vin111.rename(
    columns={"vin": "VIN1-11", "ktype": "KTYPE"}
)  # Change column header
df_vin111.insert(1, "Brand_nr", 0)  # Add column Brand_nr
df_vin111.insert(2, "Model_nr", 0)  # Add column Model_nr
df_vin111.insert(3, "last_update", nowString)  # Add Column last_update
df_vin111 = df_vin111[
    ["VIN1-11", "Brand_nr", "Model_nr", "KTYPE", "last_update"]
]  # Rearrange column order
df_vin111 = df_vin111.dropna(subset="KTYPE")  # Remove N/A from column KTYPE

# %%
# ----------------------------------------------------------------#
# -------------Begin concatenate increment into VIN1-11-----------#
# !Remember to change the file name of VIN1-11 as the version changes
df_vin111_lastupdate = pd.read_csv(
    "vin111-2024-04-16_09_51_24.csv", dtype=object
)  # Read VIN1-11

# Initialize a DataFrame to store the rows with one vin to different ktypes
df_extra = pd.DataFrame()

"""
    This for-loop is to estimate the case one VIN to multiple ktype 
    Only the distinct VIN1-11+kTYPE will be added into the VIN1-11 table
    One VIN with multiple  different ktype will be stored in df_extra

"""
for i in tqdm(range(len(df_vin111["VIN1-11"]))):
    if df_vin111.iloc[i, 0] in df_vin111_lastupdate["VIN1-11"].values:
        ktypes = df_vin111_lastupdate.loc[
            df_vin111_lastupdate["VIN1-11"] == df_vin111.iloc[i, 0], ["KTYPE"]
        ]
        KT = df_vin111.iloc[i, 3]
        if KT not in ktypes["KTYPE"].values:
            df_extra = df_extra._append(df_vin111.iloc[i, :])
            df_extra = df_extra._append(
                df_vin111_lastupdate.loc[
                    df_vin111_lastupdate["VIN1-11"] == df_vin111.iloc[i, 0]
                ]
            )

    else:
        df_vin111_lastupdate = df_vin111_lastupdate._append(df_vin111.iloc[i, :])

# ----------------------------------------------------------------#
# ---------------------Write to CSV as Output---------------------#
df_name_vin111 = f"VIN111_{nowString[:10]}.csv"
df_vin111.to_csv(df_name_vin111, sep=",", index=False)
df_name_extra = f"VIN111_EXTRA_{nowString[:10]}.csv"
df_vin111.to_csv(df_name_extra, sep=",", index=False)

# Todo: Create Engine to write to VIN_DB after having the access to vinDB

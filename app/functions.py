import os
import pymysql
import pandas as pd
from settings import DATABASE


def get_csv_path():
    """
    Returns the path of the directory where the CSV files are stored.

    Returns:
        str: Absolute path of the CSV files directory.
    """
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_directory = os.path.join(parent_dir, "bkp_tables")
    return csv_directory


def get_csv_files(directory):
    """
    Lists the CSV files in the specified directory.

    Args:
        directory (str): Path of the directory where CSV files are stored.

    Returns:
        list: List of CSV file names found in the directory.
    """
    return [f for f in os.listdir(directory) if f.endswith(".csv")]


def connect_db():
    """
    Establishes a connection to the MySQL database.

    Returns:
        pymysql.connections.Connection: Database connection object.
    """
    return pymysql.connect(
        host=DATABASE["host"],
        user=DATABASE["user"],
        password=DATABASE["password"],
        database=DATABASE["database"]
    )


def disable_foreign_keys(cursor):
    """
    Temporarily disables foreign key checks in the database.

    Args:
        cursor (pymysql.cursors.Cursor): Database connection cursor.
    """
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")


def enable_foreign_keys(cursor):
    """
    Re-enables foreign key checks in the database.

    Args:
        cursor (pymysql.cursors.Cursor): Database connection cursor.
    """
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")


def truncate_tables(cursor, tables):
    """
    Truncates the specified tables, removing all data.

    Args:
        cursor (pymysql.cursors.Cursor): Database connection cursor.
        tables (list): List of table names to truncate.
    """
    for table in tables:
        cursor.execute(f"TRUNCATE TABLE {table};")


def load_csv_to_db(cursor, connection, csv_dir):
    """
    Loads data from CSV files into the database.

    Args:
        cursor (pymysql.cursors.Cursor): Database connection cursor.
        connection (pymysql.connections.Connection): Database connection.
        csv_dir (str): Path of the directory containing the CSV files.
    """
    
    
    for file in get_csv_files(csv_dir):
        table_name = os.path.splitext(file)[0]
        csv_path = os.path.join(csv_dir, file)

        df = pd.read_csv(csv_path)
        df = df.fillna("")

        columns = ", ".join(df.columns)

        placeholders = ", ".join(["%s"] * len(df.columns))

        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        cursor.executemany(sql, df.values.tolist())

        connection.commit()


def create_bkp_folder():
    """
    Create a folder to receive all the .csv files
    """
    if not os.path.exists(get_csv_path()):
        os.makedirs(get_csv_path())
        print(f"Pasta criada em: {get_csv_path()}\n")
    else:
        print(f"A pasta já existe: {get_csv_path()}\n")

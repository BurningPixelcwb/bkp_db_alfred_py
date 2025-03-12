from functions import (
    os, pd, connect_db, get_csv_path, create_bkp_folder
)


def main():

    connection = connect_db()
    cursor = connection.cursor()

    create_bkp_folder()

    try:
        # Retrieve all tables from DB
        cursor.execute("SHOW TABLES")

        # Create a list of all table names
        tables = [db_table[0] for db_table in cursor.fetchall()]

        # Export table data to csv
        for table in tables:
            df = pd.read_sql(f"SELECT * FROM `{table}`", connection)
            df.to_csv(
                f"{get_csv_path()}/{table}.csv", 
                index=False
            )

    except Exception as e:
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()

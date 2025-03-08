from functions import (
    os, connect_db, disable_foreign_keys, enable_foreign_keys,
    truncate_tables, load_csv_to_db, get_csv_path, get_csv_files
)


def main():
    """
    Main function that manages the process of truncating tables
    and loading CSV data.
    """
    connection = connect_db()
    cursor = connection.cursor()

    try:
        disable_foreign_keys(cursor)
        tables = [
            os.path.splitext(f)[0] for f in get_csv_files(get_csv_path())
        ]

        truncate_tables(cursor, tables)

        enable_foreign_keys(cursor)

        load_csv_to_db(cursor, connection, get_csv_path())

    except Exception as e:
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()

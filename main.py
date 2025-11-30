from census_api import DataPull


def main():
    dp = DataPull()
    output = dp.conn.execute(
        """
    SELECT * FROM sqlite_db.geo_table;
    """
    ).df()
    print(output)


if __name__ == "__main__":
    main()

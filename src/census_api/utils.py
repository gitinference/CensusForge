import duckdb
import geopandas as gpd
from jp_tools import download
import logging
import os
import tempfile
import pkg_resources


class DataPull:
    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.conn = duckdb.connect()

        # Resolve the path to the database file
        db_path = pkg_resources.resource_filename(__name__, "database.db")

        # Check if the database file exists
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found at {db_path}")

        self.db_file = db_path

        # Load SQLite extension and attach the database
        self.conn.execute("LOAD sqlite;")
        self.conn.execute(f"ATTACH '{self.db_file}' AS sqlite_db (TYPE sqlite);")

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename=log_file,
        )

        # Ensure saving directories exist
        os.makedirs(os.path.join(self.saving_dir, "raw"), exist_ok=True)
        os.makedirs(os.path.join(self.saving_dir, "processed"), exist_ok=True)
        os.makedirs(os.path.join(self.saving_dir, "external"), exist_ok=True)

    def pull_geos(self, url: str, filename: str) -> gpd.GeoDataFrame:
        if not os.path.exists(filename):
            temp_filename = f"{tempfile.gettempdir()}/{hash(filename)}.zip"
            download(url=url, filename=temp_filename)
            gdf = gpd.read_file(temp_filename)
            gdf.to_parquet(filename)
        return gpd.read_parquet(filename)

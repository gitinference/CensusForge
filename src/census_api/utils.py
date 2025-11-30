import duckdb
import geopandas as gpd
from jp_tools import download
import logging
import os
import tempfile
import importlib.resources as resources


class DataPull:
    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.conn = duckdb.connect()
        self.db_file = str(resources.files("census_api").joinpath("database.db"))
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

    def get_database_id(self, name: str) -> int:
        id = self.conn.execute(
            """
            SELECT id FROM sqlite_db.dataset_table WHERE dataset=?;
            """,
            (name,),
        ).fetchone()
        if id is None:
            raise ValueError(f"{name} is not a valid database run REPLACE ME")
        return id[0]

    def get_variable_id(self, name: str) -> int:
        id = self.conn.execute(
            """
            SELECT id FROM sqlite_db.variable_table WHERE dataset=?;
            """,
            (name,),
        ).fetchone()
        if id is None:
            raise ValueError(f"{name} is not a valid variable run REPLACE ME")
        return id[0]

    def get_geo_id(self, name: str) -> int:
        id = self.conn.execute(
            """
            SELECT id FROM sqlite_db.geo_table WHERE dataset=?;
            """,
            (name,),
        ).fetchone()
        if id is None:
            raise ValueError(f"{name} is not a valid geography run REPLACE ME")
        return id[0]

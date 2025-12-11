import importlib.resources as resources
import logging
import os
import tempfile

import duckdb
import geopandas as gpd
from jp_tools import download


class DataPull:
    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.conn = duckdb.connect()
        self.db_file = str(resources.files("CensusForge").joinpath("database.db"))
        self.conn.execute("LOAD sqlite;")
        self.conn.execute(f"ATTACH '{self.db_file}' AS sqlite_db (TYPE sqlite);")

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename=log_file,
        )

        # Ensure saving directories exist
        # os.makedirs(os.path.join(self.saving_dir, "raw"), exist_ok=True)
        # os.makedirs(os.path.join(self.saving_dir, "processed"), exist_ok=True)
        # os.makedirs(os.path.join(self.saving_dir, "external"), exist_ok=True)

    def pull_geos(self, url: str, filename: str) -> gpd.GeoDataFrame:
        if not os.path.exists(filename):
            temp_filename = f"{tempfile.gettempdir()}/{hash(filename)}.zip"
            download(url=url, filename=temp_filename)
            gdf = gpd.read_file(temp_filename)
            gdf.to_parquet(filename)
        return gpd.read_parquet(filename)

    def get_database(self, database_id: str) -> str:
        name = self.conn.execute(
            """
            SELECT dataset FROM sqlite_db.dataset_table WHERE id=?;
            """,
            (database_id,),
        ).fetchone()
        if name is None:
            raise ValueError(f"{database_id} is not a valid database run REPLACE ME")
        return name[0]

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

    def get_year(self, year_id: int) -> int:
        year_name = self.conn.execute(
            """
            SELECT year FROM sqlite_db.year_table WHERE id=?;
            """,
            (year_id,),
        ).fetchone()
        if year_name is None:
            raise ValueError(f"{year_id} is not a valid database run REPLACE ME")
        return year_name[0]

    def get_year_id(self, year: int) -> int:
        year_id = self.conn.execute(
            """
            SELECT year FROM sqlite_db.year_table WHERE year=?;
            """,
            (year,),
        ).fetchone()
        if year_id is None:
            raise ValueError(f"{year} is not a valid database run REPLACE ME")
        return year_id[0]

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

    def get_geo_years(self, dataset_id: int, geo_id: int) -> list:
        result = self.conn.execute(
            """
            SELECT
                DISTINCT year_id
            FROM sqlite_db.geo_interm
            WHERE dataset_id=? AND geo_id=?;
            """,
            (dataset_id, geo_id),
        ).fetchall()

        year_ids = [row[0] for row in result]
        return sorted(year_ids)

    def get_dataset_geo(self):
        pass

    def get_geo(self):
        pass

    def get_dataset_url(self, dataset_name: str) -> str:
        name = self.conn.execute(
            """
            SELECT api_url FROM sqlite_db.dataset_table WHERE dataset=?;
            """,
            (dataset_name,),
        ).fetchone()
        if name is None:
            raise ValueError(f"{dataset_name} is not a valid database run REPLACE ME")
        return name[0]

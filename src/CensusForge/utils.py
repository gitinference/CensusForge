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
        """
        Handles access to local CensusForge database tables and downloading/
        caching of geographic files. Provides utilities to look up dataset,
        year, variable, and geography metadata stored in the bundled SQLite
        database.

        Parameters
        ----------
        saving_dir : str, default "data/"
            Directory where downloaded and processed files will be stored.
        log_file : str, default "data_process.log"
            File where log messages will be written.

        Notes
        -----
        This class loads a packaged SQLite database through DuckDB using the
        `LOAD sqlite` extension. All metadata lookups are executed against
        attached SQLite tables.

        Returns
        -------
        DataPull
            An initialized DataPull object with logging configured and the
            database attached.
        """
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
        """
        Downloads and caches a geographic file, returning it as a GeoDataFrame.

        If `filename` does not exist locally, the file is downloaded from
        the given `url` into a temporary ZIP file, read using GeoPandas,
        and then saved to Parquet at the specified filename. If the file
        already exists, it is loaded directly from disk.

        Parameters
        ----------
        url : str
            URL of the geographic file (typically a zipped shapefile or
            other GIS-compatible dataset).
        filename : str
            Local path to save or read the cached Parquet file.

        Returns
        -------
        geopandas.GeoDataFrame
            The loaded geographic dataset.
        """
        if not os.path.exists(filename):
            temp_filename = f"{tempfile.gettempdir()}/{hash(filename)}.zip"
            download(url=url, filename=temp_filename)
            gdf = gpd.read_file(temp_filename)
            gdf.to_parquet(filename)
        return gpd.read_parquet(filename)

    def get_database(self, database_id: str) -> str:
        """
        Retrieves the dataset name associated with a given dataset ID.

        Parameters
        ----------
        database_id : str
            The ID of the dataset stored in `dataset_table`.

        Returns
        -------
        str
            The dataset name.

        Raises
        ------
        ValueError
            If the given ID does not correspond to a valid dataset.
        """
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
        """
        Retrieves the dataset ID corresponding to a dataset name.

        Parameters
        ----------
        name : str
            The dataset name stored in `dataset_table`.

        Returns
        -------
        int
            The dataset ID.

        Raises
        ------
        ValueError
            If the dataset name is not found.
        """
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
        """
        Retrieves the year associated with a given year ID.

        Parameters
        ----------
        year_id : int
            Primary key in the `year_table`.

        Returns
        -------
        int
            The corresponding year.

        Raises
        ------
        ValueError
            If the year ID does not exist.
        """
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
        """
        Retrieves the ID corresponding to a given year.

        Parameters
        ----------
        year : int
            A year value stored in `year_table`.

        Returns
        -------
        int
            The associated year ID.

        Raises
        ------
        ValueError
            If the year is not found.
        """
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
        """
        Retrieves the variable ID associated with a variable name.

        Parameters
        ----------
        name : str
            The variable name stored in `variable_table`.

        Returns
        -------
        int
            The variable ID.

        Raises
        ------
        ValueError
            If the variable name does not exist.
        """
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
        """
        Retrieves the geography ID associated with a geography name.

        Parameters
        ----------
        name : str
            The geography name stored in `geo_table`.

        Returns
        -------
        int
            The geography ID.

        Raises
        ------
        ValueError
            If the geography name does not exist.
        """

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
        """
        Retrieves all available years for a given dataset and geography.

        Parameters
        ----------
        dataset_id : int
            Dataset ID from `dataset_table`.
        geo_id : int
            Geography ID from `geo_table`.

        Returns
        -------
        list of int
            Sorted list of year IDs for which the dataset/geography
            combination exists.
        """
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
        """
        Placeholder for future implementation.

        Returns
        -------
        None
        """
        pass

    def get_geo(self):
        """
        Placeholder for future implementation.

        Returns
        -------
        None
        """
        pass

    def get_dataset_url(self, dataset_name: str) -> str:
        """
        Retrieves the API URL associated with a dataset name.

        Parameters
        ----------
        dataset_name : str
            The dataset name stored in `dataset_table`.

        Returns
        -------
        str
            The API URL used to access the dataset.

        Raises
        ------
        ValueError
            If the dataset name is not found.
        """
        name = self.conn.execute(
            """
            SELECT api_url FROM sqlite_db.dataset_table WHERE dataset=?;
            """,
            (dataset_name,),
        ).fetchone()
        if name is None:
            raise ValueError(f"{dataset_name} is not a valid database run REPLACE ME")
        return name[0]

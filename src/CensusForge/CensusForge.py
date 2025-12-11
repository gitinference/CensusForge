import requests
import polars as pl

from .utils import DataPull


class CensusAPI(DataPull):
    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data_process.log",
        census_key: str = "",
    ):
        """
        Extends `DataPull` with methods for querying the U.S. Census API.
        Provides a unified interface for retrieving metadata from the local
        CensusForge database and fetching remote Census data using HTTPS
        requests.

        Parameters
        ----------
        saving_dir : str, default "data/"
            Directory where downloaded and processed files will be stored.
        log_file : str, default "data_process.log"
            File where logs will be written.
        census_key : str, optional
            API key for authenticated Census API queries. Currently unused,
            but may be required for some API endpoints.

        Returns
        -------
        CensusAPI
            An initialized CensusAPI instance.
        """

        super().__init__(saving_dir, log_file)

    def query(self, dataset: str, params_list: list, year: int, extra: str = ""):
        """
        Queries the U.S. Census API and returns the response as a
        Polars DataFrame.

        Constructs a full API URL using the dataset name, list of query
        parameters, selected year, and optional additional URL suffixes.
        The request result is parsed from JSON into a DataFrame, with the
        first row treated as column names.

        Parameters
        ----------
        dataset : str
            Dataset name, which must match an entry in the local
            `dataset_table`.
        params_list : list of str
            List of variable names, geography codes, or other Census API
            query fields.
        year : int
            Census dataset year.
        extra : str, optional
            Extra query-string components to append to the final API URL
            (e.g., `&for=state:*`).

        Returns
        -------
        polars.DataFrame
            The Census API response as a table with proper column names.

        Notes
        -----
        The constructed URL is stored on the instance as `self.url` for
        debugging or reproducibility.
        """
        dataset_url = self.get_dataset_url(dataset_name=dataset)
        params = ",".join(params_list)
        url = (
            f"https://api.census.gov/data/{year}/{dataset_url[:-1]}?get={params}"
            + extra
        )
        df = pl.DataFrame(requests.get(url).json())
        names = df.select(pl.col("column_0")).transpose()
        df = df.drop("column_0").transpose()
        df = df.rename(names.to_dicts().pop())
        self.url = url

        return pl.DataFrame(df)

    def get_all_datasets(self) -> pl.DataFrame:
        """
        Returns a DataFrame containing all datasets stored in the local
        CensusForge metadata database.

        This performs a simple SELECT * query on `dataset_table` using
        DuckDB and returns the results as a Polars DataFrame.

        Returns
        -------
        polars.DataFrame
            Table of all available datasets, including IDs, names, URLs,
            and associated metadata fields.
        """
        df = self.conn.execute(
            """
            SELECT * FROM sqlite_db.dataset_table;
            """
        ).pl()
        return df

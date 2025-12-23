import requests
import numpy as np

from .utils import CensusUtils


class CensusAPI(CensusUtils):
    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data_process.log",
        census_key: str = "",
    ):
        """
        Extends `CensusUtils` with methods for querying the U.S. Census API.
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
        numpy.array
            The Census API response as a table with proper column names.

        Notes
        -----
        The constructed URL is stored on the instance as `self.url` for
        debugging or reproducibility.
        """

        # URL Constructor
        dataset_url = self.get_dataset_url(dataset_name=dataset)
        params = ",".join(params_list)
        url = (
            f"https://api.census.gov/data/{year}/{dataset_url[:-1]}?get={params}"
            + extra
        )
        self.url = url
        # Check that if the Year is available
        if year not in self.get_available_years(dataset):
            raise ValueError(f"{year} is not available for the {dataset}")

        # Check that the variable is available
        for param in params_list:
            if not self.check_variables(dataset=dataset, variable=param, year=year) > 0:
                raise ValueError(
                    f"The variable {param} is not available for the year {year} and dataset {dataset}"
                )

        return np.array(requests.get(url).json())

    def get_all_datasets(self):
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
        )
        return df

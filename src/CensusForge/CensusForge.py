import requests
import numpy as np

from .utils import CensusUtils, retry_decorator


class CensusAPI(CensusUtils):
    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data_process.log",
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
        Queries the U.S. Census API and returns the response as a NumPy array.

        Constructs a full API URL using the dataset name, query parameters,
        and year. It validates both the availability of the year for the
        specific dataset and the existence of each requested variable before
        executing the request.

        Parameters
        ----------
        dataset : str
            The short name of the Census dataset (e.g., 'acs/acs5'). Must
            correspond to a valid entry in the internal dataset table.
        params_list : list of str
            A list of variable names or geography codes to retrieve
            (e.g., ['NAME', 'B01001_001E']).
        year : int
            The specific census year to query.
        extra : str, optional
            Additional query string parameters, such as geography filters
            (e.g., "&for=state:06"). Defaults to an empty string.

        Returns
        -------
        numpy.ndarray
            A 2D array where the first row contains the column headers and
            subsequent rows contain the requested data.

        Raises
        ------
        ValueError
            If the requested 'year' is not supported by the dataset.
            If any variable in 'params_list' is invalid for the given year/dataset.

        Notes
        -----
        The final constructed URL is stored in `self.url` for debugging.
        The actual HTTP request is handled by the internal `_query` method,
        which includes retry logic.
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

        return self._query(self.url)

    @retry_decorator()
    def _query(self, url: str):
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

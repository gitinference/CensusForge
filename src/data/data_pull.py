import logging
import os
from datetime import datetime
from json import JSONDecodeError

import polars as pl
import requests
from tqdm import tqdm

from ..models import get_conn


class DataPull:
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = ":memory:",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.data_file = database_file
        self.conn = get_conn(self.data_file)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename=log_file,
        )
        # Check if the saving directory exists
        if not os.path.exists(self.saving_dir + "raw"):
            os.makedirs(self.saving_dir + "raw")
        if not os.path.exists(self.saving_dir + "processed"):
            os.makedirs(self.saving_dir + "processed")
        if not os.path.exists(self.saving_dir + "external"):
            os.makedirs(self.saving_dir + "external")

    def pull_query(
        self, flow: str, region: str, params: list, year: int
    ) -> pl.DataFrame:
        # prepare custom census query
        param = ",".join(params)
        base = "https://api.census.gov/data/"
        # flow = "/acs/acs1/pums"
        url = f"{base}{year}{flow}?get={param}&for={region}"
        df = pl.DataFrame(requests.get(url).json())

        # get names from DataFrame
        names = df.select(pl.col("column_0")).transpose()
        names = names.to_dicts().pop()
        names = dict((k, v.lower()) for k, v in names.items())

        # Pivot table
        df = df.drop("column_0").transpose()
        return df.rename(names).with_columns(year=pl.lit(year))

    def pull_dp03(self, flow: str, region: str, params: list) -> pl.DataFrame:
        for _year in range(2014, datetime.now().year):
            try:
                logging.info(f"pulling {_year} data")
                df = self.pull_query(
                    flow=flow,
                    region=region,
                    params=params,
                    year=_year,
                )
                # df = df.with_columns(
                #     geoid=pl.col("state") + pl.col("county") + pl.col("county subdivision")
                # ).drop(["state", "county", "county subdivision"])
                # df = df.with_columns(pl.all().exclude("geoid").cast(pl.Int64))

                # Register tmp as a DuckDB view
                # self.conn.register("df", df)
                # print(df.count())

                # Create table only once
                if (
                    "DP03Table"
                    not in self.conn.sql("SHOW TABLES;").df().get("name").tolist()
                ):
                    self.conn.sql("CREATE TABLE DP03Table AS SELECT * FROM df")
                else:
                    self.conn.sql("INSERT INTO DP03Table BY NAME SELECT * FROM df")

                logging.info(f"successfully inserted {_year}")

            except JSONDecodeError:
                logging.warning(f"The ACS for {_year} is not available")
                continue

        return self.conn.sql("SELECT * FROM DP03Table").pl()

    def pull_file(self, url: str, filename: str, verify: bool = True) -> None:
        """
        Pulls a file from a URL and saves it in the filename. Used by the class to pull external files.

        Parameters
        ----------
        url: str
            The URL to pull the file from.
        filename: str
            The filename to save the file to.
        verify: bool
            If True, verifies the SSL certificate. If False, does not verify the SSL certificate.

        Returns
        -------
        None
        """
        chunk_size = 10 * 1024 * 1024

        with requests.get(url, stream=True, verify=verify) as response:
            total_size = int(response.headers.get("content-length", 0))

            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Downloading",
            ) as bar:
                with open(filename, "wb") as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            file.write(chunk)
                            bar.update(len(chunk))

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

        super().__init__(saving_dir, log_file)

    def query(self, dataset: str, params_list: list, year: int, extra: str = ""):
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
        df = self.conn.execute(
            """
            SELECT * FROM sqlite_db.dataset_table;
            """
        ).pl()
        return df

import requests

from .utils import DataPull


class CensusAPI(DataPull):
    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data_process.log",
        census_key: str = "",
    ):

        super().__init__(saving_dir, log_file)

    def query(self, dataset: str, year: int, geography: str):
        pass

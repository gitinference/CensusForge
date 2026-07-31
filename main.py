from CensusForge import CensusAPI
import os


def main():
    ca = CensusAPI(str(os.getenv("CENSUS_KEY")))
    print(
        ca.query(
            dataset="acs-acs1-pumspr",
            year=2019,
            params_list=["AGEP", "SCH", "SCHL", "HINCP", "PWGTP", "PUMA"],
            geography="state",
        )
    )


if __name__ == "__main__":
    main()

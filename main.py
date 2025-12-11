from CensusForge import CensusAPI


def main():
    ca = CensusAPI()
    print(
        ca.query(
            dataset="acs-acs1-pumspr",
            year=2019,
            params_list=["AGEP", "SCH", "SCHL", "HINCP", "PWGTP", "PUMA"],
        )
    )


if __name__ == "__main__":
    main()

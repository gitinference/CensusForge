# **CensusForge**

> [!WARNING]  
> This project is still in development and may change very quickly. I will add more
> functionality in the future but this contains the bare minimum to support the needs
> of the project.

CensusForge is a Python toolkit for retrieving data from the U.S. Census API while
also leveraging a local SQLite metadata database for fast lookups, the SQLite
database creation repo can be found [in this link in GitHub](https://github.com/gitinference/census-db).
It simplifies working with Census datasets by providing a unified interface for:

- Downloading and caching geographic files
- Querying the Census API
- Looking up dataset, variable, year, and geography metadata
- Returning results as Polars or GeoPandas objects

CensusForge consists of two main classes:

- **`DataPull`** – Handles local metadata queries and file downloads
- **`CensusAPI`** – Extends `DataPull` and adds direct Census API querying

---

## **Installation**

```bash
pip install CensusForge
```

---

## **Quick Start Example**

The following example shows how to query the Census API using the `CensusAPI` class.

```python
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
```

Running the above will:

1. Look up the dataset in the local metadata database
2. Construct the correct Census API URL
3. Fetch the API response
4. Convert it to a Polars DataFrame

---

## **Project Structure**

```
CensusForge/
│
├── CensusAPI.py       # CensusAPI and DataPull classes
├── database.db        # Local SQLite metadata database
├── jp_tools/          # Utility functions (e.g., file download helper)
│
├── data/              # Output directory for downloaded/cached files
└── README.md          # Project documentation
```

---

## **API Overview**

### **CensusAPI**

#### `query(dataset, params_list, year, extra="") → pl.DataFrame`

Query a Census dataset using any set of variables or geography parameters.

**Example**

```python
ca.query(
    dataset="acs-acs1-pumspr",
    year=2019,
    params_list=["AGEP", "HINCP", "PUMA"],
    extra="&for=state:*"
)
```

---

### **Metadata Helpers (inherited from DataPull)**

| Method                              | Description                                 |
| ----------------------------------- | ------------------------------------------- |
| `get_database(id)`                  | Returns dataset name for ID                 |
| `get_database_id(name)`             | Returns dataset ID                          |
| `get_year(id)`                      | Returns year for ID                         |
| `get_year_id(year)`                 | Returns year ID                             |
| `get_variable_id(name)`             | Returns variable ID                         |
| `get_geo_id(name)`                  | Returns geography type ID                   |
| `get_geo_years(dataset_id, geo_id)` | Returns valid years for a dataset+geography |

---

### **Geospatial Tools**

#### `pull_geos(url, filename) → gpd.GeoDataFrame`

Downloads a geographic file (if missing), caches it as Parquet, and returns a GeoDataFrame.

---

## **Requirements**

- Python 3.9+
- DuckDB
- GeoPandas
- Polars
- Requests
- jp_tools (for download helper)

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## **Development**

To run tests or modify the project:

```bash
git clone https://github.com/yourusername/CensusForge.git
cd CensusForge
pip install -e .
```

---

## **License**

This project is licensed under the GNU General Public License v3.0 (GPL-3.0).

You may copy, modify, and distribute this software only under the terms of the GPL-3.0
license.

Full license text: <https://www.gnu.org/licenses/gpl-3.0.en.html>


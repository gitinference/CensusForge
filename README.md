# **CensusForge**

A dedicated Python toolkit for retrieving processed data from the U.S. Census API. CensusForge abstracts complex interactions with Census datasets by providing a unified library interface, simplifying metadata retrieval, file management, and data manipulation into Polars or GeoPandas DataFrames.

## 🚀 Core Functionality
CensusForge is engineered to handle the entire lifecycle of Census data analysis:

*   **Unified Access:** Provides cohesive wrappers for dataset querying via the official Census API endpoint.
*   **Metadata Management:** Stores and queries local metadata (dataset IDs, variable names, geographic boundaries, year applicability) using an integrated SQLite database.
*   **Data Handling:** Automatically handles downloading, caching, and integrating diverse file types (e.g., GeoJSON, Parquet).

CensusForge encapsulates two main service classes:
1.  **`DataPull`**: Manages local metadata persistence and geographic file downloads.
2.  **`CensusAPI`**: Builds upon `DataPull` to construct and execute API queries against the Census Bureau services.

---

## 🛠️ Installation & Setup

To get started, ensure you have Python 3.9+ installed. Our dependencies include scientific computing libraries (`polars`, `geopandas`) and data retrieval tools.

```bash
# Clone the repository (Must use Codeberg URL)
git clone https://codeberg.org/gitinference/CensusForge.git CensusForge
cd CensusForge

# Install Python dependencies from requirements file
pip install -r requirements.txt 
```
*Note: Please consult the setup documentation within the repository for advanced instructions regarding virtual environments.*

---

## ✨ Quick Start Example (API Query)

This example demonstrates querying the most recent American Community Survey data available via `CensusAPI`.

```python
from CensusForge import CensusAPI

def run_query():
    """Queries and prints a sample Census dataset."""
    ca = CensusAPI()
    print("--- Starting Data Pull ---")
    df = ca.query(
        dataset="acs-acs1-pumspr",  # Dataset ID for PUMS data
        year=2019,                 # Target year
        params_list=["AGEP", "SCH", "HINCP", "PUMA"], # Variables to include
    )
    print("--- Data Pull Successful ---")
    # The result is a Polars DataFrame: pl.DataFrame(...)
    return df
```

## 🧱 API Reference

### `CensusAPI` Class
#### `query(dataset, params_list, year, extra="") → pl.DataFrame`
Queries and returns the requested Census dataset variables for a specified year and list of geographic parameters (e.g., state lists). The `extra` parameter allows appending URL query filters.

**Example:**
```python
ca.query(
    dataset="acs-acs1-pumspr",
    year=2019,
    params_list=["AGEP", "HINCP", "PUMA"],
    extra="&for=state:*&geo=county:" # Example of additional URL filters
)
```

### DataPull Methods (Metadata Helpers)
Methods inherited from `DataPull` are used for introspection against the local metadata database:

| Method | Description | Returns |
| :--- | :--- | :--- |
| `get_database(id)` | Retrieves descriptive name for a given dataset ID. | `str` |
| `get_variable_id(name)` | Finds the internal unique ID for a variable name. | `str` |
| `get_geo_years(dataset_id, geo_id)` | Returns a list of years available for a specific combination. | `list[int]` |

### Geospatial Tools
#### `pull_geos(url, filename)`
Downloads or verifies the presence of required geographic data (e.g., Shapefile/GeoJSON). It caches the file as Parquet and returns an active GeoPandas DataFrame for immediate use.

---

## 💿 Project Structure

```
CensusForge/
├── CensusAPI.py       # Contains core service classes (CensusAPI, DataPull)
├── database.db        # Local SQLite metadata store
├── requirements.txt   # Python dependencies list
├── jp_tools/          # Utility functions (e.g., file download helper, cleaning scripts)
│   └── ...
├── data/              # Directory for cached and downloaded files (Parquet, etc.)
└── README.md          # Project documentation
```

---

## 📐 Citation

If you use CensusForge in your research or commercial project, please cite it using:

```bibtex
@software{ouslan2026censusforge,
    author       = {Ouslan, Alejandro},
    title        = {CensusForge},
    month        = jan,
    year         = 2026,
    publisher    = {Zenodo},
    version      = {1.0.0}, % Use the latest version!
    doi          = {10.5281/zenodo.xxxxxxxxx} % Check Zenodo for updated DOI
}
```

---

## 📜 License
This project is licensed under the GNU General Public License v3.0 (GPL-3.0). See the full [GPL-3.0 license](https://www.gnu.org/licenses/gpl-3.0.en.html).
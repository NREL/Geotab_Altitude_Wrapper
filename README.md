# Geotab Altitude Wrapper Utilities
Functions to pull data from Geotab's Altitude API and process it. 

# Setup
1. Clone this repo.
2. Copy the file `passwords_template.txt` and name the copy `passwords.txt`. Replace the contents with your Geotab DB(s) and user account(s).
3. Update the paths found in `api_wrapper/user_params.py` to match your local directories.
4. Set up a Python environment including the following packages:
    - polars, numpy, pickle, json, glob, time, datetime, ratelimit
    - `pip install mygeotab`
    - If you need to create and upload custom Geotab regions: geopandas, shapely, geojson
    - **You may need to install python-certifi-win32 or a similar package to run queries on the API while connected a VPN.**
    
# Running queries
To get started, try running one of the example Jupyter notebooks:
- `api_query_examples/regional_domicile_examples.ipynb`: outputs a set of parquet files combining Regional Domicile results across multiple queries
- `api_query_examples/stop_analytics_examples.ipynb`: outputs a set of parquet files for Stop Analytics across multiple queries
- `api_query_examples/other_query_examples.ipynb`: outputs a polars DataFrame for each query output

# Query output
![diagram](https://media.github.nrel.gov/user/2239/files/83cbbe98-7957-48fc-979f-508a7cec042b)

Results across queries from the Regional Domicile and Stop Analytics APIs are combined into a single set of .parquet files. Each parquet file can be read into a polars DataFrame with `pl.read_parquet(filename)`:
- `all_metadata_new.parquet`: Parameters (filters used, etc.) for each API query.
- `all_subzone_definitions_new.parquet`: Descriptions of each subzone. Subzones are origin-destination pair for O-D analyses, or geographic zones for Stop Analytics or Regional Domicile results; they may also be specific to a time slice (e.g., day of week). Shape definitions (stored as strings) for each subzone for each query. For Stop Analytics or Regional Domicile results, `ZoneId` links this table to `subzone_definitions`. For O-D analyses, `ZoneId` links to `Origin_ZoneId` or `Destination_ZoneId` in `subzone_definitions`.
- `all_results_new.parquet`: Results from all queries. This can be joined to `metadata` on `Query_ID`, or can be joined to `subzone_info` table on `Query_ID`+`Subzone`.

# Software record
This repo is associated with NREL Software Record SWR-24-77.

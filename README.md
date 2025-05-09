# ETL Execution CSV Exctraction– Step by Step 

# Install Required Python Libraries
pip install pandas pyodbc python-dotenv

# Start the pipeline
Run below command:
python etl_runner.py 

(It will take 10 mins to exctract the data)

# Read config
etl_runner.py reads config.json to get table details.

# Setup logging
Creates one log file per table + one master log.

# Load SQL query
For each table, reads the .sql file defined in query_file.
Now it will not create any table 

# Connect to MSSQL
Uses credentials from .env via python-dotenv.

# Run query in chunks
Uses pandas.read_sql(query, conn, chunksize=...) to process large data.

# Format datetime columns
Each datetime column is:
YYYY-MM-DD if no time
YYYY-MM-DDTHH:MM:SS if time exists

# Write CSV files
Saves chunked CSVs to output/<table>/<table>.1.csv, .2.csv, ...
currently chunk = 10000 records

# Log progress
Logs every step including file name, row count, and parse status. It will create log folder automatically in main folder.

# Repeat for all tables
Continues for all entries in config.json. 

#  ETL Execution – CSV Loading using `myw_db` Command (Step-by-Step)

## 1.  Move CSV Files to the Mounted Data Folder

Before starting the CSV load, move your generated CSV files into the mounted **data folder** for the TRX container.

**Container path:**

```
/shared-data/iqgeo_data/
```

 This is a mounted directory where the TRX container expects input files.

---

## 2.  Verify if Features/Tables Exist

Before loading data, check if the required **features (tables)** already exist in the application.

**Path to check:**

```
Application → Configuration → Features
```

Use the search to look up your feature/table names.

---

## 3.  If Features/Tables Don’t Exist

You must create them manually using `.def` files.

**Path to `.def` files in TRX repo:**

```
/survey_lke/deploy_survey_7240/db_schema/defs
```

**Run the following command to create the feature:**

```bash
myw_db <db_name> load <path_to_def_file>
```

Repeat this for each feature.

---

## 4.  Load CSV Data into the Target Table

Use the provided `.sh` scripts to load the data chunk by chunk.

###  Example Commands

####  For `insite_workorder_history`

- ~18K records
- Expected: 2 CSV chunks
- Estimated time: 20–25 minutes

```bash
bash ./load_insite_workorder_history.sh 1 2 2
```

####  For `inspection_insite_history`

- ~115.8K records
- Expected: 16 CSV chunks
- Estimated time: 20–25 minutes

```bash
bash ./load_inspection_insite_history.sh 1 16 16
```

####  For `inspection_insite_history_component`

- ~2.96M records
- Expected: 291 CSV chunks
- Recommended to run in batches of 50
- Estimated total time: 80–100 minutes

```bash
bash ./load_inspection_insite_history_component.sh 1 291 50
```

---

## 5.  Check Runtime Logs

Logs are generated automatically during runtime under the following folder:

```
insite_migration_log/
```

Each file will generate its own log. If your AVD or system shuts down unexpectedly, you can identify which files didn’t load by checking their log files and re-running only those files.

###  Re-run failed file (Example):

If chunk/file `2` failed:

```bash
bash ./load_insite_workorder_history.sh 2 2 1
```

---

## Log Directory Paths

- `insite_workorder_history` logs:  
  `insite_migration_log/insite_workorder_history/`

- `inspection_insite_history` logs:  
  `insite_migration_log/inspection_insite_history/`

- `inspection_insite_history_component` logs:  
  `insite_migration_log/inspection_insite_history_component/`



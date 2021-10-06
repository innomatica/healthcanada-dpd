# Health Canada Drug Product Database

Python script to build Health Canada Drug Product Database from the extracts
files.

## How To Use

1. Download four extracts files (allfile.zip, allfiles_ap.zip, all_files_dr.zip,
and all_files_it.zip) from [this link.](https://www.canada.ca/en/health-canada/services/drugs-health-products/drug-products/drug-product-database/extracts.html)
2. Extract all four archive files into the directories:
```
\allfiles
\allfiles_ap
\allfiles_dr
\allfiles_it
```
3. Run the script
```
python3 convert_data.py
```
4. First choose (r) to read raw data, then (b) to build drug data.
It may take up to 30 minutes or more to build data. After that it will
create a json file, `build_yyyymmdd.json`, which will be used to create
database files in the subsequent operations.

## Output Files

* build_yyyymmdd.json: JSON data that combines all extract files.
* otc_yyyymmdd.sql3: sqlite3 database for Human OTC drugs.
* otc_yyyymmdd.zip: zipped sqlite3 database
* otc_yyyymmdd.json: reconstructed JSON data with drug id as key

## Create Firebase Realtime Database

You can easily create a Firebase Realtime Database using the
reconstructed JSON data file

```
# list all database instances
firebase database:instance:list
```
```
# create a new database instance if necessary
firebase database:instance:create
```
```
# create a new entry for import
firebase database:push /healthcanada-dpd/otc
```

Go to the Firebase console, select Realtime Database,
find the entry point above. Then import the json file.




## Links
* [Drug Product Database online query](https://health-products.canada.ca/dpd-bdpp/index-eng.jsp)
* [Drug Product Database: Access the extracts](https://www.canada.ca/en/health-canada/services/drugs-health-products/drug-products/drug-product-database/extracts.html)
* [Drug Product Database (DPD) API Guide](https://health-products.canada.ca/api/documentation/dpd-documentation-en.html)
* [Firebase CLI: Realtime Database commands](https://firebase.google.com/docs/cli#rtdb-commands)

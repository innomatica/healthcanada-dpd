# Health Canada Drug Product Database

Python script to build Health Canada Drug Product Database from the extracts
files.

## How To Use

1. Download extracts files from [this link](https://www.canada.ca/en/health-canada/services/drugs-health-products/drug-products/drug-product-database/extracts.html)
depending on the product status (allfile.zip, allfiles_ia.zip, all_files_ap.zip, all_files_dr.zip)
Due to the changes regarding therapheutic class, you need to download either
version 1 or version 2 of the therapheutic class data file separately
(ther.zip, ther_ia.zip, ther_ap.zip, ther_dr.zip).


2. Extract the archive files into the sub-directories:
```
\allfiles
\allfiles_ap
\allfiles_dr
\allfiles_it
```
Be sure to copy therapheputic class file into each directory.
Otherewise the script will fail.

3. Run the script
```
python3 convert_data.py
```
4. First choose (r) to read raw data, then (b) to build drug data.
It may take up to 30 minutes or more to build data. After that it will
create a json file, `build_yyyymmdd.json`, which will be used to create
database files in the subsequent operations.

## Output Files

* `build_<PRODUCT STATUS>.json`: JSON data that combines all extract files for the product status.
* `<prduct schedule>_<PRODUCT STATUS>.sql3`: sqlite3 database output
* `<prduct schedule>_<PRODUCT STATUS>.json`: json output with drug id as key

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

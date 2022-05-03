#!/usr/bin/env python3
#
# Drug Product Database online query
# https://health-products.canada.ca/dpd-bdpp/index-eng.jsp
#
# Drug Product Database: Access the extracts
# https://www.canada.ca/en/health-canada/services/drugs-health-products/drug-products/drug-product-database/extracts.html
#
# Drug Product Database (DPD) API Guide
# https://health-products.canada.ca/api/documentation/dpd-documentation-en.html
#
import json
import csv
import os
import shutil
import sqlite3
from datetime import datetime

json_prefix = 'build_'

build_prefix = 'build'
extract_dir = './allfiles'
source_files = ['biosimilar.txt','comp.txt','drug.txt','form.txt','ingred.txt',
        'package.txt','pharm.txt','route.txt','schedule.txt','status.txt',
        'ther.txt','vet.txt' ]

dpd_dataset = []
build_dataset = []

drug_prod_status = ['MARKETED']
drug_schedule = 'OTC'

suffixes = {'MARKETED':'', 'APPROVED':'ap', 'INACTIVE':'ia', 'DORMANT':'dr'}

# TODO: rename this variable
worksheet = {
    "active_ingredients": {
        "input": 'ingred',
        "output": [],
        "fields": ["drug_code","active_ingredient_code","ingredients",
            "ingredient_supplied_ind","strength","strength_unit",
            "strength_type","dosage_value","base","dosage_unit","notes",
            "ingredients_f","strength_unit_f","strength_type_f",
            "dosage_unit_f"]
    },
    "companies": {
        "input": 'comp',
        "output": [],
        "fields": ["drug_code","mfr_code","company_code","company_name",
            "company_type","address_mailing_flag","address_billing_flag",
            "address_notification_flag","address_other","suite_number",
            "street_name","city_name","province","country","postal_code",
            "post_office_box","province_f","country_f"]
    },
    "drug_product": {
        "input": 'drug',
        "output": [],
        "fields": ["drug_code","product_categorization","class",
            "drug_identification_number","brand_name","descriptor",
            "pediatric_flag","accession_number","number_of_ais",
            "last_update_date","ai_group_no","class_f","brand_name_f",
            "descriptor_f"]
    },
    "product_status": {
        "input": "status",
        "output": [],
        "fields": ["drug_code","current_status_flag","status","history_date",
            "status_f","lot_number","expiration_date"]
    },
    "dosage_form": {
        "input": "form",
        "output": [],
        "fields": ["drug_code","pharm_form_code","pharmaceutical_form",
            "pharmaceutical_form_f"]
    },
    "packaging": {
        "input": "package",
        "output": [],
        "fields": ["drug_code","upc","package_size_unit","package_type",
            "package_size","product_information","package_size_unit_f",
            "package_type_f"]
    },
    "pharmaceutical_standard": {
        "input": "pharm",
        "output": [],
        "fields": ["drug_code","pharmaceutical_std"]
    },
    "route_of_administration": {
        "input": "route",
        "output": [],
        "fields": ["drug_code", "route_of_adminitration_code",
            "route_of_administration","route_of_administration_f"]
    },
    "schedule": {
        "input": "schedule",
        "output": [],
        "fields": ["drug_code","schedule","schedule_f"]
    },
    "therapeutic_class": {
        "input": "ther",
        "output": [],
        "fields": ["drug_code","tc_atc_number","tc_atc","tc_ahfs_number",
            "tc_ahfs","tc_atc_f","tc_ahfs_f"]
    },
    "veterinary_species": {
        "input": "vet",
        "output": [],
        "fields": ["drug_code","vet_species","vet_sub_species","vet_species_f"]
    }
}

def load_dpd_extracts(prod_status):
    #--------------------------------------------------------------------------
    # load dpd extracts
    suffix = suffixes[prod_status]

    for key in worksheet:

        if suffix == '':
            fname = '{}/{}.txt'.format(extract_dir, worksheet[key]['input'])
        else:
            # append suffix
            fname = '{}_{}/{}_{}.txt'.format(extract_dir,
                    suffix, worksheet[key]['input'], suffix)

        if not os.path.isfile(fname):
            print('\tERROR: file not found {}'.format(fname))
            return False

        with open(fname) as csvfile:
            csv_data = csv.DictReader(csvfile, worksheet[key]['fields'])
            output = []
            for row in csv_data:
                # DPD data fields are not consistent
                row['upc'] = ''
                row['packaging'] = ''
                row['packaging_f'] = ''
                row['pharmaceutical_std'] = ''
                output.append(row)

            worksheet[key]['output'] = output

    print('...total {} drug data loaded'.format(
        len(worksheet['drug_product']['output'])))
    return True


def build_worksheet(prod_status):
    #--------------------------------------------------------------------------
    # build worksheet
    if worksheet['drug_product']['output'] == []:
        input('\tERROR: No data found...read raw data first')
        return False

    # take drug_product dict as base set
    drug_l = worksheet['drug_product']['output']

    for idx in range(len(drug_l)):
        print("... {}/{}".format(idx,len(drug_l)), end="\r", flush=True)
        # fill brand_name_f if not exist
        if drug_l[idx]['brand_name_f'] == '':
            drug_l[idx]['brand_name_f'] = drug_l[idx]['brand_name']

        # active ingredients
        ingred_l = worksheet['active_ingredients']['output']
        drug_l[idx]['ingredients'] = []
        drug_l[idx]['ingredients_f'] = []
        for idy in range(len(ingred_l)):
            if drug_l[idx]['drug_code'] == ingred_l[idy]['drug_code']:
                drug_l[idx]['ingredients'].append(
                    ingred_l[idy]['ingredients'] + ' ' +
                    ingred_l[idy]['strength'] +
                    ingred_l[idy]['strength_unit'])
                drug_l[idx]['ingredients_f'].append(
                    ingred_l[idy]['ingredients_f'] + ' ' +
                    ingred_l[idy]['strength'] +
                    ingred_l[idy]['strength_unit_f'])

        # company name and code
        company_l = worksheet['companies']['output']
        for idy in range(len(company_l)):
            if drug_l[idx]['drug_code'] == company_l[idy]['drug_code']:
                drug_l[idx]['company_name'] = company_l[idy]['company_name']
                drug_l[idx]['company_code'] = company_l[idy]['company_code']

        # dosage form
        dosage_l = worksheet['dosage_form']['output']
        drug_l[idx]['dosage_form'] = []
        drug_l[idx]['dosage_form_f'] = []
        for idy in range(len(dosage_l)):
            if drug_l[idx]['drug_code'] == dosage_l[idy]['drug_code']:
                drug_l[idx]['dosage_form'].append(
                        dosage_l[idy]['pharmaceutical_form'])
                drug_l[idx]['dosage_form_f'].append(
                        dosage_l[idy]['pharmaceutical_form_f'])

        '''
        # packaging (no meaningful entries at the moment)
        packaging_l = worksheet['packaging']['output']
        for idy in range(len(packaging_l)):
            if drug_l[idx]['drug_code'] == packaging_l[idy]['drug_code']:
                drug_l[idx]['upc'] = packaging_l[idy]['upc']
                drug_l[idx]['packaging'] = (
                        packaging_l[idy]['package_type'] + '' +
                        packaging_l[idy]['package_size'] +
                        packaging_l[idy]['package_size_unit'])
                drug_l[idx]['packaging_f'] = (
                        packaging_l[idy]['package_type_f'] + '' +
                        packaging_l[idy]['package_size'] +
                        packaging_l[idy]['package_size_unit_f'])
        '''

        # packaging (replace with product_information)
        packaging_l = worksheet['packaging']['output']
        for idy in range(len(packaging_l)):
            if drug_l[idx]['drug_code'] == packaging_l[idy]['drug_code']:
                drug_l[idx]['upc'] = packaging_l[idy]['upc']
                drug_l[idx]['packaging'] = packaging_l[idy]['product_information']
                drug_l[idx]['packaging_f'] = packaging_l[idy]['product_information']

        # phamaceutical standard
        pharm_l = worksheet['pharmaceutical_standard']['output']
        for idy in range(len(pharm_l)):
            if drug_l[idx]['drug_code'] == pharm_l[idy]['drug_code']:
                drug_l[idx]['pharmaceutical_std'] = pharm_l[idy]['pharmaceutical_std']

        # route of administration
        route_l = worksheet['route_of_administration']['output']
        drug_l[idx]['admin_route'] = []
        drug_l[idx]['admin_route_f'] = []
        for idy in range(len(route_l)):
            if drug_l[idx]['drug_code'] == route_l[idy]['drug_code']:
                drug_l[idx]['admin_route'].append(
                        route_l[idy]['route_of_administration'])
                drug_l[idx]['admin_route_f'].append(
                        route_l[idy]['route_of_administration_f'])

        # schedule
        schedule_l = worksheet['schedule']['output']
        drug_l[idx]['schedule'] = []
        drug_l[idx]['schedule_f'] = []
        for idy in range(len(schedule_l)):
            if drug_l[idx]['drug_code'] == schedule_l[idy]['drug_code']:
                drug_l[idx]['schedule'].append(
                        schedule_l[idy]['schedule'])
                drug_l[idx]['schedule_f'].append(
                        schedule_l[idy]['schedule_f'])

        # product status
        status_l = worksheet['product_status']['output']
        for idy in range(len(status_l)):
            if drug_l[idx]['drug_code'] == status_l[idy]['drug_code']:
                # only care current status
                if status_l[idy]['current_status_flag'] == 'Y':
                    drug_l[idx]['status'] = status_l[idy]['status']
                    drug_l[idx]['status_f'] = status_l[idy]['status_f']

        # therapeutic class (can be skipped)
        ther_l = worksheet['therapeutic_class']['output']
        for idy in range(len(ther_l)):
            if drug_l[idx]['drug_code'] == ther_l[idy]['drug_code']:
                drug_l[idx]['tc_atc'] = ther_l[idy]['tc_atc']
                drug_l[idx]['tc_atc_f'] = ther_l[idy]['tc_atc_f']
                drug_l[idx]['tc_ahfs'] = ther_l[idy]['tc_ahfs']
                drug_l[idx]['tc_ahfs_f'] = ther_l[idy]['tc_ahfs_f']

        # veterinary species
        species_l = worksheet['veterinary_species']['output']
        drug_l[idx]['vet_species'] = []
        drug_l[idx]['vet_species_f'] = []
        for idy in range(len(species_l)):
            if drug_l[idx]['drug_code'] == species_l[idy]['drug_code']:
                drug_l[idx]['vet_species'].append(
                        species_l[idy]['vet_species'] + '' +
                        species_l[idy]['vet_sub_species'])
                drug_l[idx]['vet_species_f'].append(
                        species_l[idy]['vet_species_f'])

    #--------------------------------------------------------------------------
    # save worksheet in json format
    fname = build_prefix + '_' + prod_status + '.json'
    json.dump(worksheet['drug_product']['output'], open(fname, 'w'))


def load_build_data(prod_status):
    fname = build_prefix + '_' + prod_status + '.json'
    try:
        drugs = json.load(open(fname))
    except:
        print('ERROR: failed to load JSON data({})...'.format(fname))
        return None
    else:
        return drugs


'''
	drug_code: 100027
	product_categorization:
	class: Human
	drug_identification_number: 02511029
	brand_name: MAR-PALIPERIDONE
    product_status: ALL | APPROVED | INACTIVE | DORMANT
	descriptor:
	pediatric_flag: N
	accession_number:
	number_of_ais: 1
	last_update_date: 02-JUL-2021
	ai_group_no: 0152320002
	class_f: Humain
	brand_name_f: MAR-PALIPERIDONE
	descriptor_f:
	ingredients: ['PALIPERIDONE 6MG']
	ingredients_f: ['Palipéridone 6MG']
	company_name: ['MARCAN PHARMACEUTICALS INC']
	dosage_form: ['TABLET (EXTENDED-RELEASE)']
	dosage_form_f: ['Comprimé (à libération prolongée)']
	packaging: []
	pharmaceutical_std: ['MFR']
	admin_route: ['ORAL']
	admin_route_f: ['Orale']
	schedule: ['Prescription']
	schedule_f: ['Prescription']
	vet_species: []
	vet_species_f: []
'''
def create_sqlite_database(drug_data, prod_status, option=None):

    if option == 'OTC':
        outfilename = 'otc_' + prod_status
    elif option == 'PRS':
        outfilename = 'prs_' + prod_status
    elif option == 'OTC+PRS':
        outfilename = 'drugs_' + prod_status
    else:
        outfilename = 'all_' + prod_status

    # sqlite3 format
    dbname = outfilename + '.sql3'
    # json format to be used with firebase storage
    fbname = outfilename + '.json'
    fbdict = {}

    # delete existing sqlite3 files if any
    if os.path.isfile(dbname):
        os.remove(dbname)

    #
    # create sqlite3 database
    #
    con = sqlite3.connect(dbname)
    # get cursor
    cursor = con.cursor()
    # create schema
    cursor.execute("CREATE TABLE drugs ("
        "id TEXT, drug_code TEXT, status TEXT, status_f TEXT,"
        "company_name TEXT, company_code TEXT, ph_std TEXT, packaging TEXT,"
        "packaging_f TEXT, upc TEXT, category TEXT, class TEXT,"
        "class_f TEXT, brand_name TEXT, brand_name_f TEXT, ingredients TEXT,"
        "ingredients_f TEXT, dosage_form TEXT, dosage_form_f TEXT, admin_route TEXT,"
        "admin_route_f TEXT,schedule TEXT, schedule_f TEXT, descriptor TEXT,"
        "descriptor_f TEXT)")

    drug_id = -1
    ingredients = ''

    # insert data
    for idx in range(len(drug_data)):
        print("...processing {}/{}".format(idx,len(drug_data)), end="\r", flush=True)
        # DPD extract has potentially duplicated entries with the same drug id
        # We remove those here on the assumption that those are listed next to
        # each other. However this assumption may not be true.
        if drug_id == drug_data[idx]['drug_identification_number']:
            continue
        else:
            drug_id = drug_data[idx]['drug_identification_number']

        if option == 'OTC':
            if ('Human' not in drug_data[idx]['class']  or
                    'CAT IV' in drug_data[idx]['product_categorization'] or
                    'TEA (HERBAL)' in drug_data[idx]['dosage_form'] or
                    'SHAMPOO' in drug_data[idx]['dosage_form'] or
                    'SOAP' in drug_data[idx]['dosage_form'] or
                    'STICK' in drug_data[idx]['dosage_form'] or
                    'TOOTHPASTE' in drug_data[idx]['dosage_form'] or
                    'WIPE' in drug_data[idx]['dosage_form'] or
                    'OTC' not in drug_data[idx]['schedule']):
                continue
        elif option == 'PRS':
            if ('Human' not in drug_data[idx]['class']  or
                    'Prescription' not in drug_data[idx]['schedule']):
                continue
        elif option == 'OTC+PRS':
            if (('Human' not in drug_data[idx]['class'] or
                    'CAT IV' in drug_data[idx]['product_categorization'] or
                    'TEA (HERBAL)' in drug_data[idx]['dosage_form'] or
                    'SHAMPOO' in drug_data[idx]['dosage_form'] or
                    'SOAP' in drug_data[idx]['dosage_form'] or
                    'STICK' in drug_data[idx]['dosage_form'] or
                    'TOOTHPASTE' in drug_data[idx]['dosage_form'] or
                    'WIPE' in drug_data[idx]['dosage_form'] or
                    'OTC' not in drug_data[idx]['schedule']) and
                    ('Human' not in drug_data[idx]['class'] or
                    'Prescription' not in drug_data[idx]['schedule'])):
                continue

        cursor.execute("INSERT INTO drugs VALUES ("
            "'{}','{}','{}','{}',"
            "'{}','{}','{}','{}',"
            "'{}','{}','{}','{}',"
            "'{}','{}','{}','{}',"
            "'{}','{}','{}','{}',"
            "'{}','{}','{}','{}','{}')".format(
            drug_data[idx]['drug_identification_number'],
            drug_data[idx]['drug_code'],
            drug_data[idx]['status'],
            drug_data[idx]['status_f'].replace("'",r"''"),
            #
            drug_data[idx]['company_name'].replace("'",r"''"),
            drug_data[idx]['company_code'],
            drug_data[idx]['pharmaceutical_std'],
            drug_data[idx]['packaging'].replace("'",r"''"),
            #
            drug_data[idx]['packaging_f'].replace("'",r"''"),
            drug_data[idx]['upc'],
            drug_data[idx]['product_categorization'].replace("'",r"''"),
            drug_data[idx]['class'].replace("'",r"''"),
            #
            drug_data[idx]['class_f'].replace("'",r"''"),
            drug_data[idx]['brand_name'].replace("'",r"''"),
            drug_data[idx]['brand_name_f'].replace("'",r"''"),
            ','.join(drug_data[idx]['ingredients']).replace("'",r"''"),
            #
            ','.join(drug_data[idx]['ingredients_f']).replace("'",r"''"),
            ','.join(drug_data[idx]['dosage_form']).replace("'",r"''"),
            ','.join(drug_data[idx]['dosage_form_f']).replace("'",r"''"),
            ','.join(drug_data[idx]['admin_route']).replace("'",r"''"),
            #
            ','.join(drug_data[idx]['admin_route_f']).replace("'",r"''"),
            ','.join(drug_data[idx]['schedule']).replace("'",r"''"),
            ','.join(drug_data[idx]['schedule_f']).replace("'",r"''"),
            drug_data[idx]['descriptor'].replace("'",r"''"),
            #
            drug_data[idx]['descriptor_f'].replace("'",r"''")))

        # add the item to firebase dict with id as a key
        fbdict[drug_data[idx]['drug_identification_number']] = drug_data[idx]

    con.commit()
    con.close()

    '''
    # create archive from the sql3 file
    shutil.make_archive(outfilename, 'zip', '.', dbname)
    '''

    # create json file for firebase storage
    with open(fbname, 'w') as f:
        json.dump(fbdict, f)



def pprint(data_list, start, end, filter=None):
    idx = start
    while idx < end:
        item = data_list[idx]
        flag = True
        if filter is not None:
            for key in filter:
                if item[key] != filter[key]:
                    flag = False
                    break;
        if flag:
            print('{}'.format(idx))
            for title in item:
                print('\t{}: {}'.format(title, item[title]))
            print('--------------------------------------')
        idx = idx + 1


def show_menu():
    os.system('clear')

    return input('''
* DPD Data Set: {}
* Build Data: {}
* Selected Drug Product Status: {}
* Selected Drug Schedule Type: {}

Select Drug Product Status:
  (1) Marketed drugs
  (2) Approved drugs
  (3) Inactive drugs
  (4) Dormant drugs
  (5) All drugs

Select Drug Schedule Type:
  (a) OTC drugs + Prescription drugs
  (b) OTC drugs only
  (c) Prescription drugs only
  (d) All drug types

Select:
  (r) Generate Output File
  (x) Clear Worksheet
  (q) Quit

> '''.format(dpd_dataset, build_dataset, drug_prod_status, drug_schedule))


def check_dpd_files():

    dpd_dataset.clear()

    for k,v in suffixes.items():
        if v == '':
            dir_name = extract_dir
        else:
            dir_name = extract_dir + '_' + v

        if os.path.isdir(dir_name):
            flag = True
            print('checking ' + dir_name)

            for f in source_files:
                if v == '':
                    file_name = f
                else:
                    file_name = f.split('.')[0] + '_' + v + '.' + f.split('.')[1]

                if not os.path.isfile(dir_name + '/' + file_name):
                    print('>>>ERROR: {} not found in the {}'.format(
                        file_name, dir_name))
                    flag = False
                    break
            if flag:
                print('  ..{} looks o.k.'.format(dir_name))
                dpd_dataset.append(k)

    #print(dpd_dataset)
    return

def check_build_files():

    build_dataset.clear()

    for f in os.listdir():
        if (f.startswith(build_prefix) and
                f.endswith('.json') and
                len(f.split('_')) > 1 and
                f.split('_')[1].split('.')[0] in suffixes):
            build_dataset.append(f.split('_')[1].split('.')[0])

    print(build_dataset)

#-------------------------------------------------------------------------------
if __name__ =='__main__':

    while(True):
        check_dpd_files()
        check_build_files()

        select = show_menu()

        if select == 'q':
            print('Bye')
            exit(0)

        elif select in ('1','2','3','4','5'):
            if select == '1':
                if 'MARKETED' in  dpd_dataset:
                    drug_prod_status = ['MARKETED']
                else:
                    input('  ERROR: DPD Dataset is not found')

            elif select == '2':
                if 'APPROVED' in  dpd_dataset:
                    drug_prod_status = ['APPROVED']
                else:
                    input('  ERROR: DPD Dataset is not found')

            elif select == '3':
                if 'INACTIVE' in  dpd_dataset:
                    drug_prod_status = ['INACTIVE']
                else:
                    input('  ERROR: DPD Dataset is not found')

            elif select == '4':
                if 'DORMANT' in  dpd_dataset:
                    drug_prod_status = ['DORMANT']
                else:
                    input('  ERROR: DPD Dataset is not found')

            elif select == '5':
                if ('MARKETED' in  dpd_dataset and 'APPROVED' in  dpd_dataset and
                        'INACTIVE' in  dpd_dataset and 'DORMANT' in  dpd_dataset):
                    drug_prod_status = ['MARKETED','APPROVED', 'INACTIVE',
                            'DORMANT']
                else:
                    input('  >>ERROR DPD Dataset is not found')

        elif select in ('a','b','c','d'):
            if select == 'a':
                drug_schedule = "OTC+PRS"
            elif select == 'b':
                drug_schedule = "OTC"
            elif select == 'c':
                drug_schedule = "PRS"
            elif select == 'd':
                drug_schedule = "ALL"

        elif select == 'r':

            for prod_status in drug_prod_status:
                build_flag = True

                if prod_status in build_dataset:
                    res = input('  Build data for {} exists... '
                        'Do you want to use it? (y/N) '.format(prod_status))
                    if(res.upper() == 'Y'):
                        build_flag = False

                if build_flag == False:
                    drug_data = load_build_data(prod_status)
                    if drug_data == None:
                        print('  Failed to load build data... Rebuilding... ')
                        build_flag = True

                if build_flag == True:
                    load_dpd_extracts(prod_status)
                    build_worksheet(prod_status)
                    drug_data = load_build_data(prod_status)

                create_sqlite_database(drug_data, prod_status, drug_schedule)

        elif select == 'x':
            os.system('rm *.json *.zip *.sql3')

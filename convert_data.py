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
extract_dir = './allfiles'
# choose product status from below
# prod_status = {'MARKETED':'', 'APPROVED':'ap', 'INACTIVE':'ia', 'DORMANT':'dr'}
data_dict = {
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


def read_extracts(prod_status):

    for status in prod_status:
        print('...reading {} status dataset'.format(status))
        for key in data_dict:
            if status == 'MARKETED':
                fname = '{}/{}.txt'.format(extract_dir,data_dict[key]['input'])
            else:
                if status == 'APPROVED':
                    suffix = 'ap'
                elif status == 'INACTIVE':
                    suffix = 'ia'
                elif status == 'DORMANT':
                    suffix = 'dr'
                else:
                    print('unknown product status:', status)
                    return

                fname = '{}_{}/{}_{}.txt'.format(
                        extract_dir,suffix,data_dict[key]['input'],suffix)

            if not os.path.isfile(fname):
                print('\tERROR: file not found {}'.format(fname))
                return False
            else:
                #print('  ...reading {}'.format(fname))
                pass

            with open(fname) as f:
                csv_reader = csv.reader(f, delimiter=',')
                output = []
                lines = 0
                for row in csv_reader:
                    item = {}
                    for idx in range(len(data_dict[key]['fields'])):
                        item[data_dict[key]['fields'][idx]] = row[idx]
                    lines = lines + 1
                    # failsafe placeholders
                    item['upc'] = ''
                    item['packaging'] = ''
                    item['packaging_f'] = ''
                    item['pharmaceutical_std'] = ''
                    output.append(item)

                data_dict[key]['output'] = data_dict[key]['output'] + output

    print('...total {} drug data loaded'.format(
        len(data_dict['drug_product']['output'])))
    return True


def build_data():
    if data_dict['drug_product']['output'] == []:
        input('\tERROR: No data found...read raw data first')
        return False

    # take drug_product dict as base
    drug_l = data_dict['drug_product']['output']

    for idx in range(len(drug_l)):
        print("... {}/{}".format(idx,len(drug_l)), end="\r", flush=True)
        # fill brand_name_f if not exist
        if drug_l[idx]['brand_name_f'] == '':
            drug_l[idx]['brand_name_f'] = drug_l[idx]['brand_name']

        # active ingredients
        ingred_l = data_dict['active_ingredients']['output']
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
        company_l = data_dict['companies']['output']
        for idy in range(len(company_l)):
            if drug_l[idx]['drug_code'] == company_l[idy]['drug_code']:
                drug_l[idx]['company_name'] = company_l[idy]['company_name']
                drug_l[idx]['company_code'] = company_l[idy]['company_code']

        # dosage form
        dosage_l = data_dict['dosage_form']['output']
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
        packaging_l = data_dict['packaging']['output']
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
        packaging_l = data_dict['packaging']['output']
        for idy in range(len(packaging_l)):
            if drug_l[idx]['drug_code'] == packaging_l[idy]['drug_code']:
                drug_l[idx]['upc'] = packaging_l[idy]['upc']
                drug_l[idx]['packaging'] = packaging_l[idy]['product_information']
                drug_l[idx]['packaging_f'] = packaging_l[idy]['product_information']

        # phamaceutical standard
        pharm_l = data_dict['pharmaceutical_standard']['output']
        for idy in range(len(pharm_l)):
            if drug_l[idx]['drug_code'] == pharm_l[idy]['drug_code']:
                drug_l[idx]['pharmaceutical_std'] = pharm_l[idy]['pharmaceutical_std']

        # route of administration
        route_l = data_dict['route_of_administration']['output']
        drug_l[idx]['admin_route'] = []
        drug_l[idx]['admin_route_f'] = []
        for idy in range(len(route_l)):
            if drug_l[idx]['drug_code'] == route_l[idy]['drug_code']:
                drug_l[idx]['admin_route'].append(
                        route_l[idy]['route_of_administration'])
                drug_l[idx]['admin_route_f'].append(
                        route_l[idy]['route_of_administration_f'])

        # schedule
        schedule_l = data_dict['schedule']['output']
        drug_l[idx]['schedule'] = []
        drug_l[idx]['schedule_f'] = []
        for idy in range(len(schedule_l)):
            if drug_l[idx]['drug_code'] == schedule_l[idy]['drug_code']:
                drug_l[idx]['schedule'].append(
                        schedule_l[idy]['schedule'])
                drug_l[idx]['schedule_f'].append(
                        schedule_l[idy]['schedule_f'])

        # product status
        status_l = data_dict['product_status']['output']
        for idy in range(len(status_l)):
            if drug_l[idx]['drug_code'] == status_l[idy]['drug_code']:
                # only care current status
                if status_l[idy]['current_status_flag'] == 'Y':
                    drug_l[idx]['status'] = status_l[idy]['status']
                    drug_l[idx]['status_f'] = status_l[idy]['status_f']

        # therapeutic class (can be skipped)
        ther_l = data_dict['therapeutic_class']['output']
        for idy in range(len(ther_l)):
            if drug_l[idx]['drug_code'] == ther_l[idy]['drug_code']:
                drug_l[idx]['tc_atc'] = ther_l[idy]['tc_atc']
                drug_l[idx]['tc_atc_f'] = ther_l[idy]['tc_atc_f']
                drug_l[idx]['tc_ahfs'] = ther_l[idy]['tc_ahfs']
                drug_l[idx]['tc_ahfs_f'] = ther_l[idy]['tc_ahfs_f']

        # veterinary species
        species_l = data_dict['veterinary_species']['output']
        drug_l[idx]['vet_species'] = []
        drug_l[idx]['vet_species_f'] = []
        for idy in range(len(species_l)):
            if drug_l[idx]['drug_code'] == species_l[idy]['drug_code']:
                drug_l[idx]['vet_species'].append(
                        species_l[idy]['vet_species'] + '' +
                        species_l[idy]['vet_sub_species'])
                drug_l[idx]['vet_species_f'].append(
                        species_l[idy]['vet_species_f'])

    return True


def save_json(version, filter=None):
    fname = json_prefix + version + '.json'
    try:
        json.dump(data_dict['drug_product']['output'], open(fname, 'w'))
    except:
        return False
    else:
        return True


def load_json(version):
    fname = json_prefix + version + '.json'
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
def create_database(dlist, version, option=None):

    if option == 'OTC':
        zipname = 'otc_' + version
    elif option == 'PRS':
        zipname = 'prs_' + version
    elif option == 'OTC+PRS':
        zipname = 'drugs_' + version
    else:
        zipname = 'all_' + version

    dbname = zipname + '.sql3'
    fbname = zipname + '.json'
    fbdict = {}
    # custom output
    outname = 'drugs.json'
    outlist = []

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
    for idx in range(len(dlist)):
        print("...processing {}/{}".format(idx,len(dlist)), end="\r", flush=True)
        # remove seemingly duplicated entries on the assumption that
        # those are listed next to each other, which might not be
        # the case
        if drug_id == dlist[idx]['drug_identification_number']:
            continue
        else:
            drug_id = dlist[idx]['drug_identification_number']

        if option == 'OTC':
            if ('Human' not in dlist[idx]['class']  or
                    'CAT IV' in dlist[idx]['product_categorization'] or
                    'TEA (HERBAL)' in dlist[idx]['dosage_form'] or
                    'SHAMPOO' in dlist[idx]['dosage_form'] or
                    'SOAP' in dlist[idx]['dosage_form'] or
                    'STICK' in dlist[idx]['dosage_form'] or
                    'TOOTHPASTE' in dlist[idx]['dosage_form'] or
                    'WIPE' in dlist[idx]['dosage_form'] or
                    'OTC' not in dlist[idx]['schedule']):
                continue
        elif option == 'PRS':
            if ('Human' not in dlist[idx]['class']  or
                    'Prescription' not in dlist[idx]['schedule']):
                continue
        elif option == 'OTC+PRS':
            if (('Human' not in dlist[idx]['class'] or
                    'CAT IV' in dlist[idx]['product_categorization'] or
                    'TEA (HERBAL)' in dlist[idx]['dosage_form'] or
                    'SHAMPOO' in dlist[idx]['dosage_form'] or
                    'SOAP' in dlist[idx]['dosage_form'] or
                    'STICK' in dlist[idx]['dosage_form'] or
                    'TOOTHPASTE' in dlist[idx]['dosage_form'] or
                    'WIPE' in dlist[idx]['dosage_form'] or
                    'OTC' not in dlist[idx]['schedule']) and
                    ('Human' not in dlist[idx]['class'] or
                    'Prescription' not in dlist[idx]['schedule'])):
                continue

        cursor.execute("INSERT INTO drugs VALUES ("
            "'{}','{}','{}','{}',"
            "'{}','{}','{}','{}',"
            "'{}','{}','{}','{}',"
            "'{}','{}','{}','{}',"
            "'{}','{}','{}','{}',"
            "'{}','{}','{}','{}','{}')".format(
            dlist[idx]['drug_identification_number'],
            dlist[idx]['drug_code'],
            dlist[idx]['status'],
            dlist[idx]['status_f'].replace("'",r"''"),
            #
            dlist[idx]['company_name'].replace("'",r"''"),
            dlist[idx]['company_code'],
            dlist[idx]['pharmaceutical_std'],
            dlist[idx]['packaging'].replace("'",r"''"),
            #
            dlist[idx]['packaging_f'].replace("'",r"''"),
            dlist[idx]['upc'],
            dlist[idx]['product_categorization'].replace("'",r"''"),
            dlist[idx]['class'].replace("'",r"''"),
            #
            dlist[idx]['class_f'].replace("'",r"''"),
            dlist[idx]['brand_name'].replace("'",r"''"),
            dlist[idx]['brand_name_f'].replace("'",r"''"),
            ','.join(dlist[idx]['ingredients']).replace("'",r"''"),
            #
            ','.join(dlist[idx]['ingredients_f']).replace("'",r"''"),
            ','.join(dlist[idx]['dosage_form']).replace("'",r"''"),
            ','.join(dlist[idx]['dosage_form_f']).replace("'",r"''"),
            ','.join(dlist[idx]['admin_route']).replace("'",r"''"),
            #
            ','.join(dlist[idx]['admin_route_f']).replace("'",r"''"),
            ','.join(dlist[idx]['schedule']).replace("'",r"''"),
            ','.join(dlist[idx]['schedule_f']).replace("'",r"''"),
            dlist[idx]['descriptor'].replace("'",r"''"),
            #
            dlist[idx]['descriptor_f'].replace("'",r"''")))

        # add the item to firebase dict with id as a key
        fbdict[dlist[idx]['drug_identification_number']] = dlist[idx]

        # build custom output
        if idx % 10 == 9 and 'ORAL' in dlist[idx]['admin_route']:
            outlist.append({
                'drug_id': dlist[idx]['drug_identification_number'],
                'brand_name': dlist[idx]['brand_name'],
                'ingredients': ','.join(dlist[idx]['ingredients'])
                    .replace("'",r"''"),
                'form': ','.join(dlist[idx]['dosage_form'])
                    .replace("'",r"''"),
                'route': ','.join(dlist[idx]['admin_route'])
                    .replace("'",r"''"),
                'manufacturer': ','.join(dlist[idx]['company_name'])
                    .replace("'",r"''"),
                'descriptor': ','.join(dlist[idx]['descriptor'])
                    .replace("'",r"''")
                })
        # sort by the name (warning: this takes time)
        outlist.sort(key=lambda drug:drug['brand_name'])

    con.commit()
    con.close()

    # create archive from the sql3 file
    shutil.make_archive(zipname, 'zip', '.', dbname)

    with open(fbname, 'w') as f:
        json.dump(fbdict, f)

    with open(outname, 'w') as f:
        json.dump(outlist, f)


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


def show_menu(version, status):
    os.system('clear')
    return input('''
Target version: {}

Load input data by status:
  (1) Marketed drugs
  (2) Marketed drugs + Approved drugs
  (3) Inactive drugs
  (4) Dormant drugs
  (5) All drugs

Create output data by type:
  (a) OTC drugs + Prescription drugs
  (b) OTC drugs only
  (c) Prescription drugs only
  (d) All drug types

Select (q) to quite

:'''.format(version, status))

#-------------------------------------------------------------------------------
if __name__ =='__main__':
    version = datetime.today().strftime('%Y%m%d')
    status = 'Marketed'

    while(True):
        select = show_menu(version, status)

        if select == 'q':
            print('Bye')
            exit(0)

        if select in ('1','2','3','4','5'):
            print('reading raw data...')
            if select == '1':
                prod_status = ['MARKETED']
            elif select == '2':
                prod_status = ['MARKETED','APPROVED']
            elif select == '3':
                prod_status = ['INACTIVE']
            elif select == '4':
                prod_status = ['DORMANT']
            elif select == '5':
                prod_status = ['MARKETED','APPROVED', 'INACTIVE', 'DORMANT']

            read_extracts(prod_status)
            print('\nbuilding drug data...')
            build_data()
            print('saving drug data to json file...')
            save_json(version)
            input('...drug data saved: {}.json'.format(json_prefix + version))


        elif select in ('a','b','c','d'):
            print('loading drug data from json file...')
            drugs = load_json(version)

            if drugs is not None:
                print('creating sqlite3 database...')

                if select == 'a':
                    create_database(drugs, version, 'OTC+PRS')
                elif select == 'b':
                    create_database(drugs, version, 'OTC')
                elif select == 'c':
                    create_database(drugs, version, 'PRS')
                elif select == 'd':
                    create_database(drugs, version, 'ALL')
                input('\n...database created')
            else:
                input('Failed to load json file: Select input data first...')

        else:
                input('Wrong selection...')


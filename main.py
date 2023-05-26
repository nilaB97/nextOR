import concurrent.futures
import re
import glob
import pandas as pd
import gzip
import shutil
import gba_qualitaetsberichte as qb
from gba_qualitaetsberichte.utils import (replace_and_save_nan, fix_standort, assign_bundesland)
import clean_data as cd
import pyprind
import os
import pandas as pd
import xml.etree.ElementTree as ET
from lxml import etree
import random

def query_data(years):
    # assign directory
    # directory = "C:/Users/nely9/Downloads/xml_2021/xml_2021"

    qr = qb.QualityReports(path='data/', years=years)

    hygiene_df = qr.query(dict(
        anzahl_betten='Anzahl_Betten',
    ))

    hygiene_df = fix_standort(hygiene_df)
    hygiene_df = assign_bundesland(hygiene_df)
    hygiene_df = hygiene_df.set_index(['ik-so', 'year'])
    assert hygiene_df.index.is_unique
    hygiene_df = replace_and_save_nan(hygiene_df, replacement='0', val_cols=['plz'])
    hygiene_df.to_csv(f'data/data_{years}.csv')
    plz = hygiene_df['plz']
    filtered_df = hygiene_df.loc[hygiene_df['ik_bl'].str.contains('5')]
    filtered_df.to_csv(f'data/filtered_df_{years}.csv')

    return filtered_df


def zip_data(years):
    paths = []

    for year in years:
        print('Year: ', year)
        files = glob.glob(f'data/base_{year}/*xml.xml')
        updater = pyprind.ProgPercent(len(files))
        for file in files:
            updater.update()  # update progress bar for each query
            with open(file, 'rb') as f_in:
                with gzip.open(f'{file}.gz', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)


def parse_path(path):
    # print(f'Path {path}')
    COLUMN_NAMES = ['path', 'ik','year', 'OPS_CODE', 'Anzahl']
    ops_codes_path = pd.DataFrame(columns=COLUMN_NAMES)
    path_split = path.split('-')
    #print(path_split)
    year = None
    for str in path_split:
        if len(str)==4:
            year = str
    #print(year)

    parser = etree.XMLParser(collect_ids=False)
    with gzip.open(path) as f:
        tree = etree.parse(f, parser)
    found_ops = False
    ops_code = None
    anzahl = None
    ik = None
    count_ops = 0
    for elem in tree.iter():
        if elem.tag == "IK":
            ik = elem.text

        if found_ops:
            # if ops code was found in the previous iteration, then we can now either extract the number of operations done
            # or it was less than 4 operations
            if elem.tag == 'Anzahl':
                anzahl = elem.text
            else:
                anzahl = random.choice([1,2,3])
            # add row to panda dataframe
            ops_codes_path.loc[count_ops] = [path, ik,year, ops_code, anzahl]
            count_ops += 1
            found_ops = False

        if elem.tag == "OPS_301":
            found_ops = True
            ops_code = elem.text
    return ops_codes_path

def query_ops_codes(years,filtered_df):
    paths = filtered_df['path'].to_numpy()  # haben die ersten 500 schon
    print(len(paths))

    COLUMN_NAMES = ['path', 'ik', 'year', 'OPS_CODE', 'Anzahl']
    ops_codes = pd.DataFrame(columns=COLUMN_NAMES)

    print()
    print('Create OPSCode List')
    print()
    updater_path = pyprind.ProgPercent(len(paths))
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(parse_path, paths)

        for i, result in enumerate(results):
            updater_path.update()
            ops_codes = ops_codes.append(result)

            if i % 500 == 0:
                filepath = f"data/ops_codes_{years}_{i}.csv"
                ops_codes.to_csv(filepath)

    filepath = f"data/ops_codes_{years}.csv"
    ops_codes.to_csv(filepath)
##########################################################################################

if __name__ == "__main__":
    years = [2015,2016,2017,2018,2019,2020,2021]  # ,2013,2014,2015,2016,2017,2018,2019,2020,2021,]
    # zip_data(years)
    # print('Finish Zipping')
    #filtered_df = query_data(years)
    #print('Finish querying data and filtering it')

    filtered_df = pd.read_csv('data/filtered_df_[2015, 2016, 2017, 2018, 2019, 2020, 2021].csv')

    # create stats of data to see what needs to be done
    no_of_datapoints_per_hospital = filtered_df[['ik-name', 'year']].groupby(by='ik-name', as_index=False).count()

    # find the datapoints for which we have less than and equal to 3 datapoints from 2015 to 2021
    no_of_datapoints_per_hospital_drop = no_of_datapoints_per_hospital[no_of_datapoints_per_hospital.year <= 3]

    # erase all rows from filtered data that are in the above dataframe
    for index, row in no_of_datapoints_per_hospital_drop.iterrows():
        filtered_df = filtered_df.drop(filtered_df[filtered_df['ik-name'] == row['ik-name']].index)
    print('Finish deleting datapoints for which we have less than and equal to 3 datapoints from 2015 to 2021')

    #get OPS Codes
    #query_ops_codes(years,filtered_df)
    #print('Finish querying ops_Codes')

    ops_codes = pd.read_csv('data/ops_codes_[2015, 2016, 2017, 2018, 2019, 2020, 2021].csv')

    #aggreagte ops codes
    ops_codes = cd.aggregate_ops(ops_codes)
    print('Finish aggregating ops_Codes')

    #add nan rows for missing years
    ops_codes = cd.add_nan_rows_ops(ops_codes,years)
    filepath = f"data/ops_codes_[2015, 2016, 2017, 2018, 2019, 2020, 2021]_cleaned.csv"
    ops_codes.to_csv(filepath)
    print('Finish adding nan_rows ops_Codes and saving it')

    # aggregate hospitals that were seperated,
    filtered_df = cd.aggregate_data(filtered_df)
    print('Finish aggregating filtered data')

    filtered_df = cd.add_nan_rows(filtered_df,years)
    print('Finish adding nan rows to filtered data')

    # save cleaned data in csv
    filepath = f"data/filtered_df_[2015, 2016, 2017, 2018, 2019, 2020, 2021]_cleaned.csv"
    filtered_df.to_csv(filepath)
    print('Finish')





import pandas as pd
import numpy as np
from uszipcode import SearchEngine
import sqlite3
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

search =  SearchEngine(db_file_dir="/tmp/db")
conn = sqlite3.connect("/tmp/db/simple_db.sqlite")

def get_zip(df):
    pdf = pd.read_sql_query('''select  zipcode, lat, lng, bounds_west, bounds_east,
                             bounds_north, bounds_south from simple_zipcode where state in ("CO") ''', conn)
    zip_series= []
    # only search for zip codes once (unique coordinates), then join back to original dataframe
    df2 = df.copy()
    df2.drop_duplicates(subset = ['latitude','longitude'], inplace = True)
    df2 = df2.reset_index(drop = True)
    lat_series = df2['latitude']
    lng_series = df2['longitude']

    for k in range(len(lat_series)):
        lat = lat_series.iloc[k]
        lng = lng_series.iloc[k]
        try:
            out = pdf[(pdf['bounds_north']>=lat) &
                      (pdf['bounds_south']<=lat) &
                      (pdf['bounds_west']<=lng) &
                      (pdf['bounds_east']>=lng) ]
            dist = [None]*len(out)
            for i in range(len(out)):
                dist[i] = (out['lat'].iloc[i]-lat)**2 + (out['lng'].iloc[i]-lng)**2
            zip = str(out['zipcode'].iloc[dist.index(min(dist))])
        except:
            zip = np.nan
        zip_series.append(zip)
    zip_series = pd.Series(zip_series)
    coord_zip = pd.concat([lat_series, lng_series, zip_series], axis = 1)
    coord_zip.rename(columns = {0:'zip'}, inplace = True)
    
    return coord_zip

def load_denver(load_new, save_clean = False):
    """
    https://www.denvergov.org/opendata/dataset/city-and-county-of-denver-crime
    """
    denver = None
    def clean_denver(df, min_date = '2018-01-01'):
        '''
        Do basic data cleanup and join with the offense code groups in order
        to have the text of the groups (vs. code number).
        Default min date is set to 2018 (denver dataframe will contain records greater than that date)
        '''
        dh = df.copy()
        # Basic data cleanup
        dh['FIRST_OCCURRENCE_DATE'] = pd.to_datetime(dh['FIRST_OCCURRENCE_DATE'])
        dh = dh.loc[(dh['GEO_LAT'].notna()) & (dh['FIRST_OCCURRENCE_DATE'].notna()) & (dh['FIRST_OCCURRENCE_DATE'] >= min_date),]
        dh.drop(columns = ['INCIDENT_ID','OFFENSE_ID','OFFENSE_CODE','OFFENSE_CODE_EXTENSION','LAST_OCCURRENCE_DATE','REPORTED_DATE','INCIDENT_ADDRESS','GEO_X','GEO_Y','DISTRICT_ID','PRECINCT_ID','NEIGHBORHOOD_ID','IS_CRIME','IS_TRAFFIC'], inplace = True)
        dh = dh.sort_values('FIRST_OCCURRENCE_DATE', ascending = False)
        
        # More cleanup
        dh.rename(columns = {'FIRST_OCCURRENCE_DATE': 'crimedate', 'OFFENSE_TYPE_ID': 'description','OFFENSE_CATEGORY_ID': 'group', 'GEO_LAT':'latitude', 'GEO_LON':'longitude'}, inplace = True)
        dh = dh[['crimedate', 'description','group','latitude','longitude']]
        dh = dh.sort_values('crimedate', ascending = False).reset_index(drop = True)
        
        return dh
        
    if load_new:
        denvercrime_csv = 'https://www.denvergov.org/media/gis/DataCatalog/crime/csv/crime.csv'
        denver = pd.read_csv(denvercrime_csv)
        denver.to_csv('denver.csv', index = False)
        denver = clean_denver(denver)
    else:
        try:
            denver = pd.read_csv('denver.csv')
            denver = clean_denver(denver)
        except:
            print("denver.csv not found in local directory, loading from web!")
            denvercrime_csv = 'https://www.denvergov.org/media/gis/DataCatalog/crime/csv/crime.csv'
            denver = pd.read_csv(denvercrime_csv)
            denver.to_csv('denver.csv', index = False)
            denver = clean_denver(denver)

    print('Data loaded and cleaned - now finding zip codes (this will take a minute).')
    clean_zips = get_zip(denver)
    denver = pd.merge(clean_zips, denver, how = "inner", left_on = ['latitude','longitude'], right_on = ['latitude','longitude'])
    denver = denver[denver['zip'].notna()]
    denver = denver[['crimedate','description','group','latitude','longitude','zip']]
    denver.drop_duplicates(inplace = True)
    denver = denver.reset_index(drop = True)
    denver = denver.sort_values(by = 'crimedate', ascending = False).reset_index(drop = True)
    denver['zip'] = denver['zip'].astype(str)

    if save_clean:
        denver.to_csv("denver_clean.csv", index = False)
    print('Complete!')
    return denver

load_denver(load_new=False, save_clean=True)
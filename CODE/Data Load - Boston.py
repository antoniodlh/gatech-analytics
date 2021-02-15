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
                             bounds_north, bounds_south from simple_zipcode where state in ("MA") ''', conn)
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

# For Boston Crime (through API at https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b)
def load_boston(load_new, save_clean = False):
    """
    Load Boston Crime data found at https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b
    and do some basic cleaning.
    link is from download button at https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system
    """
    boston = None
    def clean_boston(df, min_date = '2018-01-01'):
        '''
        Do basic data cleanup and join with the offense code groups in order
        to have the text of the groups (vs. code number).
        Default min date is set to 2018 (boston dataframe will contain records greater than that date)
        '''
        dh = df.copy()
        # Basic data cleanup
        dh['OCCURRED_ON_DATE'] = pd.to_datetime(dh['OCCURRED_ON_DATE'])
        dh = dh.loc[(dh['Lat'].notna()) & (dh['OCCURRED_ON_DATE'].notna()) & (dh['OCCURRED_ON_DATE'] >= min_date),]
        dh.drop(columns = ['INCIDENT_NUMBER', 'DISTRICT','REPORTING_AREA','YEAR','MONTH','DAY_OF_WEEK','HOUR','UCR_PART','STREET','Location'], inplace = True)
        dh = dh.sort_values('OCCURRED_ON_DATE', ascending = False)
        
        # Join with offense codes dataset and further cleanup
        offense_codes = pd.read_excel('https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/3aeccf51-a231-4555-ba21-74572b4c33d6/download/rmsoffensecodes.xlsx')
        # Offense codes spreadsheet was found to have duplicates (by offense code) - removing these duplicates and taking the first entry
        offense_codes.drop_duplicates(subset = ['CODE'], inplace = True)
        dh = pd.merge(dh, offense_codes, how = "inner", left_on = "OFFENSE_CODE", right_on = "CODE")
        # The step above actually turns out to be meaningless (although it doesn't hurt anything, so I'm leaving it in)
        
        # More cleanup
        dh.drop(columns = ['OFFENSE_CODE','OFFENSE_CODE_GROUP','CODE'], inplace = True)
        dh.rename(columns = {'OCCURRED_ON_DATE': 'crimedate', 'OFFENSE_DESCRIPTION': 'description','NAME':'OffenseGroup','SHOOTING':'shooting', 'Lat':'latitude', 'Long':'longitude'}, inplace = True)
        dh = dh[['crimedate', 'description','shooting','latitude','longitude']] # Removed 'OffenseGroup', as this is not reported consistently anymore
        dh['shooting'] = np.where(dh['shooting'].isin(['Y', 1]), 1, np.nan)
        dh = dh.sort_values('crimedate', ascending = False).reset_index(drop = True)
        
        return dh
        
    if load_new:
        bostoncrime_csv = 'https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b/download/tmpm5j20gma.csv'
        boston = pd.read_csv(bostoncrime_csv)
        boston.to_csv('boston.csv', index = False)
        boston = clean_boston(boston)
    else:
        try:
            boston = pd.read_csv('boston.csv')
            boston = clean_boston(boston)
        except:
            print("boston.csv not found in local directory, loading from web!")
            bostoncrime_csv = 'https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b/download/tmpm5j20gma.csv'
            boston = pd.read_csv(bostoncrime_csv)
            boston.to_csv('boston.csv', index = False)
            boston = clean_boston(boston)

    print('Data loaded and cleaned - now finding zip codes (this will take a minute).')
    clean_zips = get_zip(boston)
    boston = pd.merge(clean_zips, boston, how = "inner", left_on = ['latitude','longitude'], right_on = ['latitude','longitude'])
    boston = boston[boston['zip'].notna()]
    boston = boston[['crimedate','description','shooting','latitude','longitude','zip']]
    boston.drop_duplicates(inplace = True)
    boston = boston.reset_index(drop = True)
    boston = boston.sort_values(by = 'crimedate', ascending = False).reset_index(drop = True)
    boston['zip'] = boston['zip'].astype(str)
    if save_clean:
        boston.to_csv("boston_clean.csv", index = False)
    print('Complete!')
    return boston

load_boston(load_new=False, save_clean=True)
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
                             bounds_north, bounds_south from simple_zipcode where state in ("MD") AND lat IS NOT NULL ''', conn)
    zip_series= []
    # only search for zip codes once (unique coordinates), then join back to original dataframe
    df2 = df.copy()
    df2.drop_duplicates(subset = ['latitude','longitude'], inplace = True)
    df2 = df2.reset_index(drop = True)
    #df2.to_csv('temp.csv', index = False)
    lat_series = df2['latitude']
    lng_series = df2['longitude']
    #print(pdf.head(1000))
    for k in range(len(lat_series)):
        lat = lat_series.iloc[k]
        lng = lng_series.iloc[k]
        try:
            out = pdf[((pdf['bounds_north'])>=lat) & 
                  ((pdf['bounds_south'])<=lat) & 
                  ((pdf['bounds_west'])<=lng) &  
                  ((pdf['bounds_east'])>=lng) ]
            dist = [None]*len(out)
            #print(len(out))
            for i in range(len(out)):
                dist[i] = (out['lat'].iloc[i]-lat)**2 + (out['lng'].iloc[i]-lng)**2
            zip = str(out['zipcode'].iloc[dist.index(min(dist))])
        except:
            zip = np.nan 
        zip_series.append(zip)
    zip_series = pd.Series(zip_series)
    coord_zip = pd.concat([lat_series, lng_series, zip_series], axis = 1)
    coord_zip.rename(columns = {0:'zip'}, inplace = True)
    #coord_zip.to_csv('baltimore_coords.csv', index = False)
    return coord_zip

def load_baltimore(max_records, always_load_new = False, save_clean = False):
    '''
    Function to either load a local (csv) copy of the Baltimore crime dataset (found at https://data.baltimorecity.gov/Public-Safety/BPD-Part-1-Victim-Based-Crime-Data/wsfq-mvij)
    It will load a csv copy initially, then checks to see if that copy has recent data.
    If the data is too old > 10 days, it will load a new copy from the API endpoint.
    Set always_load_new to True in order to pull from API regardless of local (csv) copies.
    '''
    # For checking whether data is too old
    from datetime import date, datetime
    today = datetime.combine(date.today(), datetime.min.time())
    
    # API Endpoint with application token and max records (100,000 will be more than enough for data through all of 2019)
    baltimorecrime_json = f'https://data.baltimorecity.gov/resource/wsfq-mvij.json?$$app_token=Rx4VY5j20aC1708QMp4Zijbxd&$limit={max_records}'
    
    # Modify clean_baltimore for data cleanup/integration
    def clean_baltimore(df):
        mask = df['crimetime'].notna()
        df = df.loc[mask,]
        df = df.loc[(df["latitude"].notna()) & (df["latitude"] != 0), ]
        df['crimedate'] = pd.to_datetime(df['crimedate'].str.split("T").apply(lambda x: x[0]) + ' ' + df['crimetime'])
        df.drop(columns = ['crimetime', 'crimecode', 'post','vri_name1','total_incidents','district','neighborhood', 'location', 'premise','inside_outside'], inplace = True)
        df['weapon'] = np.where(df['weapon'] == 'NA', np.nan, df['weapon'])
        df = df[['crimedate','description','weapon','latitude','longitude']].reset_index(drop = True)
        return df
    
    if always_load_new:
        baltimore = pd.read_json(baltimorecrime_json)
        baltimore.to_csv("baltimore.csv", index = False)
        baltimore = clean_baltimore(baltimore)
    else:
        try:
            # try to load the csv
            baltimore = pd.read_csv("baltimore.csv")
            baltimore = clean_baltimore(baltimore)
            print('Local CSV loaded.')
            # if the csv is too old, then load a new copy
            if (today - baltimore['crimedate'].max()).days >= 10:
                baltimore = pd.read_json(baltimorecrime_json)
                baltimore.to_csv("baltimore.csv", index = False)
                baltimore = clean_baltimore(baltimore)
                print('Local CSV too old, loaded a new copy from API endpoint and stored it locally as baltimore.csv')
        except:
            # if there was no csv in the first place, then load a copy through API
            baltimore = pd.read_json(baltimorecrime_json)
            baltimore.to_csv("baltimore.csv", index = False)
            baltimore = clean_baltimore(baltimore)
            print('Crime data loaded via API endpoint and stored locally as baltimore.csv')
    print('Data loaded and cleaned - now finding zip codes (this will take a minute).')
    
    clean_zips = get_zip(baltimore)
    baltimore = pd.merge(clean_zips, baltimore, how = "inner", left_on = ['latitude','longitude'], right_on = ['latitude','longitude'])
    baltimore = baltimore[['crimedate','description','weapon','latitude','longitude','zip']]
    baltimore.drop_duplicates(inplace = True)
    baltimore = baltimore.reset_index(drop = True)
    baltimore = baltimore.sort_values(by = 'crimedate', ascending = False).reset_index(drop = True)
    baltimore['zip'] = baltimore['zip'].astype(str)

    print(f'Baltimore Crime dataframe ready for use, there are {len(baltimore)} records.')
    
    if save_clean:
        baltimore.to_csv("baltimore_clean.csv", index = False)
    return baltimore

load_baltimore(max_records=125000, always_load_new=False, save_clean=True)
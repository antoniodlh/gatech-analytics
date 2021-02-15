import pandas as pd
import numpy as np
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def load_phoenix(load_new, save_clean = False):
    """
    https://www.phoenixopendata.com/dataset/crime-data/resource/0ce3411a-2fc6-4302-a33f-167f68608a20
    """
    phoenix = None
    def clean_phoenix(df, min_date = '2018-01-01'):
        '''
        Do basic data cleanup and join with the offense code groups in order
        to have the text of the groups (vs. code number).
        Default min date is set to 2018 (phoenix dataframe will contain records greater than that date)
        '''
        dh = df.copy()
        # Basic data cleanup
        dh['OCCURRED ON'] = pd.to_datetime(dh['OCCURRED ON'])
        dh = dh.loc[(dh['ZIP'].notna()) & (dh['OCCURRED ON'].notna()) & (dh['OCCURRED ON'] >= min_date),]
        dh.drop(columns = ['INC NUMBER','OCCURRED TO','100 BLOCK ADDR','PREMISE TYPE'], inplace = True)
        
        # More cleanup
        dh.rename(columns = {'OCCURRED ON': 'crimedate', 'UCR CRIME CATEGORY': 'description', 'ZIP':'zip'}, inplace = True)
        dh = dh[['crimedate', 'description','zip']]
        dh = dh.sort_values('crimedate', ascending = False).reset_index(drop = True)
        
        return dh
        
    if load_new:
        phoenixcrime_csv = 'https://www.phoenixopendata.com/dataset/cc08aace-9ca9-467f-b6c1-f0879ab1a358/resource/0ce3411a-2fc6-4302-a33f-167f68608a20/download/crimestat.csv'
        phoenix = pd.read_csv(phoenixcrime_csv)
        phoenix.to_csv('phoenix.csv', index = False)
        phoenix = clean_phoenix(phoenix)
    else:
        try:
            phoenix = pd.read_csv('phoenix.csv')
            phoenix = clean_phoenix(phoenix)
        except:
            print("phoenix.csv not found in local directory, loading from web!")
            phoenixcrime_csv = 'https://www.phoenixopendata.com/dataset/cc08aace-9ca9-467f-b6c1-f0879ab1a358/resource/0ce3411a-2fc6-4302-a33f-167f68608a20/download/crimestat.csv'
            phoenix = pd.read_csv(phoenixcrime_csv)
            phoenix.to_csv('phoenix.csv', index = False)
            phoenix = clean_phoenix(phoenix)

    phoenix['zip'] = phoenix['zip'].astype(str)

    if save_clean:
        phoenix.to_csv("phoenix_clean.csv", index = False)
    print('Complete!')
    return phoenix

load_phoenix(load_new=False, save_clean=True)
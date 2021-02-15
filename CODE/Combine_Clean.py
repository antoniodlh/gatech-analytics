'''
This code takes multiple inputs:
    1) CrimeTypes.xlsx (excel mapping file)
    2) "cleaned" crime data from the four cities (see file_list variable and city_names variable)

It returns a combined, grouped version of the data by mapping each cities crime descriptions to a list of basic crime types,
and further groups those by Violent vs. Non-Violent crimes (return counts of each for each zip code/date).

This code depends on the data loading and manipulation done for each city in these python scripts (which output the clean csv files referenced below):
    1) Data Load - Baltimore.py
    2) Data Load - Boston.py
    3) Data Load - Denver.py
    4) Data Load - Phoenix.py

'''
import pandas as pd 
import numpy as np
import datetime
print('running..')
# Import mapping file 
mapping = pd.read_excel('CrimeTypes.xlsx', sheet_name= 'Mapping')

file_list = ['baltimore_clean.csv','denver_clean.csv','boston_clean.csv','phoenix_clean.csv']
city_names = ['Baltimore','Denver','Boston','Phoenix']

common_fields = ['crimedate','mapping','zip','group']

df = pd.DataFrame(columns = common_fields) 

for i in zip(file_list, city_names):
    city_mapping = mapping[mapping['city'] == i[1]] # get field mapping for each city 
    crime_data = pd.read_csv(i[0]) # get the crime data
    crime_data = crime_data[['crimedate','description','zip']] # filter crime data to relevant fields 
    crime_data = pd.merge(crime_data, city_mapping, how = "inner", on = "description") # join to mapping file
    crime_data = crime_data[crime_data['mapping'].notna()] # remove records where join returned nothing (intentional to limit analysis to certain categories of crime)
    crime_data['zip'] = crime_data['zip'].astype(int).astype(str) # convert zip code to string
    crime_data = crime_data[common_fields] # narrow down joined crime data to relevant fields
    crime_data['city'] = i[1] # add city name as a column (may not be necessary, but might make for easier filtering)
    #crime_data = crime_data[crime_data['mapping'].notna()] # removed because this is done above
    crime_data['crimedate'] = pd.to_datetime(crime_data['crimedate']) # convert date string to datetime
    df = df.append(crime_data, ignore_index = True) # append to dataframe

df['crimedate'] = df['crimedate'].dt.date # grab only the date from crimedate
df['count'] = 1 # helper column for counts

# Tranformations to group by original crime mappings and create a pivot table from this
grouped_df = df.groupby([ 'crimedate', 'zip', 'city','mapping'])['count'].count().reset_index()
grouped_df.rename(columns = {'mapping': 'description'}, inplace = True)
grouped_df = grouped_df[(grouped_df['crimedate'] >= datetime.date(2019, 9, 1)) & (grouped_df['crimedate'] <= datetime.date(2020, 9, 30))]
pivoted = grouped_df.pivot(index=['crimedate','zip','city'], columns='description', values = 'count').reset_index()

# Transformations to do the same with the 'group' field
grouped_df2 = df.groupby([ 'crimedate', 'zip', 'city','group'])['count'].count().reset_index()
grouped_df2.rename(columns = {'group': 'description'}, inplace = True)
grouped_df2 = grouped_df2[(grouped_df2['crimedate'] >= datetime.date(2019, 9, 1)) & (grouped_df2['crimedate'] <= datetime.date(2020, 9, 30))]
pivoted2 = grouped_df2.pivot(index=['crimedate','zip','city'], columns='description', values = 'count').reset_index()

# Join pivoted and pivoted2 to get crime by mapping category and summary category
pivoted = pd.merge(pivoted, pivoted2, how = "inner", on = ['crimedate','zip','city']) # join to mapping file


pivoted.to_csv('combined_crime_data.csv', index = False)
print('complete - combined_crime_data.csv is in local directory')
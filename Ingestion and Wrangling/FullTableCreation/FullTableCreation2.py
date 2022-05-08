"""
Code to merge several datasets to get a first full table to be tranformed for ML

Reads data from the teams S3 bucket and merges them all together

This .py file simply merges the FIRMS data with WFIGS data.

FIRMS (Fire Information Resource Management System) exports of satellite fire dections can be optained in the following link:
https://firms.modaps.eosdis.nasa.gov/download/

WFIGS Data ( Wildland Fire Interagency Geospatial Services) data dcan be found in the following link:
https://data-nifc.opendata.arcgis.com/search?tags=Category%2Chistoric_wildlandfire_opendata 

For team:
    This version code was used to create MergeTable1 and MergeTable2 

BA_
"""

#general imports
import pandas as pd
import boto3
import geopy.distance  
import numpy as np

"""
Pull and concat similar data to start.
We will merge the different datatypes soon
"""

#TODO For Team: enter the credentails below to run
S3_Key_id=''
S3_Secret_key=''

def pull_data(Key_id, Secret_key, file):
    """
    Function which CJ wrote to pull data from S3 
    """
    BUCKET_NAME = "gtown-wildfire-ds"
    OBJECT_KEY = file
    client = boto3.client(
        's3',
        aws_access_key_id= Key_id,
        aws_secret_access_key= Secret_key)
    obj = client.get_object(Bucket= BUCKET_NAME, Key= OBJECT_KEY) 
    file_df = pd.read_csv(obj['Body'])
    return (file_df)

#read the csvs from the S3 using the pull_data function 
print('Pulling data from S3 into dataframes...')
file = 'fire_archive_M-C61_268391.csv'
df_modis1 = pull_data(S3_Key_id, S3_Secret_key, file) #get MODIS data 1
file = 'fire_nrt_M-C61_268391.csv'
df_modis2 = pull_data(S3_Key_id, S3_Secret_key, file) #get MODIS data 2
file = 'fire_nrt_J1V-C2_268392.csv'
df_viirs = pull_data(S3_Key_id, S3_Secret_key, file) #get VIIRS data
file = 'USDAJan2018ToMar2022.csv'
df_usda = pull_data(S3_Key_id, S3_Secret_key, file) #get USDA data 
file = 'WFIGS_Pulled5-5-2022.csv'
df_wfigs = pull_data(S3_Key_id, S3_Secret_key, file) #get WFIGS data 
print('Data pulled from S3 into dataframes')

#concatonate the MODIS data only
#df_MODIS = pd.concat([df_modis1, df_modis2])
#concatonate all of the FIRMS data 
df_FIRMS = pd.concat([df_modis1, df_modis2, df_viirs])

"""
All main data sets are retrieved in respective data frames

In this file, lets start by merging MODIS with WFIGS

Start by converting MODIS to a mergable dataframe
"""

print('Adjusting the FIRMS dataframe for merging...')
#get a rounded longitude and latitude 
df_FIRMS['lat'] = round(df_FIRMS['latitude'], 1)
df_FIRMS['long'] = round(df_FIRMS['longitude'], 1)

#convert the hour 
def GetHour(acq_time):
    """
    Function which gets the hour number from the FIRMS aquired hour
    """
    aqtime = str(acq_time)
    size = len(aqtime)
    hr = aqtime[:size - 2] #get the hours
    #add the 0 if we need it to match the fire db
    if len (hr) == 1:
        hr = '0' + hr
    return hr

df_FIRMS['hour'] = df_FIRMS['acq_time'].apply(GetHour) #make the exclusive hour column
#make the date loc column
df_FIRMS['date_loc'] = df_FIRMS['acq_date'] + ',' + df_FIRMS['lat'].astype(str) + ',' + df_FIRMS['long'].astype(str)
#make the new date hour loc column
df_FIRMS['date_hour_loc'] = df_FIRMS['acq_date'] + ',' + df_FIRMS['hour'].astype(str) + ',' + df_FIRMS['lat'].astype(str) + ',' + df_FIRMS['long'].astype(str)

"""
The MODIS dataframe now has a mergable field of the time and place 'date_time_loc'

Now getting a similar key for the WFIGS database
"""

print('Adjusting the WFIGS dataframe for merging...')
# now starting to transform the fire db
#getting the fire discovered date into the same format as the same date
df_wfigs['disc_date'] = df_wfigs['irwin_FireDiscoveryDateTime'].astype(str).str[0:10]
df_wfigs['disc_date'] = df_wfigs['disc_date'].astype(str).str.replace('/', '-')
#get the hour
df_wfigs['hour'] = df_wfigs['irwin_FireDiscoveryDateTime'].astype(str).str[11:13] #make the column for just hour
#convert the init long and lat frame to 2 decimals like i did in the satellite df 
df_wfigs['init_lat_rounded'] = round(df_wfigs['irwin_InitialLatitude'], 1)
df_wfigs['init_long_rounded'] = round(df_wfigs['irwin_InitialLongitude'], 1)
#make the date loc column
df_wfigs['disc_date_loc'] = df_wfigs['disc_date'] + ',' + df_wfigs['init_lat_rounded'].astype(str) + ',' + df_wfigs['init_long_rounded'].astype(str)
#make the column for date hour loc
df_wfigs['disc_date_hour_loc'] = df_wfigs['disc_date'] + ',' + df_wfigs['hour'] + ',' + df_wfigs['init_lat_rounded'].astype(str) + ',' + df_wfigs['init_long_rounded'].astype(str)

"""
Both datframes should be ready for merging on the key

Merging on the date + location key.
"""

print('Merging the dataframes...')
dfDiscovered = df_FIRMS.merge(df_wfigs, left_on='date_loc', right_on='disc_date_loc', how='inner') #creates df of only dsetected areas
dfMerged = df_FIRMS.merge(df_wfigs, left_on='date_loc', right_on='disc_date_loc', how='left') #creates df of both 
dfMerged['FIRE_DETECTED'] = dfMerged['OBJECTID'].isnull() #if there was no fire detectection set to true
dfMerged['FIRE_DETECTED'] = ~dfMerged['FIRE_DETECTED'].astype(bool) #flip so that if detection set to true

"""
Convert results to csv
"""
print('Converting to csv...')
dfMerged.to_csv('CSVNAME.csv') #TODO change the name of the csv to somethign desireable



"""
Code to merge several datasets to get full table(s) to be tranformed for ML

Reads data from the teams S3 bucket and merges them all together

This .py file simply merges the FIRMS data with WFIGS data.

FIRMS (Fire Information Resource Management System) exports of satellite fire dections can be optained in the following link:
https://firms.modaps.eosdis.nasa.gov/download/

WFIGS Data ( Wildland Fire Interagency Geospatial Services) data dcan be found in the following link:
https://data-nifc.opendata.arcgis.com/search?tags=Category%2Chistoric_wildlandfire_opendata 

For team:
    This version code was used to create FIRMSandSCANFull2018toApr2022.csv

BA_
"""

#general imports
import pandas as pd
import boto3
import geopy.distance  
import numpy as np

"""
Pull from S3 and concat similar data to start.
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
df_wfigs = pull_data(S3_Key_id, S3_Secret_key, file) #get WFIGS (small) data table 
file = 'WFIGS_big_Pulled5-8-2022.csv'
df_wfigsbig = pull_data(S3_Key_id, S3_Secret_key, file) #get WFIGS (big) data table 
print('Data pulled from S3 into dataframes')
#concatonate all of the FIRMS data 
df_FIRMS = pd.concat([df_modis1, df_modis2, df_viirs])

"""
All main data sets are retrieved in respective data frames

In this file, lets start by merging FIRMS and WFIGS

Start by converting FIRMS to a mergable dataframe
"""

latlonground = 1 #Sets the number of decimal places to round latlong
print('Adjusting the FIRMS dataframe for merging...')
#get a rounded longitude and latitude 
df_FIRMS['lat'] = round(df_FIRMS['latitude'], latlonground) #rounds lat long into new column, using latlonground # of decimal pts
df_FIRMS['long'] = round(df_FIRMS['longitude'], latlonground) #rounds lat long into new column, using latlonground # of decimal pts
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

dftmp_FIRMS = df_FIRMS.iloc[:1000] #Creates a temporary dataframe for testing  

"""
The FIRMS dataframe now has a mergable field of the time and place 'date_loc'

Now tying wether from USDA into each FIRMS location
"""
print('Merging USDA data to FIRMS...')
#get a dataframe only of each unique station 
StationDFCol = df_usda.columns[1:13] #list of columns relevant for the station 
dfStation = df_usda[StationDFCol] #get a dataframe of just the station info
dfStation = dfStation.drop_duplicates() #remove redundant rows so now just a new df of station info

#convert both the FIRMS and USDA to date time object
df_FIRMS['newdate'] = pd.to_datetime(df_FIRMS['acq_date'], format = '%Y-%m-%d') #converts the df to date time 
def ReplaceSlash(datein):
    dashdate = datein.replace('/', '-')
    return dashdate 
df_usda['date_new'] = df_usda['Date'].apply(ReplaceSlash) #converts the format
df_usda['date_new'] =  pd.to_datetime(df_usda['date_new'], format = '%Y-%m-%d') #converts the df to date time 
#Both dfs now have a date in pandas date time format 
def CalcDist(lat1, long1, lat2, long2):
    """
    Uses geopy python module
    Function which gets distance between any 2 coordinates
    Returns distance in miles
    """
    coords_1 = (lat1, long1)
    coords_2 = (lat2, long2)
    dist = geopy.distance.geodesic(coords_1, coords_2).miles
    return dist

def CalcDistDF(row):
    """
    Same function as calc dist but for a row in a df
    """
    stationLat = row['Latitude'] 
    stationLong = row['Longitude']
    detectionLat = row['latcompare']
    detectionLong = row['longcompare']
    calcDist = CalcDist(stationLat, stationLong, detectionLat, detectionLong)
    return (calcDist)

df_FIRMS['nearestStation'] = '' #nearest station name 
df_FIRMS['StationDist'] = np.nan #will be distance between the detection and nearest station 
progressCounter = 0
for index, i in df_FIRMS.iterrows():
    tmp = dfStation #creates a temp station df
    tmp['latcompare'] = i['latitude'] #enters the detection point lat in temp df
    tmp['longcompare'] = i['longitude'] #enters the detection point lat in temp df
    tmp['CalcDist'] = tmp.apply(lambda row: CalcDistDF(row), axis=1) #creates a column of distance between all stations and this point
    tmp = tmp.sort_values(by='CalcDist') #sorts the calculated distances with closest on top 
    topStation = tmp.iloc[0] #gets the top station 
    df_FIRMS.at[index, 'nearestStation'] = topStation['Station Name'] #populates the nearest station name 
    df_FIRMS.at[index, 'StationDist'] = topStation['CalcDist'] #populates how far away the station is
    if progressCounter%10000 == 0:
        print('on line: ' + str(progressCounter)) #print the progress every 10000
    progressCounter = progressCounter + 1

#the nearest station is now in the FIRMS dataframe 
#now merge the SCAN datframe by station and date to the FIRMS dataframe 
print('Merging USDA data into the FIRMS dataframe by date and location...')
df_FIRMS = df_FIRMS.merge(df_usda, left_on=['nearestStation', 'newdate'], right_on=['Station Name', 'date_new'], how='left') 

"""
Convert results to csv
"""
print('Converting to csv...')
df_FIRMS.to_csv('FIRMSandSCANFull2018toApr2022.csv') 


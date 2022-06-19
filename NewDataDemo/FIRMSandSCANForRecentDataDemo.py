"""
Wrangling of more recent data model running during presenation

Creation of a full table without WFIGS for testing the model on new data. 

The FIRMS export was extracted from the FIRMS website:
https://firms.modaps.eosdis.nasa.gov/download/ 
    
USDA SCAN network Data was extracted from the USDA website. Report generator link below. 
https://wcc.sc.egov.usda.gov/reportGenerator/

"""


#general imports
import pandas as pd
import boto3
import geopy.distance  
import numpy as np


df_usda = pd.read_csv('USDAExportApril1ToJune8.csv')
df_MODIS = pd.read_csv('fire_nrt_M-C61_275397.csv')
df_VIIRS1=pd.read_csv('fire_nrt_J1V-C2_275398.csv')
df_VIIRS2=pd.read_csv('fire_nrt_SV-C2_275399.csv')

df_FIRMS = pd.concat([df_MODIS, df_VIIRS1, df_VIIRS2])

#Start by converting FIRMS to a mergable dataframe
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
dfTEST_FIRMS = df_FIRMS.iloc[0:10000]
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

def GetHour(acq_time):
    """
    gets simply the hour from the given military time
    """
    aqtime = str(acq_time)
    size = len(aqtime)
    hr = aqtime[:size - 2] #get the hours
    #add the 0 if we need it to match the fire db
    if len(hr) == 1:
        hr = '0' + hr
    elif len(hr) == 0: 
        hr = '00' #its a 00 hour 
    return hr


def MilitaryConvert(MilInput):
    """
    converts the given military time from the satellite to hh:mm
    """
    Mil = str(MilInput)
    size = len(Mil)
    minutes = Mil[-2:]  #get the minutes 
    hours = Mil[:size - 2] #get the hours 
    time = hours + ':' + minutes
    if len(time) == 4 : #if its a single hour, add a 0 so it can match with the fire df
        time = '0' + time 
    elif len(time) == 3: #if its a 2 digit military time 
        time = '00' + time 
    elif len(time) ==2: #if it was a single digit military time   
        time = time.replace(':', '') #removes the colon so its not something like ':2' 
        time = '00:0' + time    
    return time


#here create a universal date time column which can be comparable to other observations 
print('Preparing to add nearby detections...')
df_FIRMS['time'] = df_FIRMS['acq_time'].apply(MilitaryConvert) #get an accurate time column in HH:MM format
df_FIRMS['newdatetime'] = df_FIRMS['acq_date'] + ' ' + df_FIRMS['time'] #create a combined date time column, its a string at this point  
df_FIRMS['newdatetime'] = pd.to_datetime(df_FIRMS['newdatetime'], format = '%Y-%m-%d %H:%M') #converts the acq_date column to date time format
#declare the time it counts for a nearby detection
targetdelta_neg = pd.Timedelta(days=-1) #we will call it a match if one was detected more than once in a day in the area  
targetdelta_pos = pd.Timedelta(days=0) #get the timedelta for 0, because we want to see recent things not future things 
df_FIRMS['nearbydetections'] = np.nan #creates the column for number of recent occurences we will populate with the loop below
pd.options.mode.chained_assignment = None  # default='warn' , https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
tmp1 = df_FIRMS[['lat', 'long', 'newdatetime']] #creates a temp df identical to the main for filtering
print('Adding number of nearby detections...')
counter = 0
for index, row in df_FIRMS.iterrows(): 
    targetdatetime = row['newdatetime'] #the time delta
    targetlat = row['lat'] 
    targetlong= row['long']
    tmp2 = tmp1[(tmp1['newdatetime'] <= (targetdatetime+targetdelta_pos))&(tmp1['newdatetime'] >= (targetdatetime+targetdelta_neg))&(tmp1['lat'] >= targetlat-1) & (tmp1['lat'] <= targetlat+1) & (tmp1['long'] >= targetlong-1) & (tmp1['long'] <= targetlong+1)]
    numdetections = tmp2.shape[0] #number of rows left in the tmp df
    numdetections = numdetections - 1 #the target row was still in the tmp df, subtract one to account for it
    df_FIRMS.at[index, 'nearbydetections'] = numdetections #update the value
    counter = counter + 1
    if counter%10000==0:
        print('on line '+str(counter))

print('Cleaning some columns...')
df_FIRMS = df_FIRMS.drop('time', axis=1)
df_FIRMS = df_FIRMS.drop('newdatetime', axis=1)


print('Saving to csv...')
df_FIRMS.to_csv('April1ToJune8FullTable.csv', index=False)



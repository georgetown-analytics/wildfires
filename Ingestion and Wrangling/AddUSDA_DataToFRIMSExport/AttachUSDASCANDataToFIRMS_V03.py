"""
Script to combine a FIRMS export and a USDA weather station export 

This will provide insight to the conditions at each detection location including soil moisture, relative humidity, etc. 

FIRMS (Fire Information Resource Management System) exports of satellite fire dections can be optained in the following link:
https://firms.modaps.eosdis.nasa.gov/download/

USDA report exports of stations that track soil moisture, wind, humidity etc. can be found at the folowing link:
https://wcc.sc.egov.usda.gov/reportGenerator/
Note that the export comes with some headers at the top of the csv you have to manually remove first

A map of the stations can be found here:
https://www.nrcs.usda.gov/wps/portal/wcc/home/quicklinks/imap 

Using the python module 'geopy' for calculating distance between the detection and the USDA weather stations 
before running install the geopy module:
pip install geopy

You will want to change the file names and paths for merging the data with your own FIRMS and USDA exports

BA_
"""

import pandas as pd 
import numpy as np
import geopy.distance  

#import the 2 csvsand merge them. 
#FOR RUNNING THIS ON OWN: you will have to change the csv location/name 
dfa = pd.read_csv(r'C:\Users\anderb4\Documents\GeorgetownDSCert\WildfireCapstone\Data\Modis20to22\fire_archive_M-C61_258142.csv')
dfb = pd.read_csv(r'C:\Users\anderb4\Documents\GeorgetownDSCert\WildfireCapstone\Data\Modis20to22\fire_nrt_M-C61_258142.csv')
df = pd.concat([dfa, dfb]) #concatonate the data 

#get a rounded longitude and latitude  in the FIRMS df
df['lat'] = round(df['latitude'])
df['long'] = round(df['longitude'])
#DFB below is for testing delete before releasing full
dfb['lat'] = round(dfb['latitude'])
dfb['long'] = round(dfb['longitude'])

#now read the USDA exports 
#FOR DOING THIS ON OWN: Chan ge file name/path to your own USDA export
df_SCAN2020 = pd.read_csv('SCANSelectedData2020.csv')
df_SCAN2021 = pd.read_csv('SCANSelectedData2021.csv')
dfSCAN = pd.concat([df_SCAN2020, df_SCAN2021]) #concatonate the scan data

#get a dataframe only of each unique station 
StationDFCol = dfSCAN.columns[1:14] #list of columns relevant for the station 
dfStation = dfSCAN[StationDFCol] #get a dataframe of just the station info
dfStation = dfStation.drop_duplicates() #remove redundant rows so now just a new df of station info

#convert both the FIRMS and USDA to date time object
df['newdate'] = pd.to_datetime(df['acq_date'], format = '%Y-%m-%d') #converts the df to date time 
def ReplaceSlash(datein):
    dashdate = datein.replace('/', '-')
    return dashdate 
dfSCAN['date_new'] = dfSCAN['Date'].apply(ReplaceSlash) #converts the format
dfSCAN['date_new'] =  pd.to_datetime(dfSCAN['date_new'], format = '%m-%d-%Y') #converts the df to date time 
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

#attach nearest station to each detection 
df['nearestStation'] = '' #nearest station name 
df['StationDist'] = np.nan #will be distance between the detection and nearest station 
df['StationDateKey'] = '' #will serve as a key fpr station + day data 
dfTest2 = df.iloc[:1000] #CHANFGE TO FULL DF create a smaller test df
progressCounter = 0 
for index, i in df.iterrows():
    tmp = dfStation #creates a temp station df
    tmp['latcompare'] = i['latitude'] #enters the detection point lat in temp df
    tmp['longcompare'] = i['longitude'] #enters the detection point lat in temp df
    tmp['CalcDist'] = tmp.apply(lambda row: CalcDistDF(row), axis=1) #creates a column of distance between all stations and this point
    tmp = tmp.sort_values(by='CalcDist') #sorts the calculated distances with closest on top 
    topStation = tmp.iloc[0] #gets the top station 
    df.at[index, 'nearestStation'] = topStation['Station Name'] #populates the nearest station name 
    df.at[index, 'StationDist'] = topStation['CalcDist'] #populates how far away the station is
    df.at[index, 'StationDateKey'] = topStation['Station Name'] + ';' + i['acq_date'] #populates the station + date key 
    #print('on line: ' + str(progressCounter))
    #progressCounter = progressCounter + 1
    

#merge the data frames to get weather data 
dfSCAN['datestr'] = dfSCAN['date_new'].dt.strftime('%Y-%m-%d') #converts SCAN date to a similar string as main FIRMS df
dfSCAN['StationDatekey'] = dfSCAN['Station Name'] + ';' + dfSCAN['datestr'] #creates key for merging the dfs
df = df.merge(dfSCAN, how='left', left_on='StationDateKey', right_on='StationDatekey')

df.to_csv('FIRMSandSCAN20To21.csv')

"""
This ran successfully for me and output the csv above with the merged USDA data with each FIRMS detection

The merged data includes soil moisture, relative humidity, average wind speed, etc.  

"""


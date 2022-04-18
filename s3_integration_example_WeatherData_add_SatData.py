"""
About:
This python script is to convert a FIRMS csv export and add weather data to each point using the meteostat python library. 
Meteostat imports weather data including average temperature (C), min and max temperature(C), precitpation, wind speed and direction and more by date and coordinates

The Meteostat python library must be installed for this script to run. You can install using pip:
    
pip install meteostat

More info on the meteostat library can be found in the documentation here:
https://dev.meteostat.net/python/ 

It seems likle there are definetly missing data and certain locations, but a lot of values did populate.

You will likely want to change the name of the imported csv to whatever export you retirieved from the FIRMS website. For now, I uploaded a 2021 VIIRS export

CAUTION:, you may want to replace the 'dfSat' with 'dfSatTest' in the last 'for' loop if you only want to test it out on the top 100 rows

BA_
"""

import pandas as pd 
import numpy as np
#import and read the csv

url = 'https://gtown-wildfire-ds.s3.amazonaws.com/modis_2018_United_States.csv?AWSAccessKeyId=AKIAQXCR5B4BWJXK4HEH&Signature=q9wOTZzqC9EKo58QhjVbuLVC4N8%3D&Expires=1650922456'

dfSat = pd.read_csv(url) #change this to the FIRMS csv download file name you want

#round the lat and long to align with project
dfSat['latitude_rounded'] = round(dfSat['latitude'], 2)
dfSat['longitude_rounded'] = round(dfSat['longitude'], 2)

#split up the aqc date into year, month, day
dfSat['acq_date'] = dfSat['acq_date'].astype('str').str.split(pat = '-')

##seperating out year month day into seperate columns 
dfSat['year'] = pd.DataFrame(dfSat.acq_date.tolist())[0]
dfSat['month'] = pd.DataFrame(dfSat.acq_date.tolist())[1]
dfSat['day'] = pd.DataFrame(dfSat.acq_date.tolist())[2]

#now ge the weather data based on coordinates and date  
#start by creating the columns i want filled out related to the meteostat output
import numpy as np
dfSat['tavg'] = np.nan
dfSat['tmin'] = np.nan
dfSat['tmax'] = np.nan
dfSat['prcp'] = np.nan
dfSat['snow'] = np.nan
dfSat['wdir'] = np.nan
dfSat['wspd'] = np.nan
dfSat['wpgt'] = np.nan
dfSat['pres'] = np.nan
dfSat['tsun'] = np.nan

#create a function to get the weather 
#again, documemntation from this library: https://dev.meteostat.net/python/
from datetime import datetime
import matplotlib.pyplot as plt
from meteostat import Point, Daily

#the function below gets the weather data for a specific coordinate and date
#it returns a list of [0]tavg, [1]tmin, [2]tmax, [3]prcp, [4]snow, [5]wdir, [6]wspd, [7]wpgt, [8]pres, [9]tsun
def GetWeather(lat, lon, year, month, day):
    start = datetime(year, month, day)
    end = datetime(year, month, day)
    coord = Point(lat, lon)
    data = Daily(coord, start, end)
    data = data.fetch()
    #print(data)
    #print(data.iloc[0, 0])
    #get a list of results
    counter = list(range(0,10)) #list 0-9
    WeatherData = [] #this list serves as a list for data values [0]tavg, [1]tmin, [2]tmax, [3]prcp, [4]snow, [5]wdir, [6]wspd, [7]wpgt, [8]pres, [9]tsun
    try:
        for i in counter:
            WeatherData.append(data.iloc[0, i])
    except:
        WeatherData = [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]    
    print(data)
    return(WeatherData)

#print(GetWeather(39.42, -77.42, 2020, 6, 6)[0]) #this was a test to get wether near my house in frederick, MD on 1/6/2020
#function seems to be working well. 
#the API output doesnt always have all values, but it seems to at least have temp and wind data so that is good

#now populate the weather data for each FIRMS observation 
rowcount = 0 #this will serve as a counter for us to see the progress, if we want 
dfSatTest = dfSat.iloc[:100] #created a small test df if you want to use it
#WARNING: if you just want to test this out I recommend replacing "dfSat" with "dfSatTest" in the 'for' loop below to only run the script on the first 100 rows of the export
for index, row in dfSat.iterrows():
    print("Currently on index " + str(rowcount) + ". Weather result is:") #this tracks progress as the script runs, comment it out if you want
    latx = row['latitude_rounded']
    longx = row['longitude_rounded']
    yrx = int(row['year'])
    monthx = int(row['month'])
    dayx = int(row['day'])
    WeatherList = GetWeather(latx, longx, yrx, monthx, dayx)
    #list contents: [0]tavg, [1]tmin, [2]tmax, [3]prcp, [4]snow, [5]wdir, [6]wspd, [7]wpgt, [8]pres, [9]tsun
    dfSat.at[index, 'tavg'] = WeatherList[0]
    dfSat.at[index, 'tmin'] = WeatherList[1]
    dfSat.at[index, 'tmax'] = WeatherList[2]
    dfSat.at[index, 'prcp'] = WeatherList[3]
    dfSat.at[index, 'snow'] = WeatherList[4]
    dfSat.at[index, 'wdir'] = WeatherList[5]
    dfSat.at[index, 'wspd'] = WeatherList[6]
    dfSat.at[index, 'wpgt'] = WeatherList[7]
    dfSat.at[index, 'pres'] = WeatherList[8]
    dfSat.at[index, 'tsun'] = WeatherList[9]
    rowcount = rowcount + 1 
    



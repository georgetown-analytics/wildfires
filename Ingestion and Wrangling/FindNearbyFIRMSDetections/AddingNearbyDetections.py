"""
This is the code for adding the number of nearby recent detections to a FIRMS export 

This is calculated by counting the number of other detections at the same rounded lat and long that were within a certain time (one day) before the detection 

We can play with the the range of what we want to call 'nearby' and what we want to call recent for next steps of this

If you want to run the code yourself on a FIRMS satellite export, you will have to change the csv file names

BA_
"""

import pandas as pd 
import numpy as np

#import the 2 csvsand merge them 
#for running this on your own you will have to change the csv location/name 
dfa = pd.read_csv(r'C:\Users\anderb4\Documents\GeorgetownDSCert\WildfireCapstone\Data\Modis20to22\fire_archive_M-C61_258142.csv')
dfb = pd.read_csv(r'C:\Users\anderb4\Documents\GeorgetownDSCert\WildfireCapstone\Data\Modis20to22\fire_nrt_M-C61_258142.csv')
df = pd.concat([dfa, dfb]) #merge the csvs

#get a rounded longitude and latitude 
df['lat'] = round(df['latitude'])
df['long'] = round(df['longitude'])
#DFB below is for testing delete before releasing full
dfb['lat'] = round(dfb['latitude'])
dfb['long'] = round(dfb['longitude'])

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
df['hour'] = df['acq_time'].apply(GetHour) #make the exclusive hour column
dfb['hour'] = dfb['acq_time'].apply(GetHour) #TEST DF make the exclusive hour column
df['time'] = df['acq_time'].apply(MilitaryConvert) #get an accurate time column in HH:MM format
dfb['time'] = dfb['acq_time'].apply(MilitaryConvert) #TEST DF get an accurate time column in HH:MM format
df['newdatetime'] = df['acq_date'] + ' ' + df['time'] #create a combined date time column, its a string at this point  
dfb['newdatetime'] = dfb['acq_date'] + ' ' + dfb['time'] #TEST DF create a combined date time column, its a string at this point  
df['newdatetime'] = pd.to_datetime(df['newdatetime'], format = '%Y-%m-%d %H:%M') #converts the acq_date column to date time format
dfb['newdatetime'] = pd.to_datetime(dfb['newdatetime'], format = '%Y-%m-%d %H:%M') #TEST DF converts the acq_date column to date time format

#now filter by things within a recent time (-1 day) and same rounded lat long 
#note that one latitude degree is ~69 miles so .01 latitude deg is ~0.69 miles
#also note one longitude degree is ~54.6 miles so .01 longitude degree is about ~0.546 miles
targetdelta_neg = pd.Timedelta(days=-1) #we will call it a match if one was detected more than once in a day in the area  
targetdelta_pos = pd.Timedelta(days=0) #get the timedelta for 0, because we want to see recent things not future things 
dfb['nearbydetections'] = np.nan #creates the column for number of recent occurences we will populate with the loop below
for index, row in dfb.iterrows():
    tmp = dfb[['lat', 'long', 'newdatetime']] #creates a temp df identical to the main for filtering 
    targetdatetime = row['newdatetime'] #identifies the datetime of the row 
    tmp['delta'] = tmp['newdatetime'] - targetdatetime #creates a new column of the difference between time 
    mask = tmp['delta'] <= targetdelta_pos #create a mask to filter out future observations  
    tmp = tmp[mask] #applies the mask for filtering 
    mask = tmp['delta'] >= targetdelta_neg #filters to differences greater than our negative delta
    tmp = tmp[mask] #tmp is now only occurences -24 hours from the target 
    targetlat = row['lat'] 
    targetlong= row['long']
    tmp = tmp[(tmp['lat'] == targetlat) & (tmp['long'] == targetlong)] #filters the temp df to match the targets lat and long
    numdetections = tmp.shape[0] #number of rows left in the tmp df
    numdetections = numdetections - 1 #the target row was still in the tmp df, subtract one to account for it
    dfb.at[index, 'nearbydetections'] = numdetections #update the value



"""
The dataframe now has a new field 'nearbydetections' 

This field counts how many other detections were within just over a half mile of the detection within the past day

Again as next steps we can play with the range and how far back we want a detection to look in the future 

"""




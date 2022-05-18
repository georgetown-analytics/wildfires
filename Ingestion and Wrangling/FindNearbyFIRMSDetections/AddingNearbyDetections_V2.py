"""
Version 2 of the adding nearby detections code 

Adding nearby detections second version for adding nearby detections to the large FIRMS+SCAN table FIRMSandSCANFull2018toApr2022.csv

This is calculated by counting the number of other detections at the same rounded lat and long that were within a certain time (one day) before the detection 

FOR TEAM: for running on own, add the S3 bucket credentials where indicated

BA_
"""

import pandas as pd
import numpy as np 
import boto3  
from datetime import datetime #used to benchmark

#import the csv from the S3 bucket
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

#Pull in the firms and scan df
file = 'FIRMSandSCANFull2018toApr2022.csv'
df_FirmsAndScan = pull_data(S3_Key_id, S3_Secret_key, file)
df_FirmsAndScanTEST = df_FirmsAndScan.iloc[:100000] #a temproary test dataframe for doing code trials to start 

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
df_FirmsAndScan['time'] = df_FirmsAndScan['acq_time'].apply(MilitaryConvert) #get an accurate time column in HH:MM format
df_FirmsAndScan['newdatetime'] = df_FirmsAndScan['acq_date'] + ' ' + df_FirmsAndScan['time'] #create a combined date time column, its a string at this point  
df_FirmsAndScan['newdatetime'] = pd.to_datetime(df_FirmsAndScan['newdatetime'], format = '%Y-%m-%d %H:%M') #converts the acq_date column to date time format

#declare the time it counts for a nearby detection
targetdelta_neg = pd.Timedelta(days=-1) #we will call it a match if one was detected more than once in a day in the area  
targetdelta_pos = pd.Timedelta(days=0) #get the timedelta for 0, because we want to see recent things not future things 
df_FirmsAndScan['nearbydetections'] = np.nan #creates the column for number of recent occurences we will populate with the loop below
print('Starting the loop, time is: '+str(datetime.now()))
pd.options.mode.chained_assignment = None  # default='warn' , https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
tmp1 = df_FirmsAndScan[['lat', 'long', 'newdatetime']] #creates a temp df identical to the main for filtering
counter = 0
for index, row in df_FirmsAndScan.iterrows(): 
    targetdatetime = row['newdatetime'] #the time delta
    targetlat = row['lat'] 
    targetlong= row['long']
    tmp2 = tmp1[(tmp1['newdatetime'] <= (targetdatetime+targetdelta_pos))&(tmp1['newdatetime'] >= (targetdatetime+targetdelta_neg))&(tmp1['lat'] >= targetlat-1) & (tmp1['lat'] <= targetlat+1) & (tmp1['long'] >= targetlong-1) & (tmp1['long'] <= targetlong+1)]
    numdetections = tmp2.shape[0] #number of rows left in the tmp df
    numdetections = numdetections - 1 #the target row was still in the tmp df, subtract one to account for it
    df_FirmsAndScan.at[index, 'nearbydetections'] = numdetections #update the value
    counter = counter + 1
    if counter%100000==0:
        print(counter)
    


print('Loop ended, time is: '+str(datetime.now()))

print('Saving to csv...')
df_FirmsAndScan.to_csv('FIRMSandSCANFull2018toApr2022.csv')


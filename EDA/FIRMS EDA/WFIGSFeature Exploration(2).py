#!/usr/bin/env python
# coding: utf-8

# In[8]:


#import necessary packages
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import datetime as dt


# In[9]:


dfw = pd.read_csv("WFIGS_-_Wildland_Fire_Locations_Full_History.csv")
dfw = dfw[['FireDiscoveryDateTime', 'DailyAcres','DiscoveryAcres','InitialLatitude','InitialLongitude']]


# In[10]:


dfw.head()


# In[11]:


#Seperate 'FireDiscoveryDateTime' to Date, Year, and Time
dfw['Year'] = pd.to_datetime(dfw['FireDiscoveryDateTime']).dt.year
dfw['Date'] = pd.to_datetime(dfw['FireDiscoveryDateTime']).dt.date
dfw['Time'] = pd.to_datetime(dfw['FireDiscoveryDateTime']).dt.time


# In[12]:


#Now that we have seperated our fire discovery date & time data, we can delete the original 'FireDiscoveryDateTime' colummn
dfw.drop("FireDiscoveryDateTime", axis=1, inplace=True)


# In[13]:


#Seperate 'date' column into 'day', 'month', and 'year'
dfw['Date'] = pd.to_datetime(dfw['Date'])
dfw['year'] = dfw['Date'].dt.year
dfw['month'] = dfw['Date'].dt.month
dfw['day'] = dfw['Date'].dt.day


# In[14]:


#We can now drop the 'Date' and 'Year' columns
dfw.drop("Date", axis=1, inplace=True)


# In[15]:


dfw.drop("year", axis=1, inplace=True)


# In[16]:


dfw.rename(columns={"POOState":"State","POOCounty":"County"} ,inplace=True)


# In[17]:


dfw.head()


# In[18]:


#Create a correlation matrix for our dataframe
plt.figure(figsize = (10,10))
sns.heatmap(dfw.corr(), annot = True, cmap = 'viridis', linewidths = 0.5)


# In[19]:


dfw['DailyAcres'].describe()


# In[20]:


#Deleting rows with empty values from our dataframe
print('Original DataFrame:')
print(dfw)
print('\n')

# Default configuration drops rows having at least 1 missing value
print('DataFrame after dropping the rows having missing values:')
print(dfw.dropna())


# In[21]:


dfw.head()


# In[22]:


dfw.shape


# In[23]:


#Rename the new data frame after removing rows with empty values
dfw1 = dfw.dropna()


# In[24]:


dfw1.shape


# In[25]:


#Create a new correlation matrix for our new dataframe
plt.figure(figsize = (10,10))
sns.heatmap(dfw1.corr(), annot = True, cmap = 'viridis', linewidths = 0.5)


# In[27]:


dfw1.head()


# In[28]:


#Identify quantiles for 'Daily Acres' column
acres_burnt = list(dfw1['DailyAcres'])
quantiles = list(np.arange(0, 1.01, 0.01))
values = list(np.quantile(acres_burnt, quantiles))

print('Distribution of the acres burnt:\n')
for quantile, value in zip(quantiles, values):
    print(f'Quantile {int(quantile*100)}%: {int(value)}\n')


# In[29]:


#Import pandas-profiling to further analyze our features
#pip install pandas-profiling
from pandas_profiling import ProfileReport


# In[30]:


#Create a profile for the original dataframe
profile = ProfileReport(dfw, title="Pandas Profiling Report", explorative=True)
profile


# In[31]:


#Create a pandas profile for our modified dataframe
profile = ProfileReport(dfw1, title="Pandas Profiling Report", explorative=True)
profile


# In[ ]:





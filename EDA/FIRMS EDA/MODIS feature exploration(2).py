#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import necessary packages
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import datetime as dt
path = 'Modis20to22archive.csv'


# In[2]:


dfSat = pd.read_csv(path)


# In[3]:


dfSat.head()


# In[4]:


dfSat.describe()


# In[5]:


#Check for any missing values in the dataframe
print(list(dfSat.isnull().any()))


# In[6]:


#Drop unnecessary columns: in this case, we will drop 'Version', 'Track', 'Scan', 'Instrument'
dfSat.drop("track", axis=1, inplace=True)


# In[7]:


dfSat.drop("scan", axis=1, inplace=True)


# In[8]:


dfSat.drop("instrument", axis=1, inplace=True)


# In[9]:


#Examinging the 'Type' column
dfSat['type'].value_counts()


# In[10]:


#Let's seperate our 'Type' column into seperate columns for each respective type
types = pd.get_dummies(dfSat['type'])
dfSat = pd.concat([dfSat, types], axis=1)


# In[11]:


dfSat.head()


# In[12]:


#Now that we have each type in its own column, lets drop the type cell 
dfSat.drop("type", axis=1, inplace=True)


# In[13]:


#Create correlation matrix for MODIS data
plt.figure(figsize = (10,10))
sns.heatmap(dfSat.corr(), annot = True, cmap = 'viridis', linewidths = 0.5)


# In[14]:


#Converting our 2 categorical value columns 'daynight', 'satellite' into **boolean values and replacing the old values in the dataframe
daynight_map = {"D": 1, "N": 0}
satellite_map = {"Terra": 1, "Aqua": 0}


# In[15]:


dfSat['daynight'] = dfSat['daynight'].map(daynight_map)
dfSat['satellite'] = dfSat['satellite'].map(satellite_map)


# In[16]:


#Seperate acq_date column into 'day', 'month', and 'year'
dfSat['acq_date'] = pd.to_datetime(dfSat['acq_date'])
dfSat['year'] = dfSat['acq_date'].dt.year
dfSat['month'] = dfSat['acq_date'].dt.month
dfSat['day'] = dfSat['acq_date'].dt.day


# In[17]:


#Now that we have seperated our 'acq_date' data into day, month, and year columns, we can drop the 'acq_date' column to eliminate redundancy
dfSat.drop("acq_date", axis=1, inplace=True)


# In[18]:


dfSat.head()


# In[19]:


#For the purposes of this project, we can eliminate the 'year column'
dfSat.drop("year", axis=1, inplace=True)


# In[20]:


#Create a new correlation matrix with our modified dataframe
plt.figure(figsize = (10,10))
sns.heatmap(dfSat.corr(), annot = True, cmap = 'viridis', linewidths = 0.5)


# In[21]:


#We can also drop the 'Version' column
dfSat.drop("version", axis=1, inplace=True)


# In[22]:


plt.figure(figsize = (10,10))
sns.heatmap(dfSat.corr(), annot = True, cmap = 'viridis', linewidths = 0.5)


# In[23]:


#Analyze features using Sklearn's ExtraTreesClassifier
from sklearn.ensemble import ExtraTreesClassifier


# In[24]:


array = dfSat.values
X = array[:,0:13]
Y = array[:,13]
model = ExtraTreesClassifier(n_estimators = 100)
model.fit(X, Y)
print(model.feature_importances_)


# In[25]:


#The above scores suggest that the features that may be important are (from most to least):
'month', 'day', 'longitude', 'latitude'


# In[ ]:





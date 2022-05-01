#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import necessary packages
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px


# In[2]:


dfv = pd.read_csv("viirs-snpp_2021_United_States.csv")
dfv.head()


# In[3]:


dfv.describe()


# In[4]:


#Seperate acq_date column into 'day', 'month', and 'year'
dfv['acq_date'] = pd.to_datetime(dfv['acq_date'])
dfv['year'] = dfv['acq_date'].dt.year
dfv['month'] = dfv['acq_date'].dt.month
dfv['day'] = dfv['acq_date'].dt.day


# In[5]:


#Now that we have seperated our 'acq_date' data into day, month, and year columns, we can drop the 'acq_date' column to eliminate redundancy
dfv.drop("acq_date", axis=1, inplace=True)


# In[6]:


#We can further drop the 'instrument' and 'version' columns
dfv.drop("instrument", axis=1, inplace=True)


# In[7]:


dfv.drop("version", axis=1, inplace=True)


# In[8]:


#Since all fire entries are from 2021, we do not need the 'year' column
dfv.drop("year", axis=1, inplace=True)


# In[9]:


dfv.head()


# In[10]:


#Checking for any null values in our dataframe - There are no null values present
dfv.isnull().sum()


# In[11]:


#Examinging the 'Type' column
dfv['type'].value_counts()


# In[12]:


#Identifying the number of 'day' fire incidents vs. 'night' fire incidents
dfv['daynight'].value_counts()


# In[97]:


dfv['daynight'].hist()


# In[13]:


#Histogram plot of 'bright_ti4'
#bright_ti4 refers to VIIRS I-4 channel brightness temperature of the fire pixel measured in Kelvin.
dfv['bright_ti4'].hist()


# In[14]:


#Histogram plot of 'bright_ti5'
#bright_ti5 refers to VIIRS I-5 channel brightness temperature of the fire pixel measured in Kelvin.
dfv['bright_ti5'].hist()


# In[15]:


#As we can see, the months during which the highest number of fires occur are July, August, and September
sns.histplot(data=dfv, x="month") 
                  


# In[17]:


#Histogram of 'frp'
#'frp' refers to the pixel-integrated fire radiative power in MW (megawatts)
sns.histplot(data=dfv, x="frp", bins=100)


# In[18]:


#Interactive map of VIIRS-detected fires
import plotly.express as px
fig = px.scatter_mapbox(dfv, lat="latitude", lon="longitude", color="type")
fig.update_layout(mapbox_style="open-street-map")
fig.show()


# In[19]:


#Create a correlation matrix to examine the features of our modified dataframe
plt.figure(figsize = (10,10))
sns.heatmap(dfv.corr(), annot = True, cmap = 'viridis', linewidths = 0.5)


# In[ ]:





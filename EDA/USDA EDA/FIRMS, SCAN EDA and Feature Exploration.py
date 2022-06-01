#!/usr/bin/env python
# coding: utf-8

# In[1]:


##Exploratory data analysis (EDA) and feature exploration on the merged SCAN (Soil Climate Analysis Network) & FIRMS (Fire Information for Research Management System) datasets


# In[2]:


#import necessary packages 
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import datetime as dt
path = 'FIRMSandSCAN20To21_Try3.csv'


# In[3]:


df = pd.read_csv(path)


# In[4]:


df.head()


# In[5]:


#Checking for null values present in the dataframe
df.isnull().sum()


# In[6]:


#Converting daynight values to numerical values
daynight_map = {"D": 1, "N": 0}
satellite_map = {"Terra": 1, "Aqua": 0}


# In[7]:


df['daynight'] = df['daynight'].map(daynight_map)
df['satellite'] = df['satellite'].map(satellite_map)


# In[8]:


#Seperate acq_date column into 'day', 'month', and 'year'
df['acq_date'] = pd.to_datetime(df['acq_date'])
df['year'] = df['acq_date'].dt.year
df['month'] = df['acq_date'].dt.month
df['day'] = df['acq_date'].dt.day


# In[9]:


#Now that we have seperated our 'acq_date' data into day, month, and year columns, we can drop the 'acq_date' column to eliminate redundancy
df.drop("acq_date", axis=1, inplace=True)


# In[10]:


#Dropping 'instrument' column, as it is not revelant to our research 
df.drop("instrument", axis=1, inplace=True)


# In[11]:


#Identifying prevalence of fires by state
df['State Code'].value_counts().sort_index(ascending=False).sort_values(ascending=False) 


# In[12]:


#Create a bar graph to better visualize prevalence of fires per region in our full dataframe
df = df
plt.figure(figsize=(30,30))
sns.countplot(df['HUC2 Name'])
plt.xticks(rotation=90)
plt.grid(True)


# In[13]:


#Create a bar graph to better visualize prevalence of fires per County in our full dataframe
df = df
plt.figure(figsize=(30,30))
sns.countplot(df['County Name'])
plt.xticks(rotation=90)
plt.grid(True)


# In[14]:


#Creating a subset, df_top, of the dataframe that contains only fire incidents from the 7 states with the largest numbers of fires
df_top = df.loc[df['State Code'].isin(['CA', 'WA', 'OR', 'TX', 'AL', 'KS', 'FL'])]


# In[15]:


#Distribution of fires per state in our dataframe subset, df_top
df_top['State Code'].hist()


# In[16]:


df_top.shape


# In[17]:


#Create a bar graph to better visualize the number of fires per county in our 'df_top' data frame
df_top = df_top
plt.figure(figsize=(30,30))
sns.countplot(df_top['County Name'])
plt.xticks(rotation=90)
plt.grid(True)


# In[18]:


#Interactive map of fire incidents in the df_top dataframe, with dot colors indicating average wind speed in the area 
import plotly.express as px
fig = px.scatter_mapbox(df_top, lat="latitude", lon="longitude", color="Wind Speed Average (mph)")
fig.update_layout(mapbox_style="open-street-map")
fig.show()


# In[20]:


#Interactive map of fire incidents in the df_top dataframe, with dot colors indicating eleveation in the area 
fig = px.scatter_mapbox(df_top, lat="latitude", lon="longitude", color="Elevation (ft)")
fig.update_layout(mapbox_style="open-street-map")
fig.show()


# In[21]:


#Create a new subset of our dataframe to explore feature correlation - Features were chosen due to both relevance and completeness
df_corr_sample = df[['latitude','longitude', 'brightness', 'confidence', 'bright_t31', 'daynight', 'type', 'Elevation (ft)', 'Snow Depth (in) Start of Day Values', 'month', 'day', 'Precipitation Accumulation (in) Start of Day Values', 'Precipitation Increment (in)', 'Wind Speed Average (mph)', 'Relative Humidity Enclosure (pct)', 'Soil Moisture Percent -2in (pct) Start of Day Values', 'Soil Moisture Percent -4in (pct) Start of Day Values']]
            


# In[22]:


#Create a correlation matrix for our new dataframe subset
plt.figure(figsize = (10,10))
sns.heatmap(df_corr_sample.corr(), annot = True, cmap = 'viridis', linewidths = 0.5)


# In[23]:


#Create histograms of the distribution of relative humidity, seperated by region
ax = df.hist(column='Relative Humidity Enclosure (pct)', by='HUC2 Name', bins=25, figsize=(16,18), grid=False, color='pink')
for x in ax.flatten():
    x.set_xlabel("Relative Humidity Distribution by Region")
    x.set_ylabel("Observation Count")


# In[24]:


#Create histograms of the distribution of average wind speed, seperated by region
ax = df.hist(column='Wind Speed Average (mph)', by='HUC2 Name', bins=25, figsize=(16,18), grid=False, color='purple')
for x in ax.flatten():
    x.set_xlabel("Average Wind Speed Distribution per Region")
    x.set_ylabel("Count")


# In[25]:


#Checking for correlation between Soil Moisture (-2 in) and Precipitation Accumulation (in) per regsion
grid = sns.FacetGrid(df, col = "HUC2 Name", hue ="HUC2 Name", col_wrap=5)
grid.map(sns.scatterplot, "Soil Moisture Percent -2in (pct) Start of Day Values", "Precipitation Accumulation (in) Start of Day Values")

grid.add_legend()

plt.show()


# In[26]:


#Checking for correlation between Soil Moisture (-2 in) and Precipitation Accumulation (in) per county
grid = sns.FacetGrid(df, col = "County Name", hue ="County Name", col_wrap=5)
grid.map(sns.scatterplot, "Soil Moisture Percent -2in (pct) Start of Day Values", "Precipitation Accumulation (in) Start of Day Values")

grid.add_legend()

plt.show()


# In[ ]:





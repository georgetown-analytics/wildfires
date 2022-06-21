# wildfires
Cohort 26 Capstone Project for the Certificate of Data Science at Georgetown University School of Continuing Studies.

## Overview

Wildfires have become a growing concern in American society over the past few years, especially with increasing climate change. Using machine learning to accurately predict when wildfires are likely to occur can assist response agencies in more efficiently allocating their resources to minimize damage caused by these wildfires. The team researched different data sources of actual recorded wildfires, fire monitoring systems, as well as relevant data for conditions in which wildfires are likely to start.
The team found data sources of both past wildfire occurrences as well as potential fire start data through satellite detections. 

![alt text](https://github.com/georgetown-analytics/wildfires/blob/main/figures/ProjectPipeline.PNG)

## Data Sources 

#### FIRMS: NASA Fire Information for Resource Management System 
A system of satellite data which detects potential fires around the world. A significant portion of FIRMS satellite detections are false alarms or do not evolve into major fires. The team seeks to predict which satellite detections lead to major fires.

#### USDA SCAN: United States Department of Agriculture Soil Climate Analysis Network
Network of 208 USDA weather stations tracking relevant ground conditions including soil moisture, relative humidity, recent precipitation, etc.

#### WFIGS: Wildland Fire Interagency Geospatial Services 
Tracks historical fire data and treated as label feature for the data set in the machine learning project.

## Approach
Combining these features with more advanced metrics, the team used various machine learning techniques (ensemble, tree-based, neural networks, etc.) to try and predict the start of a wildfire. More than 3 years of data from January 2018 to May 2022 was used to train the models.

## Results
The best performing model was an extra trees classifier which had a precision of 0.904, recall of 0.581, and an F1 of 0.708 on the data frame which was randomly split into test and train data sets. The model was saved and exposed to new data and was able to predict several new fires on the day of the wildfire start between the months of April and June 2022. This was shown during the day of the project presentation and can be found in the “NewDataDemo” section of the repository.

Link to the demo:
[Demo](NewDataDemo/Testing Model Out With More Recent Data Full Apr1-June8.ipynb)

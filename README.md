# wildfires
Cohort 26 Capstone Project for the Certificate of Data Science at Georgetown University School of Continuing Studies.

## Overview

Wildfires have become a growing concern in American society over the past few years, especially with increasing climate change. The consequences of wildfires are often far-reaching. Not only do such fires lead to catastrophic damage to wildlife in a community, but also can cause significant economic loss, and even loss of life for those residing in affected areas. Using Machine Learning to accurately predict when wildfires are likely to occur can assist response agencies in more efficiently allocating their resources to minimize damage caused by these wildfires. The team researched different data sources of actual recorded wildfires, fire monitoring systems, as well as relevant data for conditions in which wildfires are likely to start.

The team found data sources of both past wildfire occurrences as well as potential fire start data through satellite detections. NASA Fire Information for Resource Management System (FIRMS) is a system of satellite data which detects potential fires around the world. A significant portion of FIRMS satellite detections are false alarms or do not lead to major fires. The project seeks to predict wildfires based on certain starting factors, specifically satellite detections from NASA FIRMS merged with ground and weather conditions from the USDA. 

![Rank by feature](figures/ProjectPipeline.png)

## Approach 

The team gathered data to serve as the label of a wildfire start from the WFIGS (Wildland Fire Interagency Geospatial Services), which tracks historical fire data. 

Combining these features with more advanced metrics, the team used various machine learning techniques (ensemble, tree-based, neural networks, etc.) to try and predict the start of a wildfire. More than 3 years of data from January 2018 to May 2022 was used to train the models.

The best performing model was an extra trees classifier which had a precision of 0.904, recall of 0.581, and an F1 of 0.708 on the data frame which was randomly split into test and train data sets. The model was saved and exposed to new data and was able to predict several new fires on the day of the wildfire start between the months of April and June 2022 during the project presentation. 

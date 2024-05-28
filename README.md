# Renewable Energy Forecast project

## Overview
I am very concerned about the questions of climate changes and ecological transition. Developing renewable sources of energy for electricity production is one of the main way to reduce our carbon footprint.  
But the energy sector faces many challenges when it comes to develop renewable energy sources, such as the intermittency of the electricity production by wind or solar powerplants due to variable weather conditions.  
This intermittency can cause issue when the renewable electricity production is high, but the demand of consumers is low.  
This is why forecasting the energy production of renewable powerplants is essential. For that, machine learning tools shuch as time series forecasting tools are very useful.  

## Goal of the project
My main goal for this project is to achieve time serie forecasting for a renewable energy powerplant.  
I will focus on predicting the electricity production of wind powerplant in France, thanks to the electricity network manager API.  
The project is also an opportunity to develop my skills at data creating a data project from ground up :  
- Data sourcing
- Data storage and retreival
- Preprocessing
- Training

## Data used
The data used is the electricity production hour by hour from the electricity [network manager API in France (RTE)](https://data.rte-france.com/catalog/-/api/generation/Actual-Generation/v1.1#)  

## The project structure
The project is divided in four parts :
- [data](https://github.com/DridrM/renewable-energy-forecast/tree/master/re_forecast/data) : Manage the connexion to the API, the data storage and retreival
- [preprocessing](https://github.com/DridrM/renewable-energy-forecast/tree/master/re_forecast/preprocessing) : Handle datetime indexing, outliers and missing values
- [exploration](https://github.com/DridrM/renewable-energy-forecast/tree/master/re_forecast/exploration) : Some plots and statistics mainly to help the preprocessing step
- training : train several machine learning algorithm for time serie forecasting. Still under developpment.


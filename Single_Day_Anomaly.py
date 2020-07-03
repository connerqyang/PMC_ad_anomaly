#Import necessary libraries
import pandas as pd
import numpy as np
import os
# If Google Drive is being used to store or access data
from google.colab import drive
drive.mount('/content/gdrive')

# Set paths
DATASET_PATH = ''
PATH_TO_ADUNIT_FOLDER = ''
PATH_TO_OUTPUT_FOLDER = ''

#Read in data + drop unnecessary columns

df = pd.read_csv(DATASET_PATH, index_col=0)
df = df.drop(columns = ['impmean','impmed'])

#Create separate CSV file for each individual 'adunit'
websiteNames = df.adunit.unique()

for i in websiteNames:
  current_web_df = df[df.adunit == i]
  # Save_PATH = PATH_TO_ADUNIT_FOLDER
  SAVE_PATH = ''
  SAVE_PATH += str(i)
  SAVE_PATH += '.csv'
  current_web_df.to_csv(SAVE_PATH)

# Set Constants
NUM_SAME_WEEKDAY = 10 
Z_THRESHOLD = 1.96    

# List of unique adunits in dataframe df
# Can be modified to contain adunits of interest
websiteNames = df.adunit.unique()          

# Dictionary to keep track of counts
flagToCount = {
     "50%_spike": 0,
     "50%_drop": 0,
     "7day_spike": 0,
     "7day_drop": 0,
     "14day_spike": 0,
     "14day_drop": 0,
     "30day_spike": 0,
     "30day_drop": 0,
     "sig_spike": 0,
     "sig_drop": 0
}

# Iterate through each adunit
numAds = len(websiteNames)

for ii in range(numAds):
  # Create dataframe for current adunit
  curWeb = websiteNames[ii]
  SAVE_PATH = PATH_TO_ADUNIT_FOLDER
  SAVE_PATH += str(curWeb)
  SAVE_PATH += '.csv'
  dfWeb = pd.read_csv(SAVE_PATH, index_col=0)

  # Flag for 50% change
  dfWeb['50%_flag'] = 0

  # Section I
  numLines = len(dfWeb)            
  for i in range(numLines-1):
    impSum = 0.0                        
    numDaysToCheck = NUM_SAME_WEEKDAY 
    percentageChange = 0.0            
    for a in range(i, numLines):
      if numDaysToCheck < 1:
        break
      if (i+a) > numLines-1:
        break

      if dfWeb['weekday'].iloc[i] == dfWeb['weekday'].iloc[i+a]:
        impSum += dfWeb['dayimp'].iloc[i+a]
        numDaysToCheck = numDaysToCheck - 1

    # Section II
    if numDaysToCheck != NUM_SAME_WEEKDAY:
      if impSum != 0:
        # Calculate percentageChange normally
        meanImp = impSum / (NUM_SAME_WEEKDAY - numDaysToCheck)
        percentageChange = (dfWeb['dayimp'].iloc[i] - meanImp) / meanImp
      else:
        if dfWeb['dayimp'].iloc[i] == 0:
          percentageChange = .00
        elif dfWeb['dayimp'].iloc[i] > 0:
          percentageChange = .99      
        elif dfWeb['dayimp'].iloc[i] < 0:
          percentageChange = -.99
    else:
      percentageChange = .00

    # Section III
    if (percentageChange >= .50):
       dfWeb['50%_flag'].iloc[i] = 1
       flagToCount["50%_spike"] = flagToCount["50%_spike"] + 1
    if (percentageChange <= -.50):
       dfWeb['50%_flag'].iloc[i] = -1
       flagToCount["50%_drop"] = flagToCount["50%_drop"] + 1

  # Flag Z-Scores
  
  #Calculate daily revenue change
  dfWeb['delta_imp'] = 0  
  for i in range(numLines - 1):
    impressions_difference = dfWeb['dayimp'].iloc[i]
                                - dfWeb['dayimp'].iloc[i+1]
    dfWeb['delta_imp'].iloc[i] = impressions_difference
    
  dfWeb['30day_zScore'] = 0.0

  # Data frames per time period
  for i in range(numLines-31):
    df_30days = dfWeb[i:i+31]

    # Standard deviation calculation
    std_dev_30days = df_30days['delta_imp'].std(ddof=0)
    if std_dev_30days == 0:
      std_dev_30days = 0.01

    # Z-score calculation
    z_score = (dfWeb['delta_imp'].iloc[i]
                - df_30days['delta_imp'].mean())
                / std_dev_30days
    dfWeb['30day_zScore'].iloc[i] = z_score

  # Columns to flag z-score
  dfWeb['30day_flag'] = 0           

  #Flag values
  for i in range(numLines - 31):
    if float(dfWeb['30day_zScore'].iloc[i]) >= Z_THRESHOLD:
      dfWeb['30day_flag'].iloc[i] = 1
      flagToCount["30day_spike"] = flagToCount["30day_spike"] + 1
    if float(dfWeb['30day_zScore'].iloc[i]) <= -1 * Z_THRESHOLD:
      dfWeb['30day_flag'].iloc[i] = -1
      flagToCount["30day_drop"] = flagToCount["30day_drop"] + 1

  # Column to flag "significant anomalies
  dfWeb['significant_anomaly'] = 0      

  for i in range(numLines - 31):
    positive_z_flag = dfWeb['30day_flag'].iloc[i] == 1
    negative_z_flag = dfWeb['30day_flag'].iloc[i] == -1
    if positive_z_flag and dfWeb['50%_flag'].iloc[i] == 1:
      dfWeb['significant_anomaly'].iloc[i] = 1
      flagToCount["sig_spike"] = flagToCount["sig_spike"] + 1
    if negative_z_flag and dfWeb['50%_flag'].iloc[i] == -1:
      dfWeb['significant_anomaly'].iloc[i] = -1
      flagToCount["sig_drop"] = flagToCount["sig_drop"] + 1

  #Output  
  #Reset the index
  dfWeb.reset_index(inplace=True)
  dfWeb = dfWeb.drop(['index'], axis=1).copy()

  #5 output dataframes
  df_7day_flag = dfWeb[(dfWeb['7day_flag'] == 1) | (dfWeb['7day_flag'] == -1)]
  df_14day_flag = dfWeb[(dfWeb['14day_flag'] == 1) | (dfWeb['14day_flag'] == -1)]
  df_30day_flag = dfWeb[(dfWeb['30day_flag'] == 1) | (dfWeb['30day_flag'] == -1)]
  df_50_flag = dfWeb[(dfWeb['50%_flag'] == 1) | (dfWeb['50%_flag'] == -1)]
  df_sig_anom = dfWeb[(dfWeb['significant_anomaly'] == 1) | (dfWeb['significant_anomaly'] == -1)]

  #Save all output dataframes as .csv files
  SAVE_PATH_7_DAYS = PATH_TO_OUTPUT_FOLDER
  SAVE_PATH_7_DAYS += str(curWeb)
  SAVE_PATH_7_DAYS += '.7day_flag.csv'
  df_7day_flag.to_csv(SAVE_PATH_7_DAYS)

  SAVE_PATH_14_DAYS = PATH_TO_OUTPUT_FOLDER
  SAVE_PATH_14_DAYS += str(curWeb)
  SAVE_PATH_14_DAYS += '.14day_flag.csv'
  df_14day_flag.to_csv(SAVE_PATH_14_DAYS)

  SAVE_PATH_30_DAYS = PATH_TO_OUTPUT_FOLDER
  SAVE_PATH_30_DAYS += str(curWeb)
  SAVE_PATH_30_DAYS += '.30day_flag.csv'
  df_30day_flag.to_csv(SAVE_PATH_30_DAYS)

  SAVE_PATH_50_PERCENT = PATH_TO_OUTPUT_FOLDER
  SAVE_PATH_50_PERCENT += str(curWeb)
  SAVE_PATH_50_PERCENT += '.50percent_flag.csv'
  df_50_flag.to_csv(SAVE_PATH_50_PERCENT)

  SAVE_PATH_SIG_ANOM = PATH_TO_OUTPUT_FOLDER
  SAVE_PATH_SIG_ANOM += str(curWeb)
  SAVE_PATH_SIG_ANOM += '.significant_anomalies.csv'
  df_sig_anom.to_csv(SAVE_PATH_SIG_ANOM)

#Print out counts of flags
for key, value in flagToCount.items():
  print(key, ':', value, 'flags')

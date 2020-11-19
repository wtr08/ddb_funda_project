# Importing libraries via Anaconda
import os
from dotenv import load_dotenv
from pathlib import Path  # python3 only
import psycopg2 as psy
import pandas as pd
import numpy as np


path = os.path.abspath(__file__ + "/../../")

env_path = Path('.') / '.env'

load_dotenv(dotenv_path=env_path)

DB_username = os.getenv("DB_USER")
DB_password = os.getenv("DB_PASSWORD")


try: 
    # connect to the db
    connection = psy.connect(
        host = "localhost",
        user = DB_username,
        password = DB_password,
        database = "fundaDB"
    )
    print("Database connection succesfully")
except Exception as err:
    print("Error")
cursor = connection.cursor()

# Getting database results and convert them into dataframe
cursor.execute("SELECT global_id, postcode, koopprijs  FROM funda2018;")
fundadata=pd.DataFrame(cursor.fetchall(),columns=['global_id', 'postcode', 'koopprijs'])

cursor.execute("SELECT *  FROM postcode2018;")
postcode =pd.DataFrame(cursor.fetchall(),columns=['PC6', 'Buurt2018', 'Wijk2018', 'Gemeente2018'])

cursor.execute("SELECT *  FROM gemeentenaam2018;")
gemeente =pd.DataFrame(cursor.fetchall(),columns=['Gemcode', 'Gemeentenaam'])

cursor.execute("SELECT *  FROM buurtnaam2018;")
buurt=pd.DataFrame(cursor.fetchall(),columns=['Buurtcode', 'Buurtnaam'])

cursor.execute("SELECT *  FROM gemiddelde_verkoopprijzen;")
verkoop=pd.DataFrame(cursor.fetchall(),columns=['Gemeentenaam','2014','2015', '2016','2017', '2018'])

# What are the price trends, average municipality price from 2014 till 2018, and average price in per neigborhoud  2018? 

# Inner joins dataframes 
# [funda Global ID -> postcode PC6] 
funda_gemeente = pd.merge(fundadata, postcode, how='left', left_on='postcode', right_on='PC6')

# Inner join dataframes
# [Postcode Gemeent2018 -> Gemeente gemmcode]
postcode_gemeente = pd.merge(funda_gemeente, gemeente, how='left', left_on='Gemeente2018', right_on='Gemcode')

#inner join dataframes
#verkoopprijs
gemeente_verkoop = pd.merge(postcode_gemeente, verkoop, how='left', left_on='Gemeentenaam', right_on='Gemeentenaam')
gemeente_verkoop = gemeente_verkoop.dropna()

# Check gemeente dataframne
print(gemeente_verkoop)

# Avarage price per neighborhood
# Inner join funda_gemeente en buurt 
buurt_verkoop = pd.merge(funda_gemeente, buurt, how='left', left_on='Buurt2018', right_on='Buurtcode')
print(buurt_verkoop)

# inner join gemeente verkoop and buurt verkoop
funda_buurt_gemeente_verkoop = pd.merge(gemeente_verkoop, buurt_verkoop, how='left', left_on='global_id', right_on='global_id')
# Delete several columns to get a more clean dataframe
funda_buurt_gemeente_verkoop = funda_buurt_gemeente_verkoop.drop(['PC6_x', 'Buurt2018_x', 'Wijk2018_x', 'PC6_y', 'Buurt2018_y', 'Wijk2018_y', 'Gemeente2018_y', 'Buurtcode', 'Gemeente2018_x', 'Gemcode', 'postcode_y', 'koopprijs_y'], axis=1)

#Calculate the mean per buurt
Mean_buurt = buurt_verkoop.groupby(['Buurtnaam'])['koopprijs'].mean().reset_index()

# Merge mean buurt withg buurt gemeente with avargae house price per year 
funda_buurt_gemeente_verkoop_and_buurt_mean = pd.merge(funda_buurt_gemeente_verkoop, Mean_buurt, how='left', left_on='Buurtnaam', right_on='Buurtnaam')
print(funda_buurt_gemeente_verkoop_and_buurt_mean)

# Export to csv
funda_buurt_gemeente_verkoop_and_buurt_mean.to_csv("storage/query9.csv", sep=';', decimal=",")




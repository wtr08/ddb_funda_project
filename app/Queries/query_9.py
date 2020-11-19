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
# [funda Global ID -> postcode PC6] -> [Postcode Gemeent2018 -> Gemeente gemmcode] -> [gemeente2018  gemeentanaam -> CBS gemeentenaam] -> [CBS gemeentenaam -> verkoop gemeentenaam]

# [funda Global ID -> postcode PC6] 
funda_gemeente = pd.merge(fundadata, postcode, how='left', left_on='postcode', right_on='PC6')

# [Postcode Gemeent2018 -> Gemeente gemmcode]
postcode_gemeente = pd.merge(funda_gemeente, gemeente, how='left', left_on='Gemeente2018', right_on='Gemcode')

#verkoopprijs
gemeente_verkoop = pd.merge(postcode_gemeente, verkoop, how='left', left_on='Gemeentenaam', right_on='Gemeentenaam')
gemeente_verkoop = gemeente_verkoop.dropna()

# Result price per municipality
# print(gemeente_verkoop)

# Avarage price per neighborhood
# buurt
buurt_verkoop = pd.merge(funda_gemeente, buurt, how='left', left_on='Buurt2018', right_on='Buurtcode')
# print(buurt_verkoop)

Mean_buurt = buurt_verkoop.groupby(['Buurtnaam'])['koopprijs'].mean().reset_index()

Mean_buurt.to_csv("storage/query9.csv", sep=';', decimal=",")




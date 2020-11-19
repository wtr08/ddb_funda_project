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

## Query 1 
#read in the needed datasets
cursor.execute("SELECT *  FROM funda2018;")
fundadata=pd.DataFrame(cursor.fetchall(),columns=['global_id', 'publicatie_datum', 'postcode', 'koopprijs', 'volledige_omschrijving', 'soort_woning', 'bouwjaar', 'oppervlakte', 'datum_ondertekening'])

cursor.execute("SELECT *  FROM postcode2018;")
postcode =pd.DataFrame(cursor.fetchall(),columns=['PC6', 'Buurt2018', 'Wijk2018', 'Gemeente2018'])

cursor.execute("SELECT *  FROM gemeentenaam2018;")
gemeente =pd.DataFrame(cursor.fetchall(),columns=['Gemcode', 'Gemeentenaam'])

#left join postcode and gemeente datasets into dataset postcode_gemeente
postcode_gemeente = pd.merge(postcode, gemeente, how='left', left_on='Gemeente2018', right_on='Gemcode')
del postcode_gemeente['Gemeente2018']

#left join fundadata and postcode_gemeente datasets, into funda_gemeente
funda_gemeente = pd.merge(fundadata, postcode_gemeente, how='left', left_on='postcode', right_on='PC6')

#convert publicatie_datum column into datetime data type
funda_gemeente["publicatie_datum"]= pd.to_datetime(funda_gemeente["publicatie_datum"])
#sort month into column called month
funda_gemeente['month'] = funda_gemeente.publicatie_datum.dt.month

#calculated the house price mean per municipality with the corresponding month
mean_muni = funda_gemeente.groupby(['Gemeentenaam','month'])['koopprijs'].mean().reset_index()

#print
print(mean_muni.head(50))

#store in csv file
mean_muni.to_csv("storage/query1.csv", sep=';', decimal=",")



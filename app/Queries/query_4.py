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


cursor.execute("SELECT *  FROM funda2018;")
houses=pd.DataFrame(cursor.fetchall(),columns=['global_id', 'publicatie_datum', 'postcode', 'koopprijs', 'volledige_omschrijving', 'soort_woning', 'bouwjaar', 'oppervlakte', 'datum_ondertekening'])

cursor.execute("SELECT *  FROM postcode2018;")
codes =pd.DataFrame(cursor.fetchall(),columns=['PC6', 'Buurt2018', 'Wijk2018', 'Gemeente2018'])

cursor.execute("SELECT *  FROM gemeentenaam2018;")
muni =pd.DataFrame(cursor.fetchall(),columns=['Gemcode', 'Gemeentenaam'])

houses_codes = pd.merge(houses, codes, how='left', left_on='postcode', right_on='PC6')
houses_codes_muni = pd.merge(houses_codes, muni, how='left', left_on='Gemeente2018', right_on='Gemcode')

houses_codes_muni["publicatie_datum"]= pd.to_datetime(houses_codes_muni["publicatie_datum"])
houses_codes_muni['month'] = houses_codes_muni.publicatie_datum.dt.month

average_price_month = houses_codes_muni.groupby( ['Gemeentenaam','month'])['koopprijs'].mean().reset_index()

average_price_month['Relative_difference %'] = average_price_month.groupby(['Gemeentenaam'])['koopprijs'].pct_change()
average_price_month['Relative_difference %'] = average_price_month['Relative_difference %'] * 100

print(average_price_month.head())
average_price_month.to_csv("storage/query4.csv", sep=';')

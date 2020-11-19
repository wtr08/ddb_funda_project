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
fundadata=pd.DataFrame(cursor.fetchall(),columns=['global_id', 'publicatie_datum', 'postcode', 'koopprijs', 'volledige_omschrijving', 'soort_woning', 'bouwjaar', 'oppervlakte', 'datum_ondertekening'])

cursor.execute("SELECT *  FROM postcode2018;")
postcode =pd.DataFrame(cursor.fetchall(),columns=['PC6', 'Buurt2018', 'Wijk2018', 'Gemeente2018'])

cursor.execute("SELECT *  FROM gemeentenaam2018;")
gemeente =pd.DataFrame(cursor.fetchall(),columns=['Gemcode', 'Gemeentenaam'])

cursor.execute("SELECT *  FROM CBS_Municipality;")
cbs=pd.DataFrame(cursor.fetchall(),columns=['Wijken_en_buurten', 'Gemeentenaam', 'Soort_regio', 'Codering', 'Mannen', 'Vrouwen', '_0_tot_15', '_15_tot_25', '_25_tot_45', '_45_tot_65_jaar', '_65_jaar_of_ouder', 'Bevolkingsdichtheid','Gemiddeld_inkomen_per_inwoner' ])


postcode_gemeente = pd.merge(postcode, gemeente, how='left', left_on='Gemeente2018', right_on='Gemcode')
del postcode_gemeente['Gemeente2018']

funda_gemeente = pd.merge(fundadata, postcode_gemeente, how='left', left_on='postcode', right_on='PC6')

code = []
for i in cbs['Codering']:
    i = i[2:]
    while i[0] == "0":
        i = i[1:]
    code.append(i)
cbs['Code'] = code
cbs['Code'] = cbs['Code'].astype(int)

funda_gemeente.head()

income = cbs[['Gemeentenaam', 'Code', 'Gemiddeld_inkomen_per_inwoner']]

funda_gemeente_income= pd.merge(funda_gemeente, income, how='left', left_on='Gemcode', right_on='Code')

asking = funda_gemeente_income.groupby(['Gemiddeld_inkomen_per_inwoner','Gemeentenaam_x'])['koopprijs'].mean().reset_index()

asking_clean = asking.dropna().sort_values(by = ['Gemiddeld_inkomen_per_inwoner'], ascending = False)

print(asking_clean.head(20))
asking_clean.to_csv("storage/query3.csv", sep=';' , decimal=",")


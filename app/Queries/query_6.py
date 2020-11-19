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
cbs=pd.DataFrame(cursor.fetchall(),columns=['Wijken_en_buurten', 'Gemeentenaam', 'Soort_regio', 'Codering', 'Mannen', 'Vrouwen', '0tot15', '15tot25', '25tot45', '45tot65', '65jaarofouder', 'Bevolkingsdichtheid','Gemiddeld_inkomen_per_inwoner' ])


print(cbs)
code = []
for i in cbs['Codering']:
    i = i[2:]
    while i[0] == "0":
        i = i[1:]
    code.append(i)
cbs['Code'] = code
cbs['Code'] = cbs['Code'].astype(int)

cbs["0tot15"]= pd.to_numeric(cbs["0tot15"])
cbs["15tot25"]= pd.to_numeric(cbs["15tot25"])
cbs["25tot45"]= pd.to_numeric(cbs["25tot45"])
cbs["45tot65"]= pd.to_numeric(cbs["45tot65"])
cbs["65jaarofouder"]= pd.to_numeric(cbs["65jaarofouder"])

cbs['Total'] = cbs['15tot25'] + cbs['25tot45'] + cbs['45tot65'] + cbs['65jaarofouder']

cbs = cbs.drop(['Wijken_en_buurten', 'Soort_regio', 'Mannen', 'Vrouwen', 'Bevolkingsdichtheid', 'Gemiddeld_inkomen_per_inwoner'], axis=1)

cbs['15tot25%'] = (cbs['15tot25']/cbs['Total'])
cbs['25tot45%'] = (cbs['25tot45']/cbs['Total'])
cbs['45tot65%'] = (cbs['45tot65']/cbs['Total'])
cbs['65jaarofouder%'] = (cbs['65jaarofouder']/cbs['Total'])

postcode_gemeente = pd.merge(postcode, gemeente, how='left', left_on='Gemeente2018', right_on='Gemcode')
del postcode_gemeente['Gemeente2018']

funda_gemeente = pd.merge(fundadata, postcode_gemeente, how='left', left_on='postcode', right_on='PC6')

mean_muni = funda_gemeente.groupby(['Gemeentenaam', 'Gemcode'])['koopprijs'].mean().reset_index()

cbs_age_price = pd.merge(cbs, mean_muni, how='left', left_on='Code', right_on='Gemcode')

cbs_age_price = cbs_age_price.drop(['15tot25', '25tot45', '45tot65', '65jaarofouder'], axis=1)

cbs_age_price['15tot25.gemprice'] = (cbs_age_price['15tot25%']*cbs_age_price['koopprijs'])
cbs_age_price['25tot45.gemprice'] = (cbs_age_price['25tot45%']*cbs_age_price['koopprijs'])
cbs_age_price['45tot65.gemprice'] = (cbs_age_price['45tot65%']*cbs_age_price['koopprijs'])
cbs_age_price['65jaarofouder%.gemprice'] = (cbs_age_price['65jaarofouder%']*cbs_age_price['koopprijs'])

#people from 0-15 dont have a house
a = 'na'
#gem_prijs_15tot25
b = (cbs_age_price['15tot25.gemprice'].sum())/len(cbs_age_price)
# gem_prijs_25tot45
c = (cbs_age_price['25tot45.gemprice'].sum())/len(cbs_age_price)
# gem_prijs_45tot65 
d = (cbs_age_price['45tot65.gemprice'].sum())/len(cbs_age_price)
# gem_prijs_65jaarofouder 
e = (cbs_age_price['65jaarofouder%.gemprice'].sum())/len(cbs_age_price)

data = {'Age_group': ['0-15','15-25','25-45','45-65','65+'],
       'Average_price': [a, b, c, d, e]}
test = pd.DataFrame(data, columns = ['Age_group','Average_price'])
print(test)

test.to_csv("storage/query6.csv", sep=';' , decimal=",")

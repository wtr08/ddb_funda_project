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

cursor.execute("SELECT *  FROM buurtnaam2018;")
buurt=pd.DataFrame(cursor.fetchall(),columns=['Buurtcode', 'Buurtnaam' ])

cursor.execute("SELECT *  FROM wijk_facilities_buurt;")
wijk_facilities=pd.DataFrame(cursor.fetchall(),columns=['Wijken_en_buurten', 'Gemeentenaam', 'Soort_regio', 'codering', 'woningvoorraad', 'Percentage_bewoond','Percentage_onbewoond', 'Diefstal_uit_woning', 'Vernieling', 'Geweld', 'Afstand_tot_huisartsenpraktijk_km', 'Afstand_tot_grote_supermarkt_km', 'Afstand_tot_kinderdagverblijf_km', 'Afstand_tot_school_km'])

cursor.execute("SELECT *  FROM gemeente_afstand_facilities;")
gemeente_facilities=pd.DataFrame(cursor.fetchall(),columns=['Gemeentenaam','Afstand_tot_apotheek_km', 'Afstand_tot_ziekenhuis_km','Afstand_tot_levensmiddelen_km','Afstand_tot_cafe_km','Afstand_tot_restaurant_km','Afstand_tot_middelbareschool_km','Afstand_tot_oprit_hoofdverkeersweg_km','Afstand_tot_treinstations_km','Afstand_tot_podiumkunsten_km'])


postcode_gemeente = pd.merge(postcode, gemeente, how='left', left_on='Gemeente2018', right_on='Gemcode')
del postcode_gemeente['Gemeente2018']
postcode_buurt = pd.merge(postcode, buurt, how='left', left_on='Buurt2018', right_on='Buurtcode')
del postcode_buurt['Buurt2018']

funda_gemeente_code = pd.merge(fundadata, postcode_gemeente, how='left', left_on='postcode', right_on='PC6')
funda_buurt_code = pd.merge(fundadata, postcode_buurt, how='left', left_on='postcode', right_on='PC6')

funda_gemeente_code = funda_gemeente_code.drop(['publicatie_datum', 'koopprijs', 'volledige_omschrijving', 'soort_woning', 'bouwjaar', 'oppervlakte', 'datum_ondertekening', 'PC6', 'Wijk2018'], axis=1)
funda_buurt_code = funda_buurt_code.drop(['publicatie_datum', 'koopprijs', 'volledige_omschrijving', 'soort_woning', 'bouwjaar', 'oppervlakte', 'datum_ondertekening', 'PC6', 'Wijk2018'], axis=1)



code = []
for i in wijk_facilities['codering']:
    i = i[2:]
    while i[0] == "0":
        i = i[1:]
    code.append(i)
wijk_facilities['codering'] = code
wijk_facilities['codering'] = wijk_facilities['codering'].astype(int)

funda_gemeente_facilties = pd.merge(funda_gemeente_code, gemeente_facilities, how='left', left_on='Gemeentenaam', right_on='Gemeentenaam')
funda_buurt_facilties = pd.merge(funda_buurt_code, wijk_facilities, how='left', left_on='Buurtcode', right_on='codering')

funda_gemeente_facilties_clean = funda_gemeente_facilties.dropna()
funda_buurt_facilties_clean = funda_buurt_facilties.dropna()

#gemeente data
aptoheek = funda_gemeente_facilties_clean.loc[(funda_gemeente_facilties_clean['Afstand_tot_apotheek_km'] < 10.0 )]
distance_apotheek = aptoheek[['global_id', 'postcode', 'Gemeentenaam', 'Afstand_tot_apotheek_km']]
print(distance_apotheek)

ziekenhuis = funda_gemeente_facilties_clean.loc[(funda_gemeente_facilties_clean['Afstand_tot_ziekenhuis_km'] < 20.0 )]
distance_ziekenhuis = ziekenhuis[['global_id', 'postcode', 'Gemeentenaam', 'Afstand_tot_ziekenhuis_km']]
print(distance_ziekenhuis)

levensmiddelen = funda_gemeente_facilties_clean.loc[(funda_gemeente_facilties_clean['Afstand_tot_levensmiddelen_km'] < 6.0 )]
distance_levensmiddelen = levensmiddelen[['global_id', 'postcode', 'Gemeentenaam', 'Afstand_tot_levensmiddelen_km']]
print(distance_levensmiddelen)

cafe = funda_gemeente_facilties_clean.loc[(funda_gemeente_facilties_clean['Afstand_tot_cafe_km'] < 11.0 )]
distance_cafe = cafe[['global_id', 'postcode', 'Gemeentenaam', 'Afstand_tot_cafe_km']]
print(distance_cafe)

restaurant = funda_gemeente_facilties_clean.loc[(funda_gemeente_facilties_clean['Afstand_tot_restaurant_km'] < 11.0 )]
distance_restaurant = restaurant[['global_id', 'postcode', 'Gemeentenaam', 'Afstand_tot_restaurant_km']]
print(distance_restaurant)

middelbareschool = funda_gemeente_facilties_clean.loc[(funda_gemeente_facilties_clean['Afstand_tot_middelbareschool_km'] < 6.0 )]
distance_middelbareschool = middelbareschool[['global_id', 'postcode', 'Gemeentenaam', 'Afstand_tot_middelbareschool_km']]
print(distance_middelbareschool)

hoofdverkeersweg = funda_gemeente_facilties_clean.loc[(funda_gemeente_facilties_clean['Afstand_tot_oprit_hoofdverkeersweg_km'] < 17.0 )]
distance_hoofdverkeersweg = hoofdverkeersweg[['global_id', 'postcode', 'Gemeentenaam', 'Afstand_tot_oprit_hoofdverkeersweg_km']]
print(distance_hoofdverkeersweg)

treinstations = funda_gemeente_facilties_clean.loc[(funda_gemeente_facilties_clean['Afstand_tot_treinstations_km'] < 5.0 )]
distance_treinstations = treinstations[['global_id', 'postcode', 'Gemeentenaam', 'Afstand_tot_treinstations_km']]
print(distance_treinstations)

podiumkunsten = funda_gemeente_facilties_clean.loc[(funda_gemeente_facilties_clean['Afstand_tot_podiumkunsten_km'] < 30.0 )]
distance_podiumkunsten = podiumkunsten[['global_id', 'postcode', 'Gemeentenaam', 'Afstand_tot_podiumkunsten_km']]
print(distance_podiumkunsten)

#buurtdata
huisartsenpraktijk = funda_buurt_facilties_clean.loc[(funda_buurt_facilties_clean['Afstand_tot_huisartsenpraktijk_km'] < 5.0 )]
distance_huisartsenpraktijk = huisartsenpraktijk[['global_id', 'postcode', 'Gemeentenaam','Buurtnaam', 'Afstand_tot_huisartsenpraktijk_km']]
print(distance_huisartsenpraktijk)

grote_supermarkt = funda_buurt_facilties_clean.loc[(funda_buurt_facilties_clean['Afstand_tot_grote_supermarkt_km'] < 11.0 )]
distance_grote_supermarkt = grote_supermarkt[['global_id', 'postcode', 'Gemeentenaam','Buurtnaam', 'Afstand_tot_grote_supermarkt_km']]
print(distance_grote_supermarkt)

kinderdagverblijf = funda_buurt_facilties_clean.loc[(funda_buurt_facilties_clean['Afstand_tot_kinderdagverblijf_km'] < 5.0 )]
distance_kinderdagverblijf = kinderdagverblijf[['global_id', 'postcode', 'Gemeentenaam','Buurtnaam', 'Afstand_tot_kinderdagverblijf_km']]
print(distance_kinderdagverblijf)

basisschool = funda_buurt_facilties_clean.loc[(funda_buurt_facilties_clean['Afstand_tot_school_km'] < 5.0 )]
distance_basisschool = basisschool[['global_id', 'postcode', 'Gemeentenaam','Buurtnaam', 'Afstand_tot_school_km']]
print(distance_basisschool)


cursor.close()
connection.close()

csv = [distance_apotheek,distance_ziekenhuis, distance_levensmiddelen, distance_cafe, distance_restaurant, distance_middelbareschool, distance_hoofdverkeersweg, distance_treinstations, distance_podiumkunsten, distance_huisartsenpraktijk, distance_grote_supermarkt, distance_kinderdagverblijf, distance_basisschool]

for row in range(len(csv)):
    distance_apotheek.to_csv(f"storage/query_7_{row}.csv", sep=';' , decimal=",")

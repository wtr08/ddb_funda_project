# listOfNum = ['BU00030000', 'BU00030000','BU00030000']

# leadingremoved = []


# leadingremoved = []
# for i in listOfNum:
#     i = i[2:]
#     while i[0] == "0":
#         i = i[1:]
#     leadingremoved.append(i)


# print(leadingremoved)

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

# Average house price in 2018-2019 according to the age group (in the Netherlands, not municipality)


# dataset1 = cursor.execute("SELECT funda2018.postcode, funda2018.koopprijs, postcode2018.pc6, postcode2018.gemeente2018, gemeentenaam2018.gemcode, gemeentenaam2018.gemeentenaam, cbs_municipality.gemeentenaam, cbs_municipality._0_tot_15, cbs_municipality._15_tot_25, cbs_municipality._25_tot_45, cbs_municipality._45_tot_65, cbs_municipality._65_or_older FROM funda2018, gemeentenaam2018, cbs_municipality INNER JOIN postcode2018 ON funda2018.postcode = postcode2018.pc6")
# dat1 = cursor.execute("SELECT funda2018.postcode, postcode2018.pc6 FROM funda2018 INNER JOIN postcode2018 ON funda2018.postcode = postcode2018.pc6 INNER JOIN gemeentenaam2018 ON postcode2018.gemeente2018 = gemeentenaam2018.gemcode INNER JOIN CBS_Municipality ON gemeentenaam2018.Gemeentenaam = CBS_Municipality.Gemeentenaam")
# dat2 = cursor.fetchall()


# dat3 = cursor.execute("SELECT funda2018.postcode, CBS_Municipality.Gemeentenaam, gemeente2018.Gemeentenaam FROM funda2018, gemeentenaam2018, CBS_Municipality ")
# dat4 = cursor.fetchall()

# print(dat4)



# Avarage kooprijs
cursor.execute("SELECT koopprijs FROM funda2018")
koopprijs1 = cursor.fetchone()
print(np.mean(koopprijs1))

# Age group 
cursor.execute("SELECT _0_tot_15, _15_tot_25, _45_tot_65 FROM CBS_Municipality")

df=pd.DataFrame(cursor.fetchall(),columns=['_0_tot_15', '_15_tot_25', '_45_tot_65'])

_0_tot_15 = df['_0_tot_15'].sum()
_15_tot_25 = df['_0_tot_15'].sum()
_45_tot_65 = df['_0_tot_15'].sum()

# Age group to percent



# age group avarage 
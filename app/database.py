import os
from dotenv import load_dotenv
from pathlib import Path  # python3 only
import psycopg2 as psy

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

ZIP_sql_select_Query1 = "DROP TABLE IF EXISTS postcode_table;"
ZIP_sql_select_Query2 = 'CREATE TABLE postcode_table (PC6 VARCHAR PRIMARY KEY, Buurt2020 int, Wijk2020 int, Gemeente2020 int);'
cursor.execute(ZIP_sql_select_Query1)
cursor.execute(ZIP_sql_select_Query2)

# SQL Setup
FundaDB_sql_select_Query1 = "DROP TABLE IF EXISTS fundahousingnl;"
FundaDB_sql_select_Query2 = 'CREATE TABLE fundahousingnl ( global_ID INT PRIMARY KEY, publicatie_datum VARCHAR, postcode VARCHAR, koopprijs VARCHAR(50),categorieobject TEXT, bouwjaar VARCHAR, ind_tuin VARCHAR, perceel_oppervlakte VARCHAR(50),aantal_kamers VARCHAR, aantal_badkamers VARCHAR(50), energielabel_klasse TEXT, oppervlakte VARCHAR, PC6 varchar, FOREIGN KEY (PC6) REFERENCES postcode_table(PC6));'
cursor.execute(FundaDB_sql_select_Query1)
cursor.execute(FundaDB_sql_select_Query2)



connection.commit()

# Fill in the content into the databases
f_contents = open(f"{path}/data/utf8fundahousingdata.csv")
cursor.copy_from(f_contents, "fundahousingnl", columns=('global_ID', 'publicatie_datum', 'postcode', 'koopprijs', 'categorieObject', 'bouwjaar', 'ind_tuin', 'perceel_oppervlakte', 'aantal_kamers', 'aantal_badkamers', 'energielabel_klasse', 'oppervlakte'), sep=";")

zip_contents = open(f"{path}/data/2020-cbs-pc6huisnr20200801-buurt/pc6-gwb20201.csv")
cursor.copy_from(zip_contents, "postcode_table", columns=('PC6', 'Buurt2020', 'Wijk2020', 'Gemeente2020'), sep=";")


# Make the changes to the database persistent
connection.commit()



# create inner joins
ZIP_sql_select_Query1 = "DROP TABLE IF EXISTS postcode_joined;"
ZIP_sql_select_Query2 = 'CREATE TABLE postcode_joined (global_ID int, koopprijs varchar, PC6 varchar, Gemeente2020 int, postcode varchar);'
ZIP_sql_select_Query3 = 'SELECT fundahousingnl.global_ID, fundahousingnl.koopprijs, postcode_table.PC6, postcode_table.Gemeente2020, fundahousingnl.postcode INTO postcode_joined FROM fundahousingnl INNER JOIN postcode_table ON fundahousingnl.postcode = postcode_table.PC6'

cursor.execute(ZIP_sql_select_Query2)
cursor.execute(ZIP_sql_select_Query3)

# Close communication with the database
cursor.close()
connection.close()
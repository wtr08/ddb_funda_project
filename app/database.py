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


# 1. [Orange] -- Setup Funda Table
cursor.execute("DROP TABLE IF EXISTS funda2018 CASCADE;")
cursor.execute('CREATE TABLE funda2018 ( global_id INTEGER NOT NULL PRIMARY KEY, publicatie_datum VARCHAR, postcode VARCHAR, koopprijs VARCHAR, volledige_omschrijving VARCHAR, soort_woning VARCHAR, bouwjaar VARCHAR, oppervlakte VARCHAR, datum_ondertekening VARCHAR );')

# 2. [Pink] -- Setup Postcode2018 Table
cursor.execute("DROP TABLE IF EXISTS postcode2018 CASCADE;")
cursor.execute('CREATE TABLE postcode2018 ( PC6 VARCHAR NOT NULL PRIMARY KEY, Buurt2018 INTEGER, Wijk2018 INTEGER, Gemeente2018 INTEGER );')

# 3. [Pink] -- Setup Gemeentenaam2018 Table
cursor.execute("DROP TABLE IF EXISTS gemeentenaam2018 CASCADE;")
cursor.execute('CREATE TABLE gemeentenaam2018 ( Gemcode INTEGER NOT NULL PRIMARY KEY, Gemeentenaam VARCHAR );')

# 4. [Pink] -- Setup Wijknaam2018 Table
cursor.execute("DROP TABLE IF EXISTS wijknaam2018 CASCADE;")
cursor.execute('CREATE TABLE wijknaam2018 ( Wijkcode INTEGER NOT NULL PRIMARY KEY, Wijknaam VARCHAR );')

# 5. [Pink] -- Setup Buurtnaam2018 Table
cursor.execute("DROP TABLE IF EXISTS buurtnaam2018 CASCADE;")
cursor.execute('CREATE TABLE buurtnaam2018 ( Buurtcode INTEGER NOT NULL PRIMARY KEY, Buurtnaam VARCHAR );')

# 6. [Yellow] -- Setup Gemeente_afstand_facilities Table
cursor.execute("DROP TABLE IF EXISTS gemeente_afstand_facilities CASCADE;")
cursor.execute('CREATE TABLE gemeente_afstand_facilities ( Gemeentenaam VARCHAR NOT NULL PRIMARY KEY, Afstand_tot_apotheek_km NUMERIC, Afstand_tot_ziekenhuis_km NUMERIC, Afstand_tot_levensmiddelen_km NUMERIC, Afstand_tot_cafe_km NUMERIC, Afstand_tot_restaurant_km NUMERIC, Afstand_tot_middelbareschool_km NUMERIC, Afstand_tot_oprit_hoofdverkeersweg_km NUMERIC, Afstand_tot_treinstations_km NUMERIC, Afstand_tot_podiumkunsten_km NUMERIC );')

# 7. [Yellow] -- Setup Gemeente_facilities_gemeente Table
cursor.execute("DROP TABLE IF EXISTS Gemeente_facilities_gemeentes CASCADE;")
cursor.execute('CREATE TABLE Gemeente_facilities_gemeentes ( Gemeentenaam VARCHAR NOT NULL PRIMARY KEY, Vestiging_uit_andere_gemeente INTEGER, Vertrek_naar_andere_gemeente INTEGER, Verhuismobiliteit NUMERIC, Gemiddeld_aantal_inwoners NUMERIC );')

# 8. [Yellow] -- Setup Wijk_facilities_buurt Table
cursor.execute("DROP TABLE IF EXISTS Wijk_facilities_buurt CASCADE;")
cursor.execute('CREATE TABLE Wijk_facilities_buurt ( Wijken_en_buurten VARCHAR, Gemeentenaam VARCHAR, Soort_regio VARCHAR, Codering VARCHAR, Woningvoorraad INTEGER, Percentage_bewoond INTEGER, Percentage_onbewoond INTEGER, Diefstal_uit_woning INTEGER, Vernieling INTEGER, Geweld INTEGER, Afstand_tot_huisenpraktijk NUMERIC, Afstand_tot_grote_supermarkt NUMERIC, Afstand_tot_kinderverblijf NUMERIC, Afstand_tot_school NUMERIC );')

# 9. [Yellow] -- Setup CBS_municipality Table
cursor.execute("DROP TABLE IF EXISTS CBS_Municipality CASCADE;")
cursor.execute('CREATE TABLE CBS_Municipality ( Wijken_en_buurten VARCHAR, Gemeentenaam VARCHAR, Soort_regio VARCHAR, Codering VARCHAR NOT NULL PRIMARY KEY, Mannen INTEGER, Vrouwen INTEGER, _0_tot_15 INTEGER, _15_tot_25 INTEGER, _25_tot_45 INTEGER, _45_tot_65 INTEGER, _65_or_older INTEGER, Bevolkingsdichtheid INTEGER, Gemiddeld_inkomen_per_inwoner NUMERIC );')


# 10. [Green] -- Setup gemiddelde_verkoopprijzen Table
cursor.execute("DROP TABLE IF EXISTS Gemiddelde_verkoopprijzen CASCADE;")
cursor.execute('CREATE TABLE Gemiddelde_verkoopprijzen ( Gemeentenaam VARCHAR NOT NULL PRIMARY KEY, Year_2014 INTEGER, Year_2015 INTEGER, Year_2016 INTEGER, Year_2017 INTEGER, Year_2018 INTEGER );')



connection.commit()


csv_file_name = f"{path}/data//1_Orange/suley_tested/funda2018_utf8.csv"
sql = "COPY funda2018 FROM STDIN DELIMITER ';' CSV HEADER"
cursor.copy_expert(sql, open(csv_file_name, "r"))
cursor.execute("COMMIT;")


# zip_contents = open(f"{path}/data/2020-cbs-pc6huisnr20200801-buurt/pc6-gwb20201.csv")
# cursor.copy_from(zip_contents, "postcode_table", columns=('PC6', 'Buurt2020', 'Wijk2020', 'Gemeente2020'), sep=";")


# # Make the changes to the database persistent
# connection.commit()

#1# Orange (funda2018) 


# # create inner joins
# ZIP_sql_select_Query1 = "DROP TABLE IF EXISTS postcode_joined;"
# ZIP_sql_select_Query2 = 'CREATE TABLE postcode_joined (global_ID int, koopprijs varchar, PC6 varchar, Gemeente2020 int, postcode varchar);'
# ZIP_sql_select_Query3 = 'SELECT fundahousingnl.global_ID, fundahousingnl.koopprijs, postcode_table.PC6, postcode_table.Gemeente2020, fundahousingnl.postcode INTO postcode_joined FROM fundahousingnl INNER JOIN postcode_table ON fundahousingnl.postcode = postcode_table.PC6'

# cursor.execute(ZIP_sql_select_Query2)
# cursor.execute(ZIP_sql_select_Query3)

# Close communication with the database
cursor.close()
connection.close()


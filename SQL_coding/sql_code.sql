/* Inserting tables */


/* 1. [Orange] -- Setup Funda Table */
CREATE TABLE funda2018_dirty (
global_id INTEGER NOT NULL PRIMARY KEY, 
publicatie_datum VARCHAR, 
postcode VARCHAR, 
koopprijs INT, 
volledige_omschrijving VARCHAR, 
soort_woning VARCHAR, 
bouwjaar VARCHAR, 
oppervlakte VARCHAR, 
datum_ondertekening VARCHAR
);

/* 2. [Pink] -- Setup Postcode2018 Table */
CREATE TABLE postcode2018 ( 
PC6 VARCHAR NOT NULL PRIMARY KEY, 
Buurt2018 INTEGER, 
Wijk2018 INTEGER, 
Gemeente2018 INTEGER 
);

/* 3. [Pink] -- Setup Gemeentenaam2018 Table */
CREATE TABLE gemeentenaam2018 (
Gemcode INTEGER NOT NULL PRIMARY KEY, 
Gemeentenaam VARCHAR 
);

/* 4. [Pink] -- Setup Wijknaam2018 Table */
CREATE TABLE wijknaam2018 (
Wijkcode INTEGER NOT NULL PRIMARY KEY, 
Wijknaam VARCHAR 
);

/* 5. [Pink] -- Setup Buurtnaam2018 Table */
CREATE TABLE buurtnaam2018 ( 
Buurtcode INTEGER NOT NULL PRIMARY KEY, 
Buurtnaam VARCHAR 
);

/* 6. [Yellow] -- Setup Gemeente_afstand_facilities Table*/
CREATE TABLE gemeente_afstand_facilities ( 
Gemeentenaam VARCHAR NOT NULL PRIMARY KEY, 
Afstand_tot_apotheek_km NUMERIC, 
Afstand_tot_ziekenhuis_km NUMERIC, 
Afstand_tot_levensmiddelen_km NUMERIC, 
Afstand_tot_cafe_km NUMERIC, 
Afstand_tot_restaurant_km NUMERIC, 
Afstand_tot_middelbareschool_km NUMERIC, 
Afstand_tot_oprit_hoofdverkeersweg_km NUMERIC, 
Afstand_tot_treinstations_km NUMERIC, 
Afstand_tot_podiumkunsten_km NUMERIC 
);

/* 7. [Yellow] -- Setup Gemeente_facilities_gemeente Table*/
CREATE TABLE Gemeente_facilities_gemeentes (
Gemeentenaam VARCHAR NOT NULL PRIMARY KEY, 
Vestiging_uit_andere_gemeente INTEGER, 
Vertrek_naar_andere_gemeente INTEGER, 
Verhuismobiliteit NUMERIC, 
Gemiddeld_aantal_inwoners NUMERIC 
);

/* 8. [Yellow] -- Setup Wijk_facilities_buurt Table */
CREATE TABLE Wijk_facilities_buurt (
Wijken_en_buurten VARCHAR, 
Gemeentenaam VARCHAR, 
Soort_regio VARCHAR, 
Codering INTEGER, 
Woningvoorraad INTEGER, 
Percentage_bewoond INTEGER, 
Percentage_onbewoond INTEGER, 
Diefstal_uit_woning INTEGER, 
Vernieling INTEGER, 
Geweld INTEGER, 
Afstand_tot_huisenpraktijk NUMERIC, 
Afstand_tot_grote_supermarkt NUMERIC, 
Afstand_tot_kinderverblijf NUMERIC, 
Afstand_tot_school NUMERIC 
);

/* 9. [Yellow] -- Setup CBS_municipality Table */
CREATE TABLE CBS_Municipality (
Wijken_en_buurten VARCHAR, 
Gemeentenaam VARCHAR, 
Soort_regio VARCHAR, 
Codering INTEGER, 
Mannen INTEGER, 
Vrouwen INTEGER, 
_0_tot_15 INTEGER, 
_15_tot_25 INTEGER, 
_25_tot_45 INTEGER, 
_45_tot_65 INTEGER, 
_65_or_older INTEGER, 
Bevolkingsdichtheid INTEGER, 
Gemiddeld_inkomen_per_inwoner NUMERIC 
);

/* 10. [Green] -- Setup gemiddelde_verkoopprijzen Table */
CREATE TABLE Gemiddelde_verkoopprijzen (
Gemeentenaam VARCHAR NOT NULL PRIMARY KEY, 
Year_2014 INTEGER, 
Year_2015 INTEGER, 
Year_2016 INTEGER, 
Year_2017 INTEGER, 
Year_2018 INTEGER 
);

/* ## -- Load data into tables -- ### */

/* ## 1. Orange (funda2018) */

/* ## 1.1 funda2018 */
\copy funda2018 FROM '/Users/suleymanekiz/Desktop/database_management/fundaproject/data/1_Orange/suley_tested/funda2018_utf8.csv' with delimiter ';' csv header ;

/* 2. Pink (Postcode 2018)*/

/* 2.1 postcode2018 */
\copy postcode2018 FROM '/Users/suleymanekiz/Desktop/database_management/fundaproject/data/2_Pink/suley_tested/new/postcode2018_utf8.csv' with delimiter ';' csv header ;
/* 2.2 gemeentenaam2018 */
\copy gemeentenaam2018 FROM '/Users/suleymanekiz/Desktop/database_management/fundaproject/data/2_Pink/suley_tested/new/newgemeentenaam2018_utf8.csv' with delimiter ';' csv header ;
/* 2.3 wijknaam2018 */
\copy wijknaam2018 FROM '/Users/suleymanekiz/Desktop/database_management/fundaproject/data/2_Pink/suley_tested/new/wijknaam2018_utf8.csv' with delimiter ';' csv header ;
/* 2.4 buurtnaam2018*/
\copy buurtnaam2018 FROM '/Users/suleymanekiz/Desktop/database_management/fundaproject/data/2_Pink/suley_tested/new/buurtnaam2018_utf8.csv' with delimiter ';' csv header ;

/* 3. Yellow */

/* 3.1 Gemeente Afstand Gemeente */
\copy gemeente_afstand_facilities FROM '/Users/suleymanekiz/Desktop/database_management/fundaproject/data/3_Yellow/suley_tested/new/afstandfacilitiescsvutf8.csv' with delimiter ';' csv header ;
/* 3.2 Gemeente Facilities Gemeente */
\copy gemeente_facilities_gemeentes FROM '/Users/suleymanekiz/Desktop/database_management/fundaproject/data/3_Yellow/suley_tested/new/gemeente_utf8_facilitiesssss.csv' with delimiter ';' csv header ;
/* 3.3 Wijk Facilities Buurt*/
\copy wijk_facilities_buurt FROM '/Users/suleymanekiz/Desktop/database_management/fundaproject/data/3_Yellow/suley_tested/new/wijk_facilities_buurt_utf8.csv' with delimiter ';' csv header ;

/* 3.4 CBS_Municipality */
\copy CBS_Municipality FROM '/Users/suleymanekiz/Desktop/database_management/fundaproject/data/3_Yellow/suley_tested/new/cbs_muni_query6_new.csv' with delimiter ';' csv header ;

/* 4. Green */

/* 4.1 Gemiddelde Verkoopprijzen */
\copy gemiddelde_verkoopprijzen FROM '/Users/suleymanekiz/Desktop/database_management/fundaproject/data/4_Green/suley_tested/new/gemiddelde_verkoopprijzen_utf8.csv' with delimiter ';' csv header ;

/* #innerjoin funda zipcodes with postcode2018 */
CREATE TABLE funda2018 AS SELECT * FROM funda2018_dirty INNER JOIN postcode2018 on postcode2018.pc6 = funda2018_dirty.postcode


/* STEP 3  Connect keys among each other*/

/* 1. connecting funda2018(postcode) with postcode2018(pc6) */
ALTER TABLE funda2018 ADD FOREIGN KEY (postcode) REFERENCES postcode2018(pc6);

/* 2. connecting postcode2018(Buurt2018) with buurtnaam2018(buurtcode)*/
ALTER TABLE postcode2018 ADD FOREIGN KEY (Buurt2018) REFERENCES buurtnaam2018(buurtcode);

/* 3. connecting postcode2018(Wijk2018) with wijknaam2018(Wijkcode) */
ALTER TABLE postcode2018 ADD FOREIGN KEY (Wijk2018) REFERENCES wijknaam2018(Wijkcode);

/* 4.connecting postcode2018(Gemeente2018) with gemeentenaam2018(Gemcode) */
ALTER TABLE postcode2018 ADD FOREIGN KEY (Gemeente2018) REFERENCES gemeentenaam2018(Gemcode);

/* 5.connecting gemeentenaam2018(gemeentenaam) with gemeente_afstand_facilities (Gemeentenaam) */
ALTER TABLE gemeentenaam2018 ADD FOREIGN KEY (gemeentenaam) REFERENCES gemeente_afstand_facilities(Gemeentenaam);

/* 6.connecting gemeentenaam2018(gemeentenaam) with gemeente_facilities_gemeentes(Gemeentenaam)# */
ALTER TABLE gemeentenaam2018 ADD FOREIGN KEY(Gemeentenaam) REFERENCES gemeente_facilities_gemeentes(Gemeentenaam);

/* 7. connecting wijk_facilities_buurt (Codering) with buurtnaam2018(Buurtcode)# */
ALTER TABLE wijk_facilities_buurt ADD FOREIGN KEY (Codering) REFERENCES buurtnaam2018(Buurtcode);

/* 8. connecting cbs_municipality (codering) with gemeentenaam2018(Gemcode)#*/
ALTER TABLE cbs_municipality ADD FOREIGN KEY (codering) REFERENCES gemeentenaam2018(Gemcode);

/* 9. connecting gemeentenaam2018(gemeentenaam) with gemiddelde_verkoopprijzen(gemeentenaam)*/
ALTER TABLE gemeentenaam2018 ADD FOREIGN KEY (gemeentenaam) REFERENCES gemiddelde_verkoopprijzen(gemeentenaam);




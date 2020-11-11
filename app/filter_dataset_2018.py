import pandas as pd

data = pd.read_csv("/Users/woutervanrijmenam/stack/02_Projects/04_Sites/DigitalHands/funda_project/data/2018-cbs-pc6huisnr20180801_buurt -vs2/pc6hnr20180801_gwb-vs2.csv", sep=";")

# Deleting the collumn huisnummer
data = data.drop(columns="Huisnummer")

# sorting by first name 
data.sort_values("PC6", inplace = True) 
  
# dropping ALL duplicte values 
data.drop_duplicates(subset=["PC6"], keep = 'last', inplace = True) 
  
# displaying data 
print(data)

data.to_csv("new_data_2018.csv", sep=';')
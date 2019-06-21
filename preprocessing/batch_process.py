import warnings
warnings.filterwarnings("ignore")

import os

import pandas as pd
import numpy as np

from qualitative import getQualiFeatures
from aggregate_qualitative import getUnique

data = os.path.abspath("../data/iWant/raw/11/IWantTransactionFactTable-20181102.csv")

cols = ["fingerprintid", "previousfingerprintid", "sitedomain", "deviceos", 
			"devicetype", "ipaddress", "browsertype", "screensize", "gigyaid", 
			"browserversion", "osversion", "devicename", "connectivitytype", "videoquality"]

df_chunk = pd.read_csv(data, dtype = str, usecols = cols, low_memory = False, chunksize = 100000)
all_df = []
for chunk in df_chunk:
	print(chunk.shape)
	all_df.append(getQualiFeatures(chunk))

all_df = pd.concat(all_df)
print(all_df.index.name)
df_cols = all_df.columns
new_df = pd.DataFrame(index = all_df.index.unique(), columns = cols)
for col in df_cols:
		new_df[col] = getUnique(all_df, col)[col].values

print(new_df)
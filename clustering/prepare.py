import warnings
warnings.filterwarnings("ignore")

import os

import pandas as pd
import numpy as np

value = lambda x: x.strip("[]").replace("'", "").split(", ")
converters={"actiontaken": value,
			"devicetype": value,
			"deviceos": value,
			"osversion": value,
			"ipaddress": value,
			"browsertype": value,
			"connectivitytype": value,
			"screensize": value,
			"videoquality": value,
			"sitedomain": value,
			"devicename": value,
			"browserversion": value}

def getUnique(df, col):
	df = df[col].apply(tuple)
	unique = df.unique()
	unique = list(set([a for t in unique for a in t]))
	unique = [word.replace('nan', 'nan_'+col) for word in unique]
	return unique

def prepareDataDevice(data_dir, filename):
	cols = ["gigyaid", "devicetype", "deviceos", "browsertype", "screensize", "videoquality"]
	device_cols = ["devicetype", "deviceos", "browsertype", "screensize", "videoquality"]
	df = pd.read_csv(os.path.join(data_dir, file), dtype = str, low_memory = False, usecols = cols, converters = converters)

	feature_cols = []
	for col in device_cols:
		x = getUnique(df, col)
		feature_cols.extend(x)
	print(df.columns)
	new_df = pd.DataFrame(index = df.gigyaid, columns = feature_cols)
	df = df.set_index("gigyaid")
	for user_id in df.index.unique():
		user = df.loc[user_id]
		for col in device_cols:
			a = user[col]
			a = [word.replace('nan', 'nan_'+col) for word in a]
			for b in a:
				new_df.loc[user_id][b] = 1

	new_df.fillna(0, inplace=True)
	return(new_df)
import warnings
warnings.filterwarnings("ignore")

import os

import pandas as pd
import numpy as np

from ast import literal_eval
from ip2geotools.databases.noncommercial import DbIpCity


def getUnique(df, col):
	group = df.groupby("gigyaid")
	df[col] = df[col].apply(tuple)
	unique = group.apply(lambda x: x[col].unique()).reset_index(name=col)
	unique[col] = unique[col].apply(lambda a: list(set([x for t in a for x in t])))
	return unique

def ipToCity(ipaddresses):
	locations = []
	for ip in ipaddresses:
		try:
			response = DbIpCity.get(ip, api_key='free')
			loc = str(response.city) + ", " + str(response.region)
			locations.append(loc)
			print(ip, loc)
		except:
			pass
	return locations

def aggregateQualitative(quali_dir, out_dir, filename):
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
	df = pd.DataFrame()
	for i in os.listdir(quali_dir):
		file = os.path.join(quali_dir, i)
		day_df = pd.read_csv(file, converters = converters)
		df = pd.concat([df, day_df], axis = 0)

	df = df.set_index("gigyaid")
	cols = df.columns
	new_df = pd.DataFrame(index = df.index.unique(), columns = cols)
	for col in cols:
		# print(col)
		new_df[col] = getUnique(df, col)[col].values

	# new_df["location_city"] = new_df["ipaddress"].apply(lambda x: ipToCity(x))

	new_df.to_csv(os.path.join(out_dir, filename))

if __name__ == '__main__':
	quali_dir = "../data/iWant/processed/qualitative/10"
	out_dir = "../data/iWant/processed/qualitative/aggregated"
	filename = "october_2018.csv"
	aggregateQualitative(quali_dir, out_dir, filename)
	
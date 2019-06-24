import warnings 
warnings.filterwarnings("ignore")

import os
import pandas as pd
import numpy as np

data_dir = "../clustering"

def getUnique(df, col):
	df = df[col].apply(tuple)
	unique = df.unique()
	unique = list([a for t in unique for a in t])
	return unique


def getCountLocation(filename, outfile):
	value = lambda x: x.strip("[]").replace("'", "").split(", ")
	converters={"ipaddress": value,
				"location_city": value}

	df = pd.read_csv(os.path.join(data_dir, filename), converters = converters, dtype = str)
	locations = getUnique(df, "location_city")
	loc_count = pd.DataFrame(index = set(locations), columns = ["count"])
	for i in set(locations):
		loc_count.loc[i]["count"] = locations.count(i)
	loc_count.to_csv(outfile)

if __name__ == '__main__':
	getCountLocation("september_2018_location", "sep_2018.csv")
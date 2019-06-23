import warnings
warnings.filterwarnings("ignore")

import os

import pandas as pd
import numpy as np

from qualitative import getQualiFeatures, getUrls, getDataChunk
from aggregate_qualitative import getUnique, aggregateQualitative

outdirectory = "../data/iWant/processed/qualitative"
temp = os.path.abspath("../data/iWant/temp")
urllist = getUrls("../data/url_try.txt")

cols = ["fingerprintid", "previousfingerprintid", "sitedomain", "deviceos", 
			"devicetype", "ipaddress", "browsertype", "screensize", "gigyaid", 
			"browserversion", "osversion", "devicename", "connectivitytype", "videoquality"]

token = lib.auth()
adl = core.AzureDLFileSystem(token, store_name = 'bigdatadevdatalake')

for i in urllist:
	print("{}-{}-{}".format(i[-12:-8], i[-8:-6], i[-6:-4]))
	df_chunk = getDataChunk(i, cols, chunksize = 5000000)
	outfile = "{}-{}-{}.csv".format(i[-12:-8], i[-8:-6], i[-6:-4])
	counter = 1
	for chunk in df_chunk:
		print("Chunk number: ", counter)
		df = chunk.loc[chunk.gigyaid.notnull()]
		quali = getQualiFeatures(df)
		temp_out = os.path.join(temp, str(counter)+".csv")
		quali.to_csv(temp_out)
		counter = counter + 1

	aggregateQualitative(temp, outdirectory, outfile)

	for f in os.listdir(temp):
		os.remove(os.path.join(temp, f))
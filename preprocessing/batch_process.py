import warnings
warnings.filterwarnings("ignore")

import os
import time

import pandas as pd
import numpy as np

from qualitative import getQualiFeatures, getUrls
from azure.datalake.store import core, lib, multithread
from aggregate_qualitative import getUnique, aggregateQualitative

outdirectory = "../data/iWant/processed/qualitative"
temp = os.path.abspath("../data/iWant/temp")
urllist = getUrls("../data/url_try.txt")

cols = ["fingerprintid", "previousfingerprintid", "sitedomain", "deviceos", 
			"devicetype", "ipaddress", "browsertype", "screensize", "gigyaid", 
			"browserversion", "osversion", "devicename", "connectivitytype", "videoquality"]

def getDataChunk(dataurl, cols, chunksize):
	s = time.time()
	print("Getting Data")
	with adl.open(dataurl, "rb") as f:
		df = pd.read_csv(f, usecols = cols, dtype = str, low_memory = False, chunksize = chunksize)
	e = time.time()
	total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
	print("Successfull getting data!", total_time)
	return df

if __name__ == '__main__':
	token = lib.auth()
	adl = core.AzureDLFileSystem(token, store_name = 'bigdatadevdatalake')

	for i in urllist:
		print("{}-{}-{}".format(i[-12:-8], i[-8:-6], i[-6:-4]))
		try:
			exist = adl.exists(i)
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
		except:
			print("Url {} dos not exist!".format(i))
			pass
			
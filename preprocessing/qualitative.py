import warnings
warnings.filterwarnings('ignore')

import os

import pandas as pd
import numpy as np

from aggregate_qualitative import ipToCity
from azure.datalake.store import core, lib, multithread

def dayDetails(transact):
	users = transact['gigyaid'].dropna().unique()
	print("Total number of users: ", len(users))
	print("Total number of signed in transactions: ", len(transact.loc[transact.gigyaid.notnull()]))
	print("Total number of transactions: ", len(transact))
	print("\n")

def monthlyTransaction(directory, outdirectory):
	quali = pd.DataFrame()
	for f in os.listdir(directory):
		file = os.path.join(directory, f)
		print("{}-{}-{}".format(file[-12:-8], file[-8:-6], file[-6:-4]))
		transact = pd.read_csv(file)
		dayDetails(transact)
		transact = transact.loc[transact.gigyaid.notnull()]
		quali = getQualiFeatures(transact)
		outfile = os.path.join(outdirectory, "{}-{}-{}.csv".format(file[-12:-8], file[-8:-6], file[-6:-4]))
		if len(quali) == 0:
			continue
		quali.to_csv(outfile)


def getQualiFeatures(transact):
	print("Getting the qualitative features")
	if len(transact) == 0:
		return pd.DataFrame()
	group = transact.groupby("gigyaid")
	devicetype = group.apply(lambda x: x["devicetype"].unique().tolist()).reset_index(name="devicetype")
	deviceos = group.apply(lambda x: x["deviceos"].unique().tolist()).reset_index(name="deviceos")
	osversion = group.apply(lambda x: x["osversion"].unique().tolist()).reset_index(name="osversion")
	ipaddress = group.apply(lambda x: x["ipaddress"].unique().tolist()).reset_index(name="ipaddress")
	browsertype = group.apply(lambda x: x["browsertype"].unique().tolist()).reset_index(name="browsertype")
	connectivitytype = group.apply(lambda x: x["connectivitytype"].unique().tolist()).reset_index(name="connectivitytype")
	screensize = group.apply(lambda x: x["screensize"].unique().tolist()).reset_index(name="screensize")
	videoquality = group.apply(lambda x: x["videoquality"].unique().tolist()).reset_index(name="videoquality")
	devicename = group.apply(lambda x: x["devicename"].unique().tolist()).reset_index(name="devicename")
	actiontaken = group.apply(lambda x: x["actiontaken"].unique().tolist()).reset_index(name="actiontaken")
	
	df = pd.concat([devicetype, deviceos, osversion, ipaddress, browsertype, connectivitytype, screensize, videoquality, actiontaken], axis = 1)
	df = df.loc[:, ~df.columns.duplicated()]
	df = df.set_index('gigyaid')
	df["location"] = df["ipaddress"].apply(lambda x: ipToCity(x))
	print("Finish getting the qualitative features.")
	return(df)

def getData(dataurl):
	print("Getting Data")
	with adl.open(dataurl, "rb") as f:
	    df = pd.read_csv(f)
	print("Successfull getting data!")
	return df

def getUrls(urltext):
	with open(urltext, 'r') as f:
		urls = f.read().splitlines()
	return urls

if __name__ == '__main__':
	# token = lib.auth()
	# adl = core.AzureDLFileSystem(token, store_name = 'bigdatadevdatalake')
	outdirectory = "../data/iWant/processed/qualitative"
	urllist = getUrls("../data/urls.txt")
	for i in urllist:
		print("{}-{}-{}".format(i[-12:-8], i[-8:-6], i[-6:-4]))
		outfile = os.path.join(outdirectory, "{}-{}-{}.csv".format(i[-12:-8], i[-8:-6], i[-6:-4]))
		transact = getData(i)
		transact = transact.loc[transact.gigyaid.notnull()]
		quali = getQualiFeatures(transact)
		print("Saving file.")
                quali.to_csv(outfile)
                print("\n\n\n")


	# monthlyTransaction("../data/iWant/raw/10", "../data/iWant/processed/qualitative/10")



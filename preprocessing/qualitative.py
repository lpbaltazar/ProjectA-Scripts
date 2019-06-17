import warnings
warnings.filterwarnings('ignore')

import os

import pandas as pd
import numpy as np

def dayDetails(transact):
	users = transact['gigyaid'].dropna().unique()
	print("Total number of users: ", len(users))
	print("Total number of signed in transactions: ", len(transact.loc[transact.gigyaid.notnull()]))
	print("Total number of transactions: ", len(transact))
	print("\n")

def monthlyTransaction(directory):
	quali = pd.DataFrame()
	for f in os.listdir(directory):
		file = os.path.join(directory, f)
		print("{}-{}-{}".format(file[-12:-8], file[-8:-6], file[-6:-4]))
		transact = pd.read_csv(file)
		dayDetails(transact)
		quali = pd.concat([quali, getQualiFeatures(transact, quali)], axis = 0)
	quali = quali.set_index('gigyaid')
	print(quali)


def getQualiFeatures(transact, quali):
	transact = transact.loc[transact.gigyaid.notnull()]
	if len(transact) == 0:
		return pd.DataFrame()
	group = transact.groupby("gigyaid")
	devicetype = group.apply(lambda x: x["devicetype"].unique()).reset_index(name="devicetype")
	deviceos = group.apply(lambda x: x["deviceos"].unique()).reset_index(name="deviceos")
	osversion = group.apply(lambda x: x["osversion"].unique()).reset_index(name="osversion")
	ipaddress = group.apply(lambda x: x["ipaddress"].unique()).reset_index(name="ipadress")
	browsertype = group.apply(lambda x: x["browsertype"].unique()).reset_index(name="browsertype")
	connectivitytype = group.apply(lambda x: x["connectivitytype"].unique()).reset_index(name="connectivitytype")
	screensize = group.apply(lambda x: x["screensize"].unique()).reset_index(name="screensize")
	videoquality = group.apply(lambda x: x["videoquality"].unique()).reset_index(name="videoquality")
	actiontaken = group.apply(lambda x: x["actiontaken"].unique()).reset_index(name="actiontaken")
	
	df = pd.concat([devicetype, deviceos, osversion, ipaddress, browsertype, connectivitytype, screensize, videoquality, actiontaken], axis = 1)
	df = df.loc[:, ~df.columns.duplicated()]
	# print(df)
	return(df)

if __name__ == '__main__':
	monthlyTransaction("../data/iWant/raw/10")

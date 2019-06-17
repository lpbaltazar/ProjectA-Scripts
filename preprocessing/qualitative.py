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
		quali = getQualiFeatures(transact, quali)

def getQualiFeatures(transact, quali):
	transact = transact.loc[transact.gigyaid.notnull()]
	group = transact.groupby("gigyaid")
	df = group.apply(lambda x: x["devicetype"].unique()).reset_index(name="devicetype")
	print(df)
	return(df)

if __name__ == '__main__':
	monthlyTransaction("../data/iWant/raw/10")

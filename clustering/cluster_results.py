import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np

from kmodes.kmodes import KModes
from prepare import prepareDataDevice
from sklearn import metrics

data_dir = "../data/iWant/processed/preliminary"

def addLabels(data_dir, file, outfile, i):
	df = prepareDataDevice(data_dir, file)
	km = KModes(n_clusters=i, init='Huang', n_init=11)
	clusters = km.fit_predict(df.values)
	df["cluster_labels"] = km.labels_
	sil_score = metrics.silhouette_score(df.values, df["cluster_labels"].values, sample_size = 10000)
	print(sil_score)
	df.to_csv(outfile)

if __name__ == '__main__':
	# addLabels("../data/iWant/processed/preliminary", "september_2018.csv", "september_2018_cluster.csv", i =3)
	# addLabels("../data/iWant/processed/preliminary", "october_2018.csv", "october_2018_cluster.csv", i =3)
	addLabels("../data/iWant/processed/preliminary", "november_2018.csv", "november_2018_cluster.csv", i =9)
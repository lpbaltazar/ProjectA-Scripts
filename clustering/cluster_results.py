import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np

from kmodes.kmodes import KModes
from prepare import prepareDataDevice
from sklearn import metrics
from memory_profiler import profile

data_dir = "../data/iWant/processed/preliminary"

@profile
def addLabels(data_dir, file, outfile, centroids_outfile, i):
	df = prepareDataDevice(data_dir, file)
	km = KModes(n_clusters=i, init='Huang', n_init=11)
	clusters = km.fit_predict(df.values)
	cluster_centroids = km.cluster_centroids_
	cluster_centroids = pd.DataFrame(index = range(i), data = cluster_centroids, columns = df.columns)
	cluster_centroids.to_csv(centroids_outfile)
	df["cluster_labels"] = km.labels_
	sil_score = metrics.silhouette_score(df.values, df["cluster_labels"].values)
	print(sil_score)
	df.to_csv(outfile)

if __name__ == '__main__':
	addLabels("../data/iWant/processed/preliminary", "september_2018.csv", "september_2018_cluster.csv", "september_2018_cluster_centroids.csv", i =3)
	# addLabels("../data/iWant/processed/preliminary", "september_2018.csv", "september_2018_cluster.csv", "september_2018_cluster_centroids.csv", i =3)
	# addLabels("../data/iWant/processed/preliminary", "october_2018.csv", "october_2018_cluster.csv", "october_2018_cluster_centroids.csv", i =3)
	# addLabels("../data/iWant/processed/preliminary", "november_2018.csv", "november_2018_cluster.csv", i =7)
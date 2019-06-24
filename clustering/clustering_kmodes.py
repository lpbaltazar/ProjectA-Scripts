import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np

from kmodes.kmodes import KModes
from prepare import prepareDataDevice
from sklearn import metrics

data_dir = "../data/iWant/processed/preliminary"

file = "september_2018.csv"

df = prepareDataDevice(data_dir, file)

num_clusters = [2,3,5,7,9]
scores = []
for j in range(30):
	print("Trial {}".format(j))
	for i in num_clusters:
		cluster_score = {}
		cluster_score['num_cluster'] = i
		km = KModes(n_clusters=i, init='Huang', n_init=11)
		clusters = km.fit_predict(df.values)
		cluster_labels = km.labels_
		sil_score = metrics.silhouette_score(df.values, cluster_labels)
		cluster_score['sil_score'] = sil_score
		scores.append(cluster_score)

scores = pd.DataFrame(scores)
scores.to_csv("score_september_2018_device.csv", index = False)
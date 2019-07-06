import warnings
warnings.filterwarnings('ignore')

import os
from azure.datalake.store import core, lib, multithread

def download(download_dir, data_dir):
	token = lib.auth()
	adl = core.AzureDLFileSystem(token, store_name = 'bigdatadevdatalake')
	download_dir = "december_2018"

	for f in adl.ls(data_dir):
		outfile = os.joindir(download_dir, f[-38:])
		downloader = multithread.ADLDownloader(adl, f, outfile)
		if downloader.successful():
			print("Finished Downloading!")
		else:
			print("error in downloading!")


if __name__ == '__main__':
	download("december_2018", 'ProdDataHub/TransactionFactTable/IWant/2018/12')
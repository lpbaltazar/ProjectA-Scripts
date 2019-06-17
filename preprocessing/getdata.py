import warnings
warnings.filterwarnings('ignore')

import pandas as pd
from azure.datalake.store import core, lib, multithread

token = lib.auth()
adl = core.AzureDLFileSystem(token, store_name = 'bigdatadevdatalake')

def getData():
	with adl.open("ProdDataHub/TransactionFactTable/IWant/2018/09/IWantTransactionFactTable-20180905.csv", "rb") as f:
	    df = pd.read_csv(f)
	    return df
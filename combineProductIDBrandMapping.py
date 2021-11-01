import os
import pandas as pd
#C:\Users\tan.j.53\Procter and Gamble\AMA eBiz Data Hub - Documents\XByte Data\KCP\2021\09.09\Search
path = r"productID-Brand"
import numpy as np
import pandas as pd
import glob
all_data = pd.DataFrame()

for f in glob.glob(path+"\*.xlsx",recursive=True):
    print(f)
    df = pd.read_excel(f,sheet_name="Product Performance Item Level")
    all_data = all_data.append(df, ignore_index=True)
print(all_data)
print(all_data.columns)
all_data=all_data[['Region','Product ID', 'Brand']]
all_data.drop_duplicates(keep=False, inplace=True)
print(all_data)

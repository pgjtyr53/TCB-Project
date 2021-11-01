from io import StringIO
import pandas as pd
from azure.storage.blob import BlobServiceClient

#get data from blob

connection_string = 'DefaultEndpointsProtocol=https;AccountName=apdgecommerceadls;AccountKey=WT2xSWQHrna8YTuLy9QwjFnM7H2vMP8XiURNynDTis93osXlgIfF5COSh/fS8aovx/Pbr3xAqDYQO7JSkQsRmw==;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
#blob_client = blob_service_client.get_blob_client(container="ebr-dev", blob="ShopDashboard/ID_2019-01-01.xlsx")
container_name="ebr-dev"
container_client=blob_service_client.get_container_client(container_name)
blob_list = container_client.list_blobs(name_starts_with="ShopDashboard/")
all_data_blob = pd.DataFrame()
for blob in blob_list:
    if ".xlsx" in blob.name:
        blob_client = blob_service_client.get_blob_client(container="ebr-dev", blob=blob.name)
        blob_file = blob_client.download_blob().content_as_text(encoding=None)
        df = pd.read_excel(blob_file,sheet_name="Product Ranking")
        #Add file path as flow variable to extract Country and Date from file name
        #Extract Country from File name
        df["Market"]=blob.name.rsplit('\\', 2)[-1].rsplit('.', 2)[-2].rsplit('_', 7)[0].rsplit('/')[-1]
        # Extract Date from File name
        df["Date"]=blob.name.rsplit('\\', 2)[-1].rsplit('.', 2)[-2].rsplit('_', 7)[-1].rsplit('/')[-1]
        #convert String to Date & Time
        df["Date"]=pd.to_datetime(df["Date"])




        all_data_blob = all_data_blob.append(df, ignore_index=True)


#get all productID-Brand
path = r"productID-Brand"
import numpy as np
import pandas as pd
import glob
all_data_productid_brand_mapping = pd.DataFrame()
for f in glob.glob(path+"\*.xlsx",recursive=True):
    print(f)
    df = pd.read_excel(f,sheet_name="Product Performance Item Level")
    all_data_productid_brand_mapping = all_data_productid_brand_mapping.append(df, ignore_index=True)

all_data_productid_brand_mapping=all_data_productid_brand_mapping[['Region','Product ID', 'Brand']]
all_data_productid_brand_mapping=all_data_productid_brand_mapping.drop_duplicates(subset=['Region','Product ID', 'Brand'], keep="last").reset_index(drop=True)

df2=pd.merge(all_data_blob,all_data_productid_brand_mapping, left_on=["Market","Product ID"] ,right_on=["Region","Product ID"],how='left')

df2["Product Name1"]=df2["Product Name"].astype(str).str.lower()
#blanks, Other brands, Unspecified
df2["Product Name1"]= df2["Product Name1"].str.split(r'dan').str.get(0)
df2["Product Name1"]= df2["Product Name1"].str.split(r'\(free').str.get(0)


df2['Brand']= np.where('oral-b' in df2['Product Name1'], "Oral-B", df2['Brand'])
df2['Brand']= np.where('braun' in df2['Product Name1'], "Braun", df2['Brand'])
df2['Brand']= np.where('series 8' in df2['Product Name1'], "Braun", df2['Brand'])

print("All lowercase")

brand_product_name=pd.read_excel("KNIME Brand Dictionary for Shopee Unspecified.xlsx")
brand_product_name["Product Name"] = brand_product_name["Product Name"].astype(str).str.lower()



# print('Remove hyphens')
# data["RepairName"] = data["RepairName"].str.replace('-', '')
#
# print('Remove brackets')
# data["RepairName"] = data["RepairName"].str.replace('(', '')
# data["RepairName"] = data["RepairName"].str.replace(')', '')

df2['Brand2'] = [
    next((c for c, k in brand_product_name.values if k in s), None) for s in df2['Product Name1']]


df2.to_csv("final.csv")
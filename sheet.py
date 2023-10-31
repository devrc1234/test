#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from google.cloud import bigquery

import pandas as pd
import time

import os
cred=r"C:\Users\Ustraa\Documents/Python Scripts/ustraa-reporting-f7efbf420afc.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred
client=bigquery.Client(project="ustraa-reporting")





query=""" SELECT orderdate,SKU,SKUDesc,sum(OrderQty) as orderqty FROM `ustraa-reporting.ORDERDETAILEXPORT.Orderdetailexport` WHERE Status not in ("Cancelled","cancelled") AND OrderType IN ("Prepaid","COD","MobiQuik Wallet") and orderdate between "2023-09-01" and "2023-09-30" and ExternOrderNo like "200%" group by 1,2,3 order by 1,4 asc """

result=client.query(query).result()

rows=list(result)

df=pd.DataFrame(data=[list(row.values()) for row in rows],columns=list(rows[0].keys()))

df.head()

#upload the data to bigquery

job_config=bigquery.LoadJobConfig(
    autodetect=True,
    #WRITE_APPEND, #WRITE_TRUNCATE
    write_disposition='WRITE_APPEND'
)

target_table="ustraa-reporting.DISPATCHREPORT.sku_unit"
job=client.load_table_from_dataframe(df,target_table,job_config=job_config)
while job.state!='DONE':
    time.sleep(2)
    job.reload()
print(job.result())  
    


# In[ ]:





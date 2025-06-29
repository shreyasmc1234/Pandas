#handling a big file
#Your getting a large file which is loading to s3, how will u handle it
  use chunk in pandas
  use pyspark

import pandas as pd

chunk_size=100000
chunk_no=1

df=pd.read_csv("D:\\data\\big_file.csv",chunksize=chunk_size)
for i in df:
    i.to_parquet(f"D:\\data\\chunk{chunk_no}",index=False)
    chunk_no+=1

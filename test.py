import pandas as pd

df = pd.read_parquet("./Saved/FLAG_TBL_20250516T095800.parquet")
print(df)
print(df.info())
df.to_parquet("test.parquet", engine="pyarrow", coerce_timestamps="ms")

print(pd.read_parquet("./test.parquet"))
print(pd.read_parquet("./test.parquet")["UPDATE_TIME"].info())

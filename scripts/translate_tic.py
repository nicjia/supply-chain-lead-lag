import pandas as pd
import numpy as np

df = pd.read_csv("data/transactions.csv")

# parse dates (used for "most recent" logic)
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["srcdate"] = pd.to_datetime(df["srcdate"], errors="coerce")

# Build mapping from customer_gvkey -> customer_tic using most recent row per gvkey
cust = df.dropna(subset=["customer_gvkey", "customer_tic"]).copy()
cust["customer_gvkey"] = cust["customer_gvkey"].astype(int)

# choose "most recent" by srcdate first, then date
cust = cust.sort_values(["customer_gvkey", "srcdate", "date"])
gvkey_to_tic = cust.groupby("customer_gvkey")["customer_tic"].last().to_dict()

# Fill supplier_tic by mapping supplier_gvkey through the learned dict
df["supplier_gvkey_int"] = pd.to_numeric(df["supplier_gvkey"], errors="coerce").astype("Int64")
df["supplier_tic_fixed"] = df["supplier_gvkey_int"].map(gvkey_to_tic)

# If nothing found, keep NA (pandas will store as <NA>)
# If you want literal string "NA" instead:
# df["supplier_tic_fixed"] = df["supplier_tic_fixed"].fillna("NA")

# Optionally overwrite the bad supplier_tic column
df["supplier_tic"] = df["supplier_tic_fixed"]
df = df.drop(columns=["supplier_tic_fixed", "supplier_gvkey_int"])

df.to_csv("data/transaction_fixed.csv", index=False)
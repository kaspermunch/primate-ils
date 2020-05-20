
import pandas as pd
import sys

_, file_name = sys.argv

print(pd.read_hdf(file_name).head())
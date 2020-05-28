
import sys
import pandas as pd

_, *input_files, output_file = sys.argv

df_list = []
for input_file in input_files:
    df_list.append(pd.read_csv(input_file))

#df_list = [pd.read_hdf(f) for f in input_files]

pd.concat(df_list).to_csv(output_file, index=False)


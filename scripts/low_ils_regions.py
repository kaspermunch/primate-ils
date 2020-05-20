import sys
import pandas as pd
from chromwindow import window

_, input_file_name, low_ils_output_file_name = sys.argv

df = pd.read_hdf(input_file_name)

df['prop_ils'] = df[['V2', 'V3']].sum(axis=1) / df[['V0', 'V1', 'V2', 'V3']].sum(axis=1)
df['is_low'] = df.prop_ils <= df.prop_ils.mean() * 0.2
df['segment'] = (df.is_low != df.is_low.shift()).cumsum()

def start_end(df):
    return pd.Series(dict(start=df.start.min(), end=df.end.max()))

df = (df
        .groupby(['chrom', 'analysis', 'segment', 'is_low'])
        .apply(start_end)
        .reset_index()        
        .loc[lambda df: (df.is_low == True) & (df.end - df.start > 500000)]
        .drop(columns=['segment', 'is_low'])
    )

df.to_hdf(low_ils_output_file_name, 'df', mode='w', format='table')

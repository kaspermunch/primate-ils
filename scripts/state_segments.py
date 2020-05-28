
import sys, os
import pandas as pd

_, input_file_name, output_file_name = sys.argv

sp1, sp2, sp3, outgr, _, chrom = os.path.splitext(os.path.basename(input_file_name))[0].split('_')

analysis = '_'.join([sp1, sp2, sp3, outgr])
df = pd.read_hdf(input_file_name)

# remove coordinates that are undefined in human
df = df.loc[df.Homo_sapiens != -1]

# find MAP state
df['state'] = df[['V0', 'V1', 'V2', 'V3']].idxmax(axis=1)

# label each pos with the segment it belongs to
df['segment'] = ((df.state != df.state.shift()) | (df.Homo_sapiens - df.Homo_sapiens.shift() != 1)).cumsum()

def segment_coordinates(x):
    return pd.Series(dict(start=x.Homo_sapiens.min(), end=x.Homo_sapiens.max()))

# find min and max of postions in segment
df = df.groupby(['segment', 'state']).apply(segment_coordinates).reset_index()

df.drop(columns=['segment'], inplace=True)

df['chrom'] = chrom
df['analysis'] = analysis
# df['species1'] = sp1
# df['species2'] = sp2
# df['species3'] = sp3
# df['outgroup'] = outgr

# df['start'] = pd.to_numeric(df.start, downcast='unsigned')
# df['end'] = pd.to_numeric(df.end, downcast='unsigned')

df.to_hdf(output_file_name, 'df', mode='w', format='table')

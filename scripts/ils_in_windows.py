import sys
import pandas as pd
from chromwindow import window
import scipy.stats

_, input_file_name, window_output_file_name = sys.argv

# compute total segment length of each state:
@window(size=100000, fill='hg38')
def segment_total(df):
    return (df.end - df.start).sum()

df = pd.read_hdf(input_file_name)
window_df = (df
    .groupby(['chrom', 'state', 'analysis'])
    .apply(segment_total)
    .reset_index(drop=True, level=-1)
    .reset_index()
)
window_df = (pd
    .pivot_table(window_df, 
                index=['chrom', 'start', 'end', 'analysis'],
                columns='state', values='segment_total')
    .reset_index()
    )
# df['propils'] = df[['V2', 'V3']].sum(axis=1) / df[['V0', 'V1', 'V2', 'V3']].sum(axis=1)
# df['propils_z_score'] = scipy.stats.zscore(df.propils)
# df['ilsskew'] = np.log10(df.V2 / df.V3)
# df['ilsskew_z_score'] = scipy.stats.zscore(df.ilsskew)


window_df.to_hdf(window_output_file_name, 'df', mode='w', format='table')

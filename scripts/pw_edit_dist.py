
def distance(sr1, sr2, global_ils_prop):
    return (sr1 == sr2).astype(int) * global_ils_prop


# load all data frames for all chromosomes and concat them
df = 

trios = df.columns.values
# global ils values for each trio
global_ils_prop = # in the column order

# maybe only do combinations including one megabase in each run of the script
combinations = 

combinations_df = pd.DataFrame([df.loc[c, :].apply(distance) for c in combinations], index=combinations)
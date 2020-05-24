
import sys
import pandas as pd


def optimize_dataframe(df, down_int='integer'):
    # down_int can also be 'unsigned'
    
    converted_df = pd.DataFrame()

    floats_optim = (df
                    .select_dtypes(include=['float'])
                    .apply(pd.to_numeric,downcast='float')
                   )
    converted_df[floats_optim.columns] = floats_optim

    ints_optim = (df
                    .select_dtypes(include=['int'])
                    .apply(pd.to_numeric,downcast=down_int)
                   )
    converted_df[ints_optim.columns] = ints_optim

    for col in df.select_dtypes(include=['object']).columns:
        num_unique_values = len(df[col].unique())
        num_total_values = len(df[col])
        if num_unique_values / num_total_values < 0.5:
            converted_df[col] = df[col].astype('category')
        else:
            converted_df[col] = df[col]

    unchanged_cols = df.columns[~df.columns.isin(converted_df.columns)]
    converted_df[unchanged_cols] = df[unchanged_cols]

    # keep columns order
    converted_df = converted_df[df.columns]      
            
    return converted_df

_, *input_files, output_file = sys.argv

df_list = []
for input_file in input_files:
    df_list.append(pd.read_csv(input_file))

#df_list = [pd.read_hdf(f) for f in input_files]

optimize_dataframe((pd.concat(df_list)), down_int='unsigned').to_hdf(output_file, 'df', format='table', mode='w')


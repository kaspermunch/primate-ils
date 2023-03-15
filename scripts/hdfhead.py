
import pandas as pd
import sys

import argparse

pd.set_option('display.max_rows', None)

parser = argparse.ArgumentParser()
parser.add_argument('--head', type=int, default=10)
parser.add_argument('--start', type=int, default=0)
#parser.add_argument('--all', action='store_true')
parser.add_argument('file_name', type=str)
args = parser.parse_args()

#_, file_name = sys.argv

# if args.all:
#     print(pd.read_hdf(args.file_name))
# else:
#     print(pd.read_hdf(args.file_name).head(args.head))

print(pd.read_hdf(args.file_name).iloc[args.start:args.start+args.head])    
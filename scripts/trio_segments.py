import sys
import pandas as pd
import numpy as np
from pathlib import Path

# input_file_names = list(Path('steps/state_segments').glob(f'*_chr_22.h5'))
# output_file_name = 'gene_trees.h5'

# python scripts/trio_segments.py gene_trees.h5 steps/state_segments/*_chr_22.h5

_, output_file_name, *input_file_names = sys.argv

breaks = set()
segments = dict()
for path in input_file_names:
    df = pd.read_hdf(path)
    breaks.update(df.start.tolist())
    breaks.update(df.end.tolist())

    trio = df.analysis[0]
    states = [int(x[1]) for x in df.state]
    segments[trio] = list(zip(df.start, df.end, states))

breaks = sorted(breaks)

trio_states = pd.DataFrame()
trio_states['start'] = breaks
trio_states['end'] = trio_states.start.shift(-1)

# the last break is and end not a start
trio_states.drop(trio_states.tail(1).index,inplace=True)
breaks = breaks[:-1]

# trio_states['start'] = trio_states.start.astype('uint32')
# trio_states['end'] = trio_states.end.astype('uint32')
trio_states.set_index(['start', 'end'], inplace=True)

breaks = list(zip(breaks, [float('inf')]*len(breaks), [None]*len(breaks)))
for trio in segments:
    sp1, sp2, sp3, sp4 = trio.split('_')

    # genealogies = [f'(({sp1},{sp2}),{sp3})', f'(({sp1},{sp2}),{sp3})',
    #             f'(({sp1},{sp3}),{sp2})', f'(({sp2},{sp1}),{sp1})']

    lst = list()
    prev_state, prev_start, prev_end = None, -1, -1
    for start, end, state in sorted(breaks + segments[trio]):
        # only unqiue starts (segment starts are also included in breaks but sort after these):
        if start == prev_start:
            continue

        # replace undefined states (labeled as 4) if its end is within the bouds of the trio interval
        if state is None:
            if start <= prev_end:
                state = prev_state
        else:
            prev_state = state
            prev_start = start
            prev_end = end

        if state is None:
            lst.append(np.nan)
        else:
            # lst.append(genealogies[state])
            lst.append(state)

    trio_states[trio] = pd.Categorical(lst)
    # trio_states[trio] = pd.Series(lst).astype('uint8')

trio_states.to_hdf(output_file_name, 'df', mode='w', format='table')


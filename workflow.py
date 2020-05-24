

import os, re
from pathlib import Path
import pandas as pd

from gwf import Workflow, AnonymousTarget
from gwf.workflow import collect

def modpath(p, parent=None, base=None, suffix=None):
    par, name = os.path.split(p)
    name_no_suffix, suf = os.path.splitext(name)
    if type(suffix) is str:
        suf = suffix
    if parent is not None:
        par = parent
    if base is not None:
        name_no_suffix = base

    new_path = os.path.join(par, name_no_suffix + suf)
    if type(suffix) is tuple:
        assert len(suffix) == 2
        new_path, nsubs = re.subn(r'{}$'.format(suffix[0]), suffix[1], new_path)
        assert nsubs == 1, nsubs
    return new_path

def state_segments(posterior_file):

    stepsdir = 'steps/state_segments'
    if not os.path.exists(stepsdir):
        os.makedirs(stepsdir)

    segment_file = modpath(posterior_file, parent=stepsdir, suffix='.h5')

    inputs = {'posterior_file': posterior_file}
    outputs = {'segment_file': segment_file}

    options = {'memory': '40g', 'walltime': '01:00:00'} 
    spec = f"""
    python scripts/state_segments.py {posterior_file} {segment_file}
    """
    return AnonymousTarget(inputs=inputs, outputs=outputs, options=options, spec=spec)

def ils_in_windows(segment_file):

    stepsdir = 'steps/ils_in_windows'
    if not os.path.exists(stepsdir):
        os.makedirs(stepsdir)

    window_file = modpath(segment_file, parent=stepsdir, suffix='.h5')

    inputs = {'segment_file': segment_file}
    outputs = {'window_file': window_file}

    options = {'memory': '8g', 'walltime': '01:00:00'} 
    spec = f"""
    python scripts/ils_in_windows.py {segment_file} {window_file}
    """
    return AnonymousTarget(inputs=inputs, outputs=outputs, options=options, spec=spec)


def low_ils_regions(window_file):

    stepsdir = 'steps/low_ils_regions'
    if not os.path.exists(stepsdir):
        os.makedirs(stepsdir)

    low_ils_file = modpath(window_file, parent=stepsdir, suffix='.csv')

    inputs = {'window_file': window_file}
    outputs = {'low_ils_file': low_ils_file}

    options = {'memory': '8g', 'walltime': '01:00:00'} 
    spec = f"""
    python scripts/low_ils_regions.py {window_file} {low_ils_file}
    """
    return AnonymousTarget(inputs=inputs, outputs=outputs, options=options, spec=spec)



gwf = Workflow(defaults={'account': 'Primategenomes'})

data_dir = '/home/kmt/Primategenomes/data/final_tables'
state_posterior_files = sorted(Path(data_dir).glob('**/*.HDF'))

# compute state segments
targets_state_segments = gwf.map(state_segments, state_posterior_files)

# compute ils in windows
targets_ils_in_windows = gwf.map(ils_in_windows, targets_state_segments.outputs)

stepsdir = 'steps/merge_ils_data'
if not os.path.exists(stepsdir):
    os.makedirs(stepsdir)

ils_window_files = collect(targets_ils_in_windows.outputs, ['window_file'])['window_files']
merged_ils_file = os.path.join(stepsdir, 'merged_ils_data.h5')
input_args = ' '.join(ils_window_files)
gwf.target('merge_ils_data', memory='36g', walltime='01:00:00', inputs=ils_window_files, outputs=[merged_ils_file]) << f"""
python scripts/merge_hdf_files.py {input_args} {merged_ils_file}
"""

# compute low ils regions
targets_low_ils_regions = gwf.map(low_ils_regions, targets_ils_in_windows.outputs)

stepsdir = 'steps/merge_low_data'
if not os.path.exists(stepsdir):
    os.makedirs(stepsdir)

low_ils_region_files = collect(targets_low_ils_regions.outputs, ['low_ils_file'])['low_ils_files']
merged_low_region_file = os.path.join(stepsdir, 'merged_low_ils_regions.csv')
input_args = ' '.join(low_ils_region_files)
gwf.target('merge_low_ils_regions', memory='36g', walltime='01:00:00', inputs=low_ils_region_files, outputs=[merged_low_region_file]) << f"""
python scripts/merge_csv_files.py {input_args} {merged_low_region_file}
"""






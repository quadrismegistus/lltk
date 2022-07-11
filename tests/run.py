import os
here=os.path.abspath(os.path.dirname(__file__))

files=[]
for fn in sorted(os.listdir(here)):
    if fn.startswith('test_') and fn.endswith('.ipynb'):
        files.append(os.path.join(here,fn))

from tqdm import tqdm
iterr=tqdm(files)
for fn in iterr:
    iterr.set_description(f'Running ipytest in: {os.path.basename(fn)}')
    # print(f'### {fn} ###')
    os.system(f'ipython {fn}')

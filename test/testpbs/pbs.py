# dummy pbs module
import os
fn = 'fake_pbs_constants.py'
absfn = os.path.join(os.path.dirname(__file__), fn)

execfile(absfn)


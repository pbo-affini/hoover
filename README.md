# hoover

Hoover is a python library allowing to clan a *sv file, field by field. Just give the file to clean, add header,
type of datas, transformations to apply to each field, and launch it ! Hoover can work in single process, or in multiprocess.

# Basic usage

<pre>from hoover import *

 h = Hoover(file_to_clean, separator, filetowrite, nb_process)
  h.add_header()
   h.add_type()
    h.add_transformation()
     h.launch_hoover()
     </pre>

# Contribute

All the contribution are accepted, please just launch the unit tests with py.test and verify they are good before commit.

# Amelioration

Some ameliorations can be done, I will try to do them:
- add the support of date, with easy transformation (using arrow for example)
- use cython to make the process faster
- use bash function to manipulate file (remove the first line, concatenate...)

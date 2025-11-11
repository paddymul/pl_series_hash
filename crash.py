import polars as pl
from pl_series_hash import crash

df_1 = pl.DataFrame({"u64": pl.Series([5, 3, 20], dtype=pl.UInt64)})

try:
    result_1 = df_1.select(hash_col=crash("u64"))
except Exception:
    print("caught exception")
    print("e")

print("after crash call")
    

"""
  shell output from this file.

  The exception can't be caught, it crashes hard
 % python crash.py 
thread caused non-unwinding panic. aborting.
zsh: abort      python crash.py
 %
"""

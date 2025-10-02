# pl_series_hash


pl_series_hash is a polars plugin to compute lightning fast hashes per series in [polars](https://pola.rs/)

This will be used by [buckaroo](https://github.com/paddymul/buckaroo) to enable summary stats caching.

# Basic implementation

This uses [twox-hash](https://github.com/shepmaster/twox-hash) a very performant hashing library.

For each series I first write out a type identifier.

For each element in a series I add the bytes, for strings I also write a `STRING_SEPERATOR` of `128u16` which isn't a valid UTF8 symbol and shouldn't ever appear.
For NANs/Nulls I write out `NAN_SEPERATOR` - `129u16` also an invalid unicode character.  

Next I write out the array position in bytes (u64)

All of this is then hashed.




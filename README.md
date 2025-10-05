# pl_series_hash


pl_series_hash is a polars plugin to compute lightning fast hashes per series in [polars](https://pola.rs/)

This will be used by [buckaroo](https://github.com/paddymul/buckaroo) to enable summary stats caching.

## using pl_series_hash

```python
>>> import polars as pl
>>> from pl_series_hash import hash_xx
>>> df = pl.DataFrame({"u64": pl.Series([5, 3, 20], dtype=pl.UInt64)})
>>> df.select(hash_col=hash_xx("u64"))
shape: (1, 1)
┌─────────────────────┐
│ hash_col            │
│ ---                 │
│ u64                 │
╞═════════════════════╡
│ 6142793559755377588 │
└─────────────────────┘
```

You can hash every column in a dataframe with the namespaced function.

```python
import pl_series_hash
df.select(pl.all().pl_series_hash.hash_xx())
```


## Installing pl_series_hash

```
pip install pl_series_hash
```

## properties of pl_series_hash

The same values in a different dtype will result in different different hash values.
The name of a column or struct part doesn't effect the hash values
The presence and position of nulls do affect the hash value

## Supported column types

The following polars Rust datatypes are supported

* Boolean
* UInt8
* UInt16
* UInt32
* UInt64
* Int8
* Int16
* Int32
* Int64
* Float32
* Float64
* String
* Date
* Datetime
* Duration
* Time
* Array
* Null
* Categorical
* Enum
* Struct

## Unsupported datatypes

### Planned
Binary
BinaryOffset
Int128 - planned, it's a compile/config option
Decimal - planned it's a compile/config option

### Not planned
Object - Summary stats on objects are useless and these columns rarely show up.  I will probably skip
List Complex nested type implementation, rarely used
DataType::Unknown #  have no idea what could be done with this in use
Null  - Currently implemented but I don't know the use case for this

## Basic implementation

This uses [twox-hash](https://github.com/shepmaster/twox-hash) a very performant hashing library.

For each series I first write out a type identifier.

For each element in a series I add the bytes, for strings I also write a `STRING_SEPERATOR` of `128u16` which isn't a valid UTF8 symbol and shouldn't ever appear.
For NANs/Nulls I write out `NAN_SEPERATOR` - `129u16` also an invalid unicode character.  

Next I write out the array position in bytes (u64)

All of this is then hashed.

Structs and arrays are hashed recursively - a vector of each constituent sub-series is hashed, then that vector is hashed.

## Further research


Articles pulled from the polars codebase
https://www.cockroachlabs.com/blog/vectorized-hash-joiner/
http://myeyesareblind.com/2017/02/06/Combine-hash-values/

If you want elementwise hashing take a look at [polars-hash](https://github.com/ion-elgreco/polars-hash) It is a much more mature plugin that allows you to choose different hashing algorithms.



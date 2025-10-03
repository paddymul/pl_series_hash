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
* DataType::Int64
* DataType::Int32
* DataType::Int16
* DataType::Int8
* DataType::UInt64
* DataType::UInt32
* DataType::UInt16
* DataType::UInt8
* DataType::Float64
* DataType::Float32
* DataType::String
* DataType::Boolean
* DataType::Datetime(_, _)
* DataType::Duration(_)
* DataType::Time
* DataType::Date
* DataType::Struct(_)
* DataType::Array(_,_)

## Nearterm planned datatypes
enums, categoricals, List, Decimal, 

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



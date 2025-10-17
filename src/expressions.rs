#![allow(clippy::unused_unit)]
use std::hash::Hasher;

use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use twox_hash::XxHash64;

const SEED: u64 = 1234;
fn hardcode_bytes(i: u8) -> [u8; 1] {
    i.to_le_bytes()
}

//according to my tests these are both unrepresentable utf8
// characters, which means they can't come up in a string which should prohibit hash collisions.
const STRING_SEPERATOR: &[u8; 2] = &128u16.to_le_bytes();
const NAN_SEPERATOR: &[u8; 2] = &129u16.to_le_bytes();

macro_rules! hash_func {
    ($a:ident, $b:ty, $type_num:expr) => {
        fn $a(cb: $b) -> u64 {
            let mut hasher = XxHash64::with_seed(SEED);
            hasher.write(&hardcode_bytes($type_num));
            let mut count: u64 = 0;
            for val in cb.iter() {
                count += 1;
                match val {
                    Some(val) => hasher.write(&val.to_le_bytes()),
                    _ => {
                        hasher.write(NAN_SEPERATOR);
                    },
                }
            hasher.write(&count.to_le_bytes());
            }
            hasher.finish()
        }
    };
}

// non macro implementation for reference
// it's of course easier to reason about this in a non macro context

// check macro expansion with
// cargo rustc --profile=check -- -Zunpretty=expanded
// fn hash_i64_chunked(cb: &Int64Chunked) -> u64 {
//     let mut hasher = XxHash64::with_seed(SEED);
//     hasher.write(&hardcode_bytes(1));
//     let mut count: u64 = 0;
//     for val in cb.iter() {
//         count += 1;
//         match val {
//             Some(val) => { hasher.write(&val.to_le_bytes()) }
//             _ => { hasher.write(NAN_SEPERATOR); }
//         }
//         hasher.write(&count.to_le_bytes());
//     }
//     hasher.finish()
// }

hash_func!(hash_i64_chunked, &Int64Chunked, 1);
hash_func!(hash_i32_chunked, &Int32Chunked, 2);
hash_func!(hash_i16_chunked, &Int16Chunked, 3);
hash_func!(hash_i8_chunked, &Int8Chunked, 4);
hash_func!(hash_u64_chunked, &UInt64Chunked, 5);
hash_func!(hash_u32_chunked, &UInt32Chunked, 6);
hash_func!(hash_u16_chunked, &UInt16Chunked, 7);
hash_func!(hash_u8_chunked, &UInt8Chunked, 8);
hash_func!(hash_f64_chunked, &Float64Chunked, 9);
hash_func!(hash_f32_chunked, &Float32Chunked, 10);
hash_func!(hash_datetime_chunked, &DatetimeChunked, 13);
hash_func!(hash_duration_chunked, &DurationChunked, 14);
hash_func!(hash_time_chunked, &TimeChunked, 15);
hash_func!(hash_date_chunked, &DateChunked, 16);
//  #[cfg(feature = "dtype-decimal")]
// hash_func!(hash_decimal_chunked, &DecimalChunked, 17);

fn hash_string_chunked(cb: &StringChunked) -> u64 {
    let mut hasher = XxHash64::with_seed(SEED);
    hasher.write(&hardcode_bytes(11));
    let mut count: u64 = 0;
    for val in cb.iter() {
        count += 1;
        match val {
            Some(val) => {
                hasher.write(val.as_bytes());
            },
            _ => hasher.write(NAN_SEPERATOR)
        }
        hasher.write(STRING_SEPERATOR);
    }
    hasher.write(&count.to_le_bytes());
    //find_invalid_utf8();
    hasher.finish()
}

fn hash_bool_chunked(cb: &BooleanChunked) -> u64 {
    let mut hasher = XxHash64::with_seed(SEED);
    hasher.write(&hardcode_bytes(12));
    let mut count: u64 = 0;
    for val in cb.iter() {
        count += 1;
        match val {
            Some(val) => {
                if val {
                    hasher.write(&(1u8).to_le_bytes())
                } else {
                    hasher.write(&(0u8).to_le_bytes())
                }
            },
            _ => hasher.write(NAN_SEPERATOR)
        }
        hasher.write(&count.to_le_bytes());
    }
    hasher.finish()
}

fn hash_struct_series(cb: &StructChunked) -> Option<u64> {
    let mut hasher = XxHash64::with_seed(SEED);
    hasher.write(&hardcode_bytes(18));
    let mut count: u64 = 0;
    for ser in cb.fields_as_series() {
        let maybe_hash = hash_single_series(&ser);
        count += 1;
        match maybe_hash {
            Some(maybe_hash) => {
                hasher.write(&maybe_hash.to_le_bytes());
                hasher.write(&count.to_le_bytes());
            },
            _ => return None
        }
    }
    Some(hasher.finish())
}

fn hash_array_series(cb: &ArrayChunked) -> Option<u64> {
    let mut hasher = XxHash64::with_seed(SEED);
    hasher.write(&hardcode_bytes(19));
    let mut count: u64 = 0;
    let num_columns = cb.len();
    for i in 0..num_columns {
        let ser = cb.get_as_series(i)?;
        let maybe_hash = hash_single_series(&ser);
        count += 1;
        match maybe_hash {
            Some(maybe_hash) => {
                hasher.write(&maybe_hash.to_le_bytes());
                hasher.write(&count.to_le_bytes());
            },
            _ => return None
        }
    }
    Some(hasher.finish())
}

fn hash_categorical_chunked(cb: &CategoricalChunked) -> u64 {
    let mut hasher = XxHash64::with_seed(SEED);
    hasher.write(&hardcode_bytes(20));
    let mut count: u64 = 0;
    for val in cb.iter_str() {
        count += 1;
        match val {
            Some(val) => {
                hasher.write(val.as_bytes());
                hasher.write(STRING_SEPERATOR);
                hasher.write(&count.to_le_bytes());
            },
            _ => hasher.write(NAN_SEPERATOR)
        }
    }
    hasher.finish()
}

fn hash_null_series(_s:&Series) -> Option<u64> {
    // fine, i guess, maybe I should return the length of the series here
    // it all seems silly

    Some(0)
}

fn hash_single_series(s:&Series) -> Option<u64> {
    // this match statement was reorderd to coincide with
    // https://docs.rs/polars/0.49.1/polars/datatypes/enum.DataType.html
    match s.dtype() {
        DataType::Boolean => Some(hash_bool_chunked(s.bool().ok()?)),

        DataType::UInt8 => Some(hash_u8_chunked(s.u8().ok()?)),
        DataType::UInt16 => Some(hash_u16_chunked(s.u16().ok()?)),
        DataType::UInt32 => Some(hash_u32_chunked(s.u32().ok()?)),
        DataType::UInt64 => Some(hash_u64_chunked(s.u64().ok()?)),

        DataType::Int8 => Some(hash_i8_chunked(s.i8().ok()?)),
        DataType::Int16 => Some(hash_i16_chunked(s.i16().ok()?)),
        DataType::Int32 => Some(hash_i32_chunked(s.i32().ok()?)),
        DataType::Int64 => Some(hash_i64_chunked(s.i64().ok()?)),
        //DataType::Int128 => Some(hash_i128_chunked(s.i128().ok()?)),

        DataType::Float32 => Some(hash_f32_chunked(s.f32().ok()?)),
        DataType::Float64 => Some(hash_f64_chunked(s.f64().ok()?)),

        // #[cfg(feature = "dtype-decimal")]
        // DataType::Decimal => Some(hash_decimal_chunked(s.decimal().ok()?)),

        DataType::String => Some(hash_string_chunked(s.str().ok()?)),

        //Binary
        //BinaryOffset

        DataType::Date => Some(hash_date_chunked(s.date().ok()?)),
        DataType::Datetime(_, _) => Some(hash_datetime_chunked(s.datetime().ok()?)),
        DataType::Duration(_) => Some(hash_duration_chunked(s.duration().ok()?)),
        DataType::Time => Some(hash_time_chunked(s.time().ok()?)),

        DataType::Array(_,_) => hash_array_series(s.array().ok()?),
        //DataType::List => Some(hash_list_chunked(s.list().ok()?)),

        //Object  skipped
        DataType::Null => hash_null_series(s),
        DataType::Categorical(_, _) => Some(hash_categorical_chunked(s.categorical().ok()?)),
        DataType::Enum(_, _) => Some(hash_categorical_chunked(s.categorical().ok()?)),
        DataType::Struct(_) => hash_struct_series(s.struct_().ok()?),
        //once again why?
        //DataType::Unknown(_) => None,
        _ => None,
    }
}

#[polars_expr(output_type=UInt64)]
fn hash_series(inputs: &[Series]) -> PolarsResult<Series> {
    let chunks = &inputs[0];
    let maybe_hash = hash_single_series(chunks);
    match maybe_hash {
        Some(maybe_hash) => Ok(Series::new("hash".into(), vec![maybe_hash])),
        _ => Err(PolarsError::ComputeError("couldn't compute hash for column type".into()))
    }
}

#[polars_expr(output_type=UInt64)]
fn crash_period(_inputs: &[Series]) -> PolarsResult<Series> {
  // Causes a segmentation fault by dereferencing a null pointer (undefined behavior).
    let p: *const i32 = std::ptr::null();
    unsafe {
        println!("{}", *p);
    }
    Ok(Series::new("hash".into(), vec![0u64]))
}


/*
fn demo<T, const N: usize>(v: Vec<T>) -> [T; N] {
    v.try_into()
        .unwrap_or_else(|v: Vec<T>| panic!("Expected a Vec of length {} but it was {}", N, v.len()))
}

fn vec_loop(input: &[u8]) -> [u8; 2]{
    let mut output = Vec::new();
    for element in input {
        output.push(*element);
    }
    demo(output)
}

fn is_invalid_utf8(sep:u16) -> bool {
    let sparkle_heart = vec_loop(&sep.to_le_bytes());
    let _sparkle_heart2 = str::from_utf8(&sparkle_heart);
    match _sparkle_heart2 {
        Ok(_sparkle_heart2) => {
            return false; },
        _ => { return true; }
    }
}

fn find_invalid_utf8() -> u64 {
    // 128u16 is invalid u16
    // so is 129u16
    //for i in 0u16..5000u16 {
    for i in 129u16..5000u16 {
        if is_invalid_utf8(i) {
            println!("{}", i);
            return 2u64;
        }
    }
    return 1u64;
}
*/

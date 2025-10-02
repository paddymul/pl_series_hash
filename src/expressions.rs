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

fn hash_string_chunked(cb: &StringChunked) -> u64 {
    let mut hasher = XxHash64::with_seed(SEED);
    hasher.write(&hardcode_bytes(11));
    let mut count: u64 = 0;
    for val in cb.iter() {
        count += 1;
        match val {
            Some(val) => {
                hasher.write(val.as_bytes());
                hasher.write(STRING_SEPERATOR);
                hasher.write(&count.to_le_bytes());
            },
            _ => {
                hasher.write(NAN_SEPERATOR);
            },
        }
    }
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
            _ => {
                hasher.write(NAN_SEPERATOR);
            },
        }
        hasher.write(&count.to_le_bytes());
    }
    hasher.finish()
}

#[polars_expr(output_type=UInt64)]
fn hash_series(inputs: &[Series]) -> PolarsResult<Series> {
    let chunks = &inputs[0];

    if let Ok(ichunks) = chunks.i64() {
        let hash = hash_i64_chunked(ichunks);
        return Ok(Series::new("hash".into(), vec![hash]));
    }
    if let Ok(ichunks) = chunks.i32() {
        let hash = hash_i32_chunked(ichunks);
        return Ok(Series::new("hash".into(), vec![hash]));
    }
    if let Ok(ichunks) = chunks.i16() {
        let hash = hash_i16_chunked(ichunks);
        return Ok(Series::new("hash".into(), vec![hash]));
    }
    if let Ok(ichunks) = chunks.i8() {
        let hash = hash_i8_chunked(ichunks);
        return Ok(Series::new("hash".into(), vec![hash]));
    }
    if let Ok(ichunks) = chunks.u64() {
        let hash = hash_u64_chunked(ichunks);
        return Ok(Series::new("hash".into(), vec![hash]));
    }
    if let Ok(ichunks) = chunks.u32() {
        let hash = hash_u32_chunked(ichunks);
        return Ok(Series::new("hash".into(), vec![hash]));
    }
    if let Ok(ichunks) = chunks.u16() {
        let hash = hash_u16_chunked(ichunks);
        return Ok(Series::new("hash".into(), vec![hash]));
    }
    if let Ok(ichunks) = chunks.u8() {
        let hash = hash_u8_chunked(ichunks);
        return Ok(Series::new("hash".into(), vec![hash]));
    }
    if let Ok(ichunks) = chunks.f64() {
        let hash = hash_f64_chunked(ichunks);
        return Ok(Series::new("hash".into(), vec![hash]));
    }
    if let Ok(ichunks) = chunks.f32() {
        let hash = hash_f32_chunked(ichunks);
        return Ok(Series::new("hash".into(), vec![hash]));
    }
    if let Ok(ichunks) = chunks.str() {
        let hash = hash_string_chunked(ichunks);
        return Ok(Series::new("hash".into(), vec![hash]));
    }
    if let Ok(ichunks) = chunks.bool() {
        let hash = hash_bool_chunked(ichunks);
        return Ok(Series::new("hash".into(), vec![hash]));
    }

    Err(PolarsError::ComputeError(
        "couldn't compute hash for column type".into(),
    ))
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

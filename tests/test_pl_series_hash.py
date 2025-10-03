import polars as pl
from pl_series_hash import hash_xx


U64_5_3_20_HASH = 6142793559755377588



def test_hash_u64():
    """
      This test is written without any syntatic sugar so it is easy to understand what is going on
    """
    df_1 = pl.DataFrame({"u64": pl.Series([5, 3, 20], dtype=pl.UInt64)})

    result_1 = df_1.select(hash_col=hash_xx("u64"))

    expected_df1 = pl.DataFrame({"hash_col": [U64_5_3_20_HASH]})

    assert result_1.equals(expected_df1)

def hash_sequence(vals, dtype=None) -> int:
    if dtype:
        ser = pl.Series(vals, dtype=dtype)
    else:
        ser = pl.Series(vals)

    df = pl.DataFrame({'raw':ser})
    return df.select(hash_col=hash_xx('raw'))['hash_col'][0]

I64_5_3_20_HASH = 12770448285348326088

    
def test_hash_i64_same_u64():
    # verify that the exact same values as a different type result in a different hash
    actual = hash_sequence([5, 3, 20], pl.Int64)
    assert actual == I64_5_3_20_HASH
    assert not U64_5_3_20_HASH == I64_5_3_20_HASH


def test_hash_i64():
    # explicityly tests negative values
    assert 17812342556943928683 == hash_sequence([-5, 3, 20], dtype=pl.Int64)


def test_hash_u64_two_chunks():
    s = pl.Series([5, 3], dtype=pl.UInt64)

    s_two_chunks = s.append(pl.Series([20], dtype=pl.UInt64))

    assert len(s_two_chunks.get_chunks()) == 2

    df_1 = pl.DataFrame({"u64": s_two_chunks})

    result_1 = df_1.select(hash_col=hash_xx("u64"))

    expected_df1 = pl.DataFrame({"hash_col": [U64_5_3_20_HASH]})

    assert result_1.equals(expected_df1)

    assert len(df_1["u64"].get_chunks()) == 2


def test_hash_i32():
    assert 8094616336673590623 = hash_sequence([-5, 3, 20], dtype=pl.Int32)


def test_hash_u64_nan():
    result_1 = hash_sequence([5, 3, None, 20], dtype=pl.UInt64)

    hash_1 = 6959525719124025770
    assert result_1 == hash_1
    assert not hash_1 == U64_5_3_20_HASH  # make sure adding a nan changes the hash

    result_2 = hash_sequence([5, 3, 20, None], dtype=pl.UInt64)
    hash_2 = 11887503197445608313
    assert result_2 == hash_2
    assert (
        not hash_2 == hash_1
    )

    # make sure changing the position of the nan changes the result
    assert not hash_2 == U64_5_3_20_HASH


def test_hash_null_str():
    result_1 = hash_sequence(["this", None, "is", "not", "pig", "latin"])
    hash_1 = 16789198962064671277
    assert hash_1 == result_1

    result_2 = hash_sequence(["this", "is", "not", "pig", "latin"])

    hash_2 = 9724091221529583951
    assert hash_2 == result_2
    assert not hash_1 == hash_2


def test_hash_str():
    """
    Basic test of the string hashing
    """
    result_1 = hash_sequence(["this", "is", "not"])
    hash_1 = 5371592560750954784
    assert hash_1 == result_1

    # Note the concatenation of this-is
    result_2 = hash_sequence(["thisis", "not", "pig", "latin"])

    hash_2 = 13865378224932904863
    assert hash_2 == result_2
    assert not hash_1 == hash_2

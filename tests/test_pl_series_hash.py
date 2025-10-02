import polars as pl
from pl_series_hash import hash_xx


U64_5_3_20_HASH = 6142793559755377588
I64_5_3_20_HASH = 12770448285348326088


def test_hash_u64():
    df_1 = pl.DataFrame({"u64": pl.Series([5, 3, 20], dtype=pl.UInt64)})

    result_1 = df_1.select(hash_col=hash_xx("u64"))

    expected_df1 = pl.DataFrame({"hash_col": [U64_5_3_20_HASH]})

    assert result_1.equals(expected_df1)


def test_hash_i64_same_u64():
    # verify that the exact same values as a different type result in a different hash

    df_1 = pl.DataFrame({"i64": pl.Series([5, 3, 20], dtype=pl.Int64)})

    result_1 = df_1.select(hash_col=hash_xx("i64"))

    expected_df1 = pl.DataFrame({"hash_col": [I64_5_3_20_HASH]})

    assert result_1.equals(expected_df1)
    assert not U64_5_3_20_HASH == I64_5_3_20_HASH


def test_hash_i64():
    # explicityly tests negative values
    df_1 = pl.DataFrame({"i64": pl.Series([-5, 3, 20], dtype=pl.Int64)})

    result_1 = df_1.select(hash_col=hash_xx("i64"))

    hash_1 = 17812342556943928683
    expected_df1 = pl.DataFrame({"hash_col": [hash_1]})

    assert result_1.equals(expected_df1)


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
    df_1 = pl.DataFrame({"i32": pl.Series([-5, 3, 20], dtype=pl.Int32)})

    result_1 = df_1.select(hash_col=hash_xx("i32"))

    hash_1 = 8094616336673590623
    expected_df1 = pl.DataFrame({"hash_col": [hash_1]})

    assert result_1.equals(expected_df1)


def test_hash_u64_nan():
    df_1 = pl.DataFrame({"u64": pl.Series([5, 3, None, 20], dtype=pl.UInt64)})

    result_1 = df_1.select(hash_col=hash_xx("u64"))

    hash_1 = 6959525719124025770
    expected_df1 = pl.DataFrame({"hash_col": [hash_1]})

    assert result_1.equals(expected_df1)
    assert not hash_1 == U64_5_3_20_HASH  # make sure adding a nan changes the hash

    df_2 = pl.DataFrame({"u64": pl.Series([5, 3, 20, None], dtype=pl.UInt64)})

    result_2 = df_2.select(hash_col=hash_xx("u64"))

    hash_2 = 11887503197445608313
    expected_df2 = pl.DataFrame({"hash_col": [hash_2]})

    assert result_2.equals(expected_df2)
    assert (
        not hash_2 == hash_1
    )  # make sure changing the position of the nan changes the result
    assert not hash_2 == U64_5_3_20_HASH


def test_hash_null_str():
    df_1 = pl.DataFrame(
        {
            "english": ["this", None, "is", "not", "pig", "latin"],
        }
    )
    result_1 = df_1.select(hash_col=hash_xx("english"))

    hash_1 = 16789198962064671277
    expected_df1 = pl.DataFrame({"hash_col": [hash_1]})

    assert result_1.equals(expected_df1)

    df_2 = pl.DataFrame(
        {
            # Note the concatenation of this-is
            "english": ["this", "is", "not", "pig", "latin"],
        }
    )
    result_2 = df_2.select(hash_col=hash_xx("english"))

    hash_2 = 9724091221529583951
    expected_df2 = pl.DataFrame({"hash_col": [hash_2]})

    assert result_2.equals(expected_df2)

    assert not hash_1 == hash_2


def test_hash_str():
    """
    Basic test of the string hashing
    """
    df_1 = pl.DataFrame({"english": ["this", "is", "not"]})
    result_1 = df_1.select(hash_col=hash_xx("english"))

    hash_1 = 5371592560750954784
    expected_df1 = pl.DataFrame({"hash_col": [hash_1]})

    assert result_1.equals(expected_df1)

    # Note the concatenation of this-is
    df_2 = pl.DataFrame(
        {
            "english": ["thisis", "not", "pig", "latin"],
        }
    )
    result_2 = df_2.select(hash_col=hash_xx("english"))

    hash_2 = 13865378224932904863
    expected_df2 = pl.DataFrame({"hash_col": [hash_2]})

    assert result_2.equals(expected_df2)

    assert not hash_1 == hash_2

def test_hash_str2():
    df_1 = pl.DataFrame({"english": ["this", "is"]})
    result_1 = df_1.select(hash_col=hash_xx("english"))

    hash_1 = 10264270744541082640
    expected_df1 = pl.DataFrame({"hash_col": [hash_1]})

    assert result_1.equals(expected_df1)

    # Note the concatenation of this-is
    df_2 = pl.DataFrame({"english": ["this|is"]})
    result_2 = df_2.select(hash_col=hash_xx("english"))

    hash_2 = 12926212925376531029
    expected_df2 = pl.DataFrame({"hash_col": [hash_2]})

    assert result_2.equals(expected_df2)

    # df_1['english'] and df_2['english'] should have different hash values
    assert not hash_1 == hash_2

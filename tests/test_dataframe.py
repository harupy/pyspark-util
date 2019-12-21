import pytest
from pyspark.sql import SparkSession
import pyspark_util as psu

spark = SparkSession.builder.getOrCreate()


def test_prefix_columns():
    data = [(1, 2, 3)]
    columns = ['a', 'b', 'c']
    df = spark.createDataFrame(data, columns)

    prefixed = psu.prefix_columns(df, 'x')
    assert prefixed.columns == ['x_a', 'x_b', 'x_c']

    prefixed = psu.prefix_columns(df, 'x', sep='|')
    assert prefixed.columns == ['x|a', 'x|b', 'x|c']

    prefixed = psu.prefix_columns(df, 'x', exclude=['b', 'c'])
    assert prefixed.columns == ['x_a', 'b', 'c']

    with pytest.raises(ValueError, match='The given dataframe does not contain'):
        psu.prefix_columns(df, 'x', exclude=['e'])


def test_suffix_columns():
    data = [(1, 2, 3)]
    columns = ['a', 'b', 'c']
    df = spark.createDataFrame(data, columns)

    suffixed = psu.suffix_columns(df, 'x')
    assert suffixed.columns == ['a_x', 'b_x', 'c_x']

    suffixed = psu.suffix_columns(df, 'x', sep='|')
    assert suffixed.columns == ['a|x', 'b|x', 'c|x']

    suffixed = psu.suffix_columns(df, 'x', exclude=['b', 'c'])
    assert suffixed.columns == ['a_x', 'b', 'c']

    with pytest.raises(ValueError, match='The given dataframe does not contain'):
        psu.suffix_columns(df, 'x', exclude=['e'])


def test_rename_columns():
    data = [(1, 2, 3)]
    columns = ['a', 'b', 'c']
    df = spark.createDataFrame(data, columns)

    renamed = psu.rename_columns(df, {'a': 'x', 'b': 'y', 'c': 'z'})
    assert renamed.columns == ['x', 'y', 'z']

    renamed = psu.rename_columns(df, {'a': 'x', 'b': 'y'})
    assert renamed.columns == ['x', 'y', 'c']

    with pytest.raises(ValueError, match='The given dataframe does not contain'):
        psu.rename_columns(df, {'e': 'x'})


def test_select_columns_regex():
    data = [(1, 2, 3)]
    columns = ['abc', '123', '___']
    df = spark.createDataFrame(data, columns)

    matched = psu.select_columns_regex(df, r'.+')
    assert matched.columns == ['abc', '123', '___']

    matched = psu.select_columns_regex(df, r'[a-z]+')
    assert matched.columns == ['abc']

    matched = psu.select_columns_regex(df, r'[0-9]+')
    assert matched.columns == ['123']

    matched = psu.select_columns_regex(df, '___')
    assert matched.columns == ['___']

    matched = psu.select_columns_regex(df, 'no_match')
    assert matched.columns == []

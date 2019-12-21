import pytest
from pyspark.sql import SparkSession
import pyspark_util as psu
from tests.utils import assert_frame_equal

spark = SparkSession.builder.getOrCreate()


def test_null_ratio():
    df = spark.createDataFrame([(1,), (2,), (None,), (None,)], ['x'])
    df_res = df.select(psu.null_ratio('x'))
    df_exp = spark.createDataFrame([(0.5,)], ['x'])
    assert_frame_equal(df_res, df_exp)


@pytest.mark.parametrize('rows, include_null, expected', [
    ([('a',), ('b',), ('',), ('',)], None, 0.5),
    ([('a',), ('b',), ('',), ('',), (None,)], None, 0.5),
    ([('a',), ('b',), ('',), ('',), (None,)], True, 0.4),
])
def test_blank_ratio(rows, include_null, expected):
    df = spark.createDataFrame(rows, ['x'])

    if include_null is None:
        df_res = df.select(psu.blank_ratio('x'))
    else:
        df_res = df.select(psu.blank_ratio('x', include_null=include_null))

    df_exp = spark.createDataFrame([(expected,)], ['x'])
    assert_frame_equal(df_res, df_exp)


@pytest.mark.parametrize('rows, expected', [
    ([(1,), (2,), (3,)], True),
    ([(1,), (2,), (2,)], False),
    ([(1,), (2,), (3,), (None,)], True),
    ([(1,), (2,), (3,), (None,), (None,)], False),
])
def test_is_unique(rows, expected):
    df = spark.createDataFrame(rows, ['x'])
    df_res = df.select(psu.is_unique('x'))
    df_exp = spark.createDataFrame([(expected,)], ['x'])
    assert_frame_equal(df_res, df_exp)


@pytest.mark.parametrize('pat, expected', [
    ('abc', [(True,), (False,), (None,)]),
    ('123', [(False,), (True,), (None,)]),
    (r'[a-z]+', [(True,), (False,), (None,)]),
    (r'\d+', [(False,), (True,), (None,)]),
    (r'[a-z]+|\d+', [(True,), (True,), (None,)]),
])
def test_contains(pat, expected):
    rows = [('abc',), ('123',), (None,)]
    df = spark.createDataFrame(rows, ['x'])
    df_res = df.withColumn('y', psu.contains('x', pat))
    df_exp = spark.createDataFrame(
        [(x[0], y[0]) for x, y in zip(rows, expected)],
        ['x', 'y']
    )
    assert_frame_equal(df_res, df_exp)

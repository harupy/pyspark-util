import pytest
from pyspark.sql import SparkSession
import pyspark_util as psu
from tests.utils import assert_frame_equal

spark = SparkSession.builder.getOrCreate()


def test_null_ratio():
    df = spark.createDataFrame([
        (1,),
        (2,),
        (None,),
        (None,),
    ], ['x'])

    result = df.select(psu.null_ratio('x'))
    expected = spark.createDataFrame([
        (0.5,),
    ], ['x'])

    assert_frame_equal(result, expected)


def test_blank_ratio():
    # dataframe that does not contain NULL.
    df = spark.createDataFrame([
        ('a',),
        ('b',),
        ('',),
        ('',),
    ], ['x'])

    result = df.select(psu.blank_ratio('x'))
    expected = spark.createDataFrame([
        (0.5,),
    ], ['x'])

    assert_frame_equal(result, expected)

    # dataframe that contains NULL.
    df = spark.createDataFrame([
        ('a',),
        ('b',),
        ('',),
        ('',),
        (None,),
    ], ['x'])

    result = df.select(psu.blank_ratio('x'))
    expected = spark.createDataFrame([
        (0.5,),
    ], ['x'])

    assert_frame_equal(result, expected)

    result = df.select(psu.blank_ratio('x', include_null=True))
    expected = spark.createDataFrame([
        (0.4,),
    ], ['x'])

    assert_frame_equal(result, expected)


def test_is_unique():
    df = spark.createDataFrame([(1,), (2,), (3,)], ['x'])
    result = df.select(psu.is_unique('x'))
    expected = spark.createDataFrame([(True,)], ['x'])
    assert_frame_equal(result, expected)

    df = spark.createDataFrame([(1,), (2,), (2,)], ['x'])
    result = df.select(psu.is_unique('x'))
    expected = spark.createDataFrame([(False,)], ['x'])
    assert_frame_equal(result, expected)

    df = spark.createDataFrame([(1,), (2,), (3,), (None,)], ['x'])
    result = df.select(psu.is_unique('x'))
    expected = spark.createDataFrame([(True,)], ['x'])
    assert_frame_equal(result, expected)

    df = spark.createDataFrame([(1,), (2,), (3,), (None,), (None,)], ['x'])
    result = df.select(psu.is_unique('x'))
    expected = spark.createDataFrame([(False,)], ['x'])
    assert_frame_equal(result, expected)


@pytest.mark.parametrize('pat,rows_expected', [
    ('abc', [(True,), (False,), (None,)]),
    ('123', [(False,), (True,), (None,)]),
    (r'[a-z]+', [(True,), (False,), (None,)]),
    (r'\d+', [(False,), (True,), (None,)]),
    (r'[a-z]+|\d+', [(True,), (True,), (None,)]),
])
def test_contains(pat, rows_expected):
    rows_in = [('abc',), ('123',), (None,)]
    df = spark.createDataFrame(rows_in, ['x'])
    result = df.withColumn('y', psu.contains('x', pat))
    expected = spark.createDataFrame(
        [(x[0], y[0]) for x, y in zip(rows_in, rows_expected)],
        ['x', 'y']
    )
    assert_frame_equal(result, expected)

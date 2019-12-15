from pyspark_util import sample, count_rows


def test_sample():
    assert sample() == 'pyspark_util'


def test_count_rows(spark):
    data = [('a', ), ('b', ), ('c', )]
    schema = ['str']
    df = spark.createDataFrame(data, schema)
    assert count_rows(df) == 3

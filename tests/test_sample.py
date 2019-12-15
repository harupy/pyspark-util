from pyspark_util import sample


def test_sample():
    assert sample() == 'pyspark_util'

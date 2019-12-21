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

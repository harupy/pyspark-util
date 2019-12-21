import pytest
from pyspark.sql import SparkSession
import pyspark_util as psu

session = SparkSession.builder.getOrCreate()


@pytest.fixture(scope='session', autouse=True)
def spark():
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def add_spark(doctest_namespace):
    doctest_namespace['spark'] = session
    doctest_namespace['psu'] = psu

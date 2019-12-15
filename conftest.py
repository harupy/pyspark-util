import pytest
from pyspark.sql import SparkSession

session = SparkSession.builder.getOrCreate()


@pytest.fixture(scope='session', autouse=True)
def spark():
    yield session
    session.stop()

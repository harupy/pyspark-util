from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def null_ratio(col_name):
    """
    Return the null ratio of the given column.

    Parameters
    ----------
    col_name : str
        column name

    Returns
    -------
    column
        Null ratio.

    """
    return F.mean(F.col(col_name).isNull().cast(IntegerType())).alias(col_name)

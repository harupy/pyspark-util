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

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     (1,),
    ...     (2,),
    ...     (None,),
    ...     (None,),
    ... ], ['x'])
    >>> df.select(psu.null_ratio('x')).show()  # doctest: +NORMALIZE_WHITESPACE
    +---+
    |  x|
    +---+
    |0.5|
    +---+

    """
    return F.mean(F.col(col_name).isNull().cast(IntegerType())).alias(col_name)


def blank_ratio(col_name, include_null=False):
    """
    Return the null ratio of the given column.

    Parameters
    ----------
    col_name : str
        column name
    include_null : bool, default False
        If True, the blank ratio is calculated including ``NULL`` rows.

    Returns
    -------
    column
        Blank ratio.

    Examples
    --------
    By default, ``NULL`` is ignored.

    >>> df = spark.createDataFrame([
    ...     ('a',),
    ...     ('b',),
    ...     ('',),
    ...     ('',),
    ...     (None,),
    ... ], ['x'])
    >>> df.select(psu.blank_ratio('x')).show()  # doctest: +NORMALIZE_WHITESPACE
    +---+
    |  x|
    +---+
    |0.5|
    +---+

    With ``include_null=True``, ``NULL`` is included in the calculation.

    >>> df = spark.createDataFrame([
    ...     ('a',),
    ...     ('b',),
    ...     ('',),
    ...     ('',),
    ...     (None,),
    ... ], ['x'])
    >>> df.select(psu.blank_ratio('x', include_null=True)).show()  # doctest: +NORMALIZE_WHITESPACE
    +---+
    |  x|
    +---+
    |0.4|
    +---+

    """
    is_blank = F.col(col_name) == ''
    if include_null:
        # fill NULL with False
        filled = F.when(F.col(col_name).isNull(), False).otherwise(is_blank)
        return F.mean(filled.cast(IntegerType())).alias(col_name)
    else:
        return F.mean(is_blank.cast(IntegerType())).alias(col_name)


def is_unique(col_name):
    """
    Return True if the given column is unique.

    Parameters
    ----------
    col_name : str
        column name

    Returns
    -------
    column
        is_unique

    Examples
    --------
    >>> df = spark.createDataFrame([(1,), (2,), (3,)], ['x'])
    >>> df.select(psu.is_unique('x')).show()  # doctest: +NORMALIZE_WHITESPACE
    +----+
    |   x|
    +----+
    |true|
    +----+

    >>> df = spark.createDataFrame([(1,), (2,), (2,)], ['x'])
    >>> df.select(psu.is_unique('x')).show()  # doctest: +NORMALIZE_WHITESPACE
    +-----+
    |    x|
    +-----+
    |false|
    +-----+

    >>> df = spark.createDataFrame([(1,), (2,), (3,), (None,)], ['x'])
    >>> df.select(psu.is_unique('x')).show()  # doctest: +NORMALIZE_WHITESPACE
    +----+
    |   x|
    +----+
    |true|
    +----+

    >>> df = spark.createDataFrame([(1,), (2,), (3,), (None,), (None,)], ['x'])
    >>> df.select(psu.is_unique('x')).show()  # doctest: +NORMALIZE_WHITESPACE
    +-----+
    |    x|
    +-----+
    |false|
    +-----+

    """
    return (
        (F.count(col_name) == F.countDistinct(col_name)) &
        (F.count(F.when(F.col(col_name).isNull(), 1).otherwise(None)) <= 1)
    ).alias(col_name)

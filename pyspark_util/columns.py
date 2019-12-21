import re
from functools import reduce
import pyspark.sql.functions as F


def _validate_columns(df, columns):
    """
    Raises a ValueError if the given dataframe doesn't contains columns with the given names.

    Parameters
    ----------
    df : dataframe
        dataframe to check.
    columns : list of str
        columns to check.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the given dataframe doesn't contain columns with the given names.

    Examples
    --------
    >>> data = [(1, 2, 3)]
    >>> columns = ['a', 'b', 'c']
    >>> df = spark.createDataFrame(data, columns)
    >>> _validate_columns(df, 'a')
    >>> _validate_columns(df, 'd')  # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: The given dataframe does not contain ['d'].
    """
    not_found = [c for c in columns if c not in df.columns]
    if len(not_found) > 0:
        raise ValueError('The given dataframe does not contain {}.'.format(not_found))


def prefix_columns(df, prefix, sep='_', exclude=[]):
    """
    Prefix dataframe columns.

    Parameters
    ----------
    df : dataframe
        dataframe to be prefixed.
    prefix : str
        string to add before each column.
    sep : str, default '_'
        separator to join ``prefix`` and each column with.
    exclude : list of str, default []
        A selection of columns to exclude from being prefixed.

    Returns
    -------
    dataframe
        dataframe with prefixed columns.

    Raises
    ------
    ValueError
        If ``exclude`` contains columns that don't exist in the given dataframe.

    Examples
    --------
    >>> data = [(1, 2, 3)]
    >>> columns = ['a', 'b', 'c']
    >>> df = spark.createDataFrame(data, columns)
    >>> df.show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+---+
    |  a|  b|  c|
    +---+---+---+
    |  1|  2|  3|
    +---+---+---+

    >>> psu.prefix_columns(df, 'x').show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+---+
    |x_a|x_b|x_c|
    +---+---+---+
    |  1|  2|  3|
    +---+---+---+

    >>> psu.prefix_columns(df, 'x', sep='|').show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+---+
    |x|a|x|b|x|c|
    +---+---+---+
    |  1|  2|  3|
    +---+---+---+

    >>> psu.prefix_columns(df, 'x', exclude=['b', 'c']).show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+---+
    |x_a|  b|  c|
    +---+---+---+
    |  1|  2|  3|
    +---+---+---+

    """
    _validate_columns(df, exclude)
    prefixed = [F.col(c).alias(prefix + sep + c) if c not in exclude else c for c in df.columns]
    return df.select(prefixed)


def suffix_columns(df, suffix, sep='_', exclude=[]):
    """
    Suffix dataframe columns.

    Parameters
    ----------
    df : dataframe
        dataframe to be suffixed.
    suffix : str
        string to add after each column.
    sep : str, default '_'
        separator to join each column and ``suffix`` with.
    exclude : list of str, default []
        A selection of columns to exclude from being suffixed.

    Returns
    -------
    dataframe
        dataframe with suffixed columns.

    Raises
    ------
    ValueError
        If ``exclude`` contains columns that don't exist in the given dataframe.

    Examples
    --------
    >>> data = [(1, 2, 3)]
    >>> columns = ['a', 'b', 'c']
    >>> df = spark.createDataFrame(data, columns)
    >>> df.show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+---+
    |  a|  b|  c|
    +---+---+---+
    |  1|  2|  3|
    +---+---+---+

    >>> psu.suffix_columns(df, 'x').show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+---+
    |a_x|b_x|c_x|
    +---+---+---+
    |  1|  2|  3|
    +---+---+---+

    >>> psu.suffix_columns(df, 'x', sep='|').show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+---+
    |a|x|b|x|c|x|
    +---+---+---+
    |  1|  2|  3|
    +---+---+---+

    >>> psu.suffix_columns(df, 'x', exclude=['b', 'c']).show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+---+
    |a_x|  b|  c|
    +---+---+---+
    |  1|  2|  3|
    +---+---+---+

    """
    _validate_columns(df, exclude)
    prefixed = [F.col(c).alias(c + sep + suffix) if c not in exclude else c for c in df.columns]
    return df.select(prefixed)


def rename_columns(df, mapper):
    """
    Rename dataframe columns.

    Parameters
    ----------
    df : dataframe
        dataframe to be renamed.
    mapper : dict
        dictionary with old name as keys and new name as values.

    Returns
    -------
    dataframe
        dataframe with renamed columns.

    Raises
    ------
    ValueError
        If ``mapper`` contains columns that don't exist in the given dataframe.

    Examples
    --------
    >>> data = [(1, 2, 3)]
    >>> columns = ['a', 'b', 'c']
    >>> df = spark.createDataFrame(data, columns)
    >>> df.show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+---+
    |  a|  b|  c|
    +---+---+---+
    |  1|  2|  3|
    +---+---+---+

    >>> psu.rename_columns(df, {'a': 'x'}).show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+---+
    |  x|  b|  c|
    +---+---+---+
    |  1|  2|  3|
    +---+---+---+

    """
    _validate_columns(df, list(mapper.keys()))
    return reduce(lambda df, item: df.withColumnRenamed(*item), mapper.items(), df)


def select_columns_regex(df, regex):
    """
    Select columns that match a given regular expression.

    Parameters
    ----------
    df : dataframe
        dataframe to be selected from.
    regex : str
        regular expression.

    Returns
    -------
    dataframe
        dataframe with matched columns

    Examples
    --------
    >>> data = [(1, 2)]
    >>> columns = ['abc', '123']
    >>> df = spark.createDataFrame(data, columns)
    >>> df.show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+
    |abc|123|
    +---+---+
    |  1|  2|
    +---+---+

    >>> psu.select_columns_regex(df, r'[a-z]+').show()  # doctest: +NORMALIZE_WHITESPACE
    +---+
    |abc|
    +---+
    |  1|
    +---+

    >>> psu.select_columns_regex(df, r'[0-9]+').show()  # doctest: +NORMALIZE_WHITESPACE
    +---+
    |123|
    +---+
    |  2|
    +---+

    """
    return df.select([c for c in df.columns if re.search(regex, c)])

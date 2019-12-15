import unittest
from pyspark.sql import SparkSession
import pyspark_util as psu

spark = SparkSession.builder.getOrCreate()


class TestDataFrame(unittest.TestCase):

    def test_prefix_columns(self):
        data = [(1, 2, 3)]
        columns = ['a', 'b', 'c']
        df = spark.createDataFrame(data, columns)

        prefixed = psu.prefix_columns(df, 'x')
        expected = ['x_a', 'x_b', 'x_c']
        self.assertEqual(prefixed.columns, expected)

        prefixed = psu.prefix_columns(df, 'x', sep='|')
        expected = ['x|a', 'x|b', 'x|c']
        self.assertEqual(prefixed.columns, expected)

        prefixed = psu.prefix_columns(df, 'x', exclude=['b', 'c'])
        expected = ['x_a', 'b', 'c']
        self.assertEqual(prefixed.columns, expected)

        with self.assertRaisesRegex(ValueError, 'The given dataframe does not contain'):
            psu.prefix_columns(df, 'x', exclude=['e'])

    def test_suffix_columns(self):
        data = [(1, 2, 3)]
        columns = ['a', 'b', 'c']
        df = spark.createDataFrame(data, columns)

        suffixed = psu.suffix_columns(df, 'x')
        expected = ['a_x', 'b_x', 'c_x']
        self.assertEqual(suffixed.columns, expected)

        suffixed = psu.suffix_columns(df, 'x', sep='|')
        expected = ['a|x', 'b|x', 'c|x']
        self.assertEqual(suffixed.columns, expected)

        suffixed = psu.suffix_columns(df, 'x', exclude=['b', 'c'])
        expected = ['a_x', 'b', 'c']
        self.assertEqual(suffixed.columns, expected)

        with self.assertRaisesRegex(ValueError, 'The given dataframe does not contain'):
            psu.suffix_columns(df, 'x', exclude=['e'])

    def test_rename_columns(self):
        data = [(1, 2, 3)]
        columns = ['a', 'b', 'c']
        df = spark.createDataFrame(data, columns)

        renamed = psu.rename_columns(df, {'a': 'x', 'b': 'y', 'c': 'z'})
        expected = ['x', 'y', 'z']
        self.assertEqual(renamed.columns, expected)

        renamed = psu.rename_columns(df, {'a': 'x', 'b': 'y'})
        expected = ['x', 'y', 'c']
        self.assertEqual(renamed.columns, expected)

        with self.assertRaisesRegex(ValueError, 'The given dataframe does not contain'):
            psu.rename_columns(df, {'e': 'x'})

    def test_select_columns_regex(self):
        data = [(1, 2, 3)]
        columns = ['abc', '123', '___']
        df = spark.createDataFrame(data, columns)

        matched = psu.select_columns_regex(df, r'.+')
        expected = ['abc', '123', '___']
        self.assertEqual(matched.columns, expected)

        matched = psu.select_columns_regex(df, r'[a-z]+')
        expected = ['abc']
        self.assertEqual(matched.columns, expected)

        matched = psu.select_columns_regex(df, r'[0-9]+')
        expected = ['123']
        self.assertEqual(matched.columns, expected)

        matched = psu.select_columns_regex(df, '___')
        expected = ['___']
        self.assertEqual(matched.columns, expected)

        matched = psu.select_columns_regex(df, 'no_match')
        expected = []
        self.assertEqual(matched.columns, expected)

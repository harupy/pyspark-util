# pyspark-util

![test_status](https://github.com/harupy/pyspark-util/workflows/Test/badge.svg)

A set of pyspark utility functions.

Example:

```python
import pyspark_util as psu

data = [(1, 2, 3)]
columns = ['a', 'b', 'c']
df = spark.createDataFrame(data, columns)
prefixed = psu.prefix_columns(df, 'x')
prefixed.show()
```

Output:

```
+---+---+---+
|x_a|x_b|x_c|
+---+---+---+
|  1|  2|  3|
+---+---+---+
```

## Development

### Setup

```
docker-compose build
docker-compose up -d
```

### Lint

```
docker exec psu-cnt ./tools/lint.sh
```

### Test

```
docker exec psu-cnt ./tools/test.sh
```

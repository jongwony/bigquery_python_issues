Case
====

MySQL Timestamp -> PyArrow -> BigQuery

Problem
=======

```
ValueError: year 0 is out of range
```

Solution
========

이 문제는 MySQL, Python, BigQuery 에서 지원하는 timestamp 범위가 다 달라서 생기는 문제입니다.

> MySQL: 0000-00-00 00:00:00 ~ 9999-12-31 23:59:59
> BigQuery, AWS Athena: 0001-01-01 00:00:00 ~ 9999-12-31 23:59:59
> Python Datetime: 0001-01-02 ~ 9999-12-31
> Pandas: 1900 ~ 2262

PyArrow 에서 Python Datetime 이 처리가 되지 않는 0001-01-01 과 그 전 날짜는 1900년도에서 초를 빼는 식으로 해결되어 있습니다.

이 문제를 단순 NULL로 처리하기에는 레거시 등에서 NULL 이 아닌 다른 의미로도 사용될 여지가 있기 때문입니다.

https://github.com/apache/arrow/blob/da641aa14ba7b228782fbfa7acd3f045ebfe2c86/python/pyarrow/scalar.pxi#L350

```python
import pyarrow as pa


def preprocess(arrow, schema):
    def date_condition(x):
        """
        mysql: timestamp data from `0000-00-00 00:00:00` to `NaT`
        python: timestamp >= datetime.fromtimestamp(-62135540872) `0000-01-02`
        BigQuery: select UNIX_SECONDS(timestamp '0001-01-01 00:00:00') -> -62135596800 seconds;
        """
        if x is None:
            return True
        if isinstance(x, datetime):
            return False
        elif isinstance(x, int):
            return x < -62135596800000000
        else:
            return False

    fields = {}
    for c_arr in arrow:
        name = c_arr._name
        if isinstance(c_arr.type, pa.TimestampType):
            col = c_arr.__array__().astype(object)
            col = np.where(np.vectorize(date_condition)(col), np.datetime64('NaT'), col)
            fields[name] = pa.array(col, type=pa.timestamp('us'))
        else:
            fields[name] = c_arr

    return pa.table(fields)

```


Case
====

MySQL Decimal type -> Pandas Preprocessing -> PyArrow -> BigQuery

Problem
=======

Pandas chunks 데이터 전송의 경우 가끔 컬럼의 모든 row 가 자동 데이터 변환이 일어나는 경우가 있다.
Decimal -> Float64 -> Int
전처리 단계의 Pandas 객체와 BigQuery 로 저장되어야 할 데이터 타입이 달라서 아래 에러를 발생시킬 경우가 있다.

```
ArrowInvalid: Got bytestring of length 8 (expected 16)
```

Solution
========

```python
import pandas as pd
pd.read_sql(sql, con, coerce_float=False)
```

https://github.com/pandas-dev/pandas/issues/20525
https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types?hl=ko#numeric_type

MySQL Decimal Type 의 경우 BigQuery Numeric Type, Python 의 Decimal 타입과 역할이 거의 동일합니다. 이 유형은 소수를 정확히 나타낼 수 있으며 금융 계산에 적합합니다.
하지만 MySQL의 Decimal Type을 Pandas 로 가져오는 중에 Python 의 FLOAT64 로 강제 형변환이 내부적으로 일어나게 되는데, 이때 소수데이터 유실이 일어납니다.
`coerce_float=False` 옵션을 설정할 경우에는 FLOAT64 로 형변환을 시키지않고, 강제로 Python 기본 라이브러리인 Decimal 타입 형태로 DataFrame을 가져옵니다.

```python
def send_bigquery(df, table, table_schema, if_exists='append', service_file=None):
    client = bigquery.Client(credentials=service_credentials(service_file))
    if if_exists == 'replace':
        client.delete_table(table, not_found_ok=True)
    job_config = bigquery.LoadJobConfig(schema=table_schema)
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()
```

table_schema 를 NUMERIC 으로 지정해서 BigQuery 로 전송할 수 있습니다.

```python
send_bigquery(df, 'dataset.table', [bigquery.SchemaField('decimal_column_name', 'NUMERIC'), ...])
```

---

Problem
=======

```
ArrowInvalid: Rescaling decimal value would cause data loss
```

BigQuery 라이브러리에서 Decimal Type 에 대한 내부적인 캐스팅이 일어나면서 발생하는 문제.

Solution
========

BigQuery 에서 Python API 를 통해 데이터를 전송하는 경우 Pandas DataFrame 을 PyArrow 형태로 가공하고, SNAPPY 로 압축해서 BigQuery 로 전송합니다.
이 에러는 Decimal Type 이 BigQuery 에서 표준으로 하는 Decimal(38, 9) 범위를 넘어 섰을 때 형변환을 하면 데이터가 손실된다고 에러를 발생하는 것입니다.

실제로 이 에러를 디버깅하면 arrow_type 이 아래와 같이 나타납니다.

```
ipdb> p arrow_type
Decimal128Type(decimal(38, 9))
```

MySQL 컬럼 타입이 `DECIMAL(18, 11)` 정도로 설정을 했을 때 위 에러가 발생하였습니다.

요구사항에 따라 처리방법이 달라지겠지만 굳이 11자리를 고수할 필요는 없었습니다.

BigQuery 의 최대 범위만 포함하는 식으로 변환을 진행합니다.

https://docs.python.org/ko/3/library/decimal.html#decimal-faq

```
from decimal import Decimal, getcontext


def trunc_decimal(x):
    if isinstance(x, Decimal):
        return +Decimal(x)
    else:
        return x


# BigQuery Decimal Type:: Decimal(38, 9)
getcontext().prec = 9
getcontext().Emax = 38
getcontext().Emin = 0

df = pd.read_sql(sql, con, coerce_float=False)

obj_col = df.select_dtypes(object).columns
df[obj_col] = df[obj_col].applymap(trunc_decimal)

send_bigquery(df, 'dataset.table', bigquery_schema)
```

Emax, Emin 을 벗어날 경우 마찬가지로 실제 형변환을 거치기 때문에 지정해준다.
0E-16 은 소수 아래 16자리까지 다루기 때문이다.

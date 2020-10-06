```python
def preprocess(arrow, schema):
    fields = {}
    for c_arr in arrow:
        name = c_arr._name
        if c_arr.type in (pa.float16(), pa.float32(), pa.float64()):
            # Nullable Integer: all integer with NaN -> int64 datatype
            col = c_arr.__array__()
            fields[name] = pa.array(col, type=schema.get(name), mask=np.isnan(col))
        else:
            fields[name] = c_arr

    return pa.table(fields)
```


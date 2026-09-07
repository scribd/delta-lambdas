
# query-metrics

This Lambda will execute DataFusion queries defined in a YAML file and submit
the results to CloudWatch Metrics which can be alerted upon or forwarded into
other tools



## Types

### Count

This is the simplest type of query and will simply record the number of rows from the query, e.g.:

```sql
SELECT id FROM source WHERE id > 1000 AND id <= 2000
```

Would consistently produce a counted metric value of `1000`.


### Numeric

Numeric is likely the most common and easy to understand query. There should only be one row in the result set and all of its values should be numeric values, e.g.:

```sql
SELECT COUNT(*) AS total, SUM(CASE WHEN (id > 1000 AND id <= 2000) THEN 1 ELSE 0 END) AS valid_ids FROM source
```

This will produce a result set of:

```
+-------+-----------+
| total | valid_ids |
+-------+-----------+
|  4096 |   1000    |
+-------+-----------+
```

Which will produce metric values of:

* `total` 4096
* `valid_ids` 1000


### Dimensional Count

The dimensional count is the most advanced query type and can be used to
provide dimensional (or tagged) metrics in CloudWatch

## Standardized results format


#### Columns

| column        | type        | description                                                  |
|---------------|-------------|--------------------------------------------------------------|
| `dataset.num` | int         | Simulation index (e.g., `0001` … `3400`)                     |
| `variable`    | string      | `Overall` or one of `X1`, `X2`, `X3`, `X4`, `X5`             |
| `level`       | string      | Subgroup level (`0/1` or `A/B/C`), or `NA` if not applicable |
| `year`        | int         | `3`, `4`, or `NA` if not applicable                          |
| `id.practice` | int         | `1..500`, or `NA` if not applicable                          |
| `satt`        | float       | Point estimate                                               |
| `lower90`     | float       | Lower bound of 90% interval                                  |
| `upper90`     | float       | Upper bound of 90% interval                                  |


#### Example

| dataset.num | variable | level | year | id.practice | satt | lower90 | upper90 |
|-------------|----------|-------|------|-------------|------|--------|--------|
| 1 | Overall | NA | NA | NA | `result` | `result` | `result` |
| 1 | Overall | NA | 3  | NA | `result` | `result` | `result` |
| 1 | Overall | NA | 4  | NA | `result` | `result` | `result` |
| 1 | X1 | 0 | NA | NA | `result` | `result` | `result` |
| 1 | X1 | 1 | NA | NA | `result` | `result` | `result` |
| 1 | X2 | A | NA | NA | `result` | `result` | `result` |
| 1 | X2 | B | NA | NA | `result` | `result` | `result` |
| 1 | X2 | C | NA | NA | `result` | `result` | `result` |
| 1 | X3 | 0 | NA | NA | `result` | `result` | `result` |
| 1 | X3 | 1 | NA | NA | `result` | `result` | `result` |
| 1 | X4 | A | NA | NA | `result` | `result` | `result` |
| 1 | X4 | B | NA | NA | `result` | `result` | `result` |
| 1 | X4 | C | NA | NA | `result` | `result` | `result` |
| 1 | X5 | 0 | NA | NA | `result` | `result` | `result` |
| 1 | X5 | 1 | NA | NA | `result` | `result` | `result` |
| 1 | NA | NA | NA | 1   | `result` | `result` | `result` |
| 1 | NA | NA | NA | ... | `result` | `result` | `result` |
| 1 | NA | NA | NA | 500 | `result` | `result` | `result` |
| ... | ... | ... | ... | ... | `result` | `result` | `result` |
| 3400 | NA | NA | NA | 500 | `result` | `result` | `result` |



---

### Recommended export format

Results should be exported as **CSV** by default.


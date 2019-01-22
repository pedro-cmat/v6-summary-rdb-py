|:warning: Privacy Preserving Distributed Learning (ppdDLI) |
|------------------|
| This algorithm is part of the [ppDLI](https://github.com/IKNL/ppDLI). A docker build of this algorithm can be obtained from docker-registry.distributedlearning.ai/dsummary |

# Distributed Summary
Algorithm that is inspired by the `Summary` function in R.  

## Privacy Preserving Dis

## Possible Privacy Issues

🚨 Categorial column with only one category <br />
🚨 `Min` an `Max` for each column is reported <br />
🚨 Column names can be geussed, by trail and error

## Privacy Protection

✔️ If column names do not match nothing else is reported
✔️ If dataset has less that 10 records, no statistical analysis is performed
✔️ Only statistical results are shared which include `Min`, `Q1`, `Mean`, `Median`, `Q3`, `Max` and number of `Nan` values per column.



```
docker run -e DATABASE_URI=/app/database.csv -v C:\Users\FMa1805.36838\Repositories\dSummary\local\input.txt:/app/input.txt -v C:\Users\FMa1805.36838\Repositories\dSummary\local\output.txt:/app/output.txt -v C:\Users\FMa1805.36838\Repositories\dSummary\local\database.csv:/app/database.csv dsummary
```

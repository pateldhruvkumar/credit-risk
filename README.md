# Lending Club Credit Risk Analysis

Exploratory analysis of the Lending Club loan dataset on **Databricks** using **PySpark** and **Spark SQL**. The notebook loads the raw CSV, cleans and reshapes it into an analysis-ready table, then answers five business questions about loan volume, pricing and borrower risk.

All work lives in one notebook: [lending-club.ipynb](lending-club.ipynb).

## Tech stack

- Databricks (Unity Catalog volume for storage)
- PySpark DataFrame API (`pyspark.sql.functions`, `pyspark.sql.types`)
- Spark SQL (`%sql` cells over a temp view)

## Data

- Source: Lending Club loan data CSV
- Location: `/Volumes/lending-club-loan/default/data-lending-club-loan`

The raw file contains free-text fields (for example `emp_title`) with commas, line breaks and escaped quotes. A plain `spark.read.csv` splits these rows, so the notebook reads with:

```python
spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .option("multiLine", "true")
  .option("escape", "\"")
  .csv("/Volumes/lending-club-loan/default/data-lending-club-loan")
```

## Data preparation

1. **Schema fixes.** Capture the inferred schema, then override only `id` (Integer) and `annual_inc` (Double), and re-read the CSV with the explicit schema.
2. **Loan term.** Rename `term` to `term_in_months` and strip the `" months"` text so the column is an integer.
3. **Dates.** Convert `issue_d`, `earliest_cr_line` and `last_credit_pull_d` from `MMM-yyyy` to `MM-yyyy` with `try_to_date`, so bad values become null instead of failing.
4. **Profiling.** Row count, `summary()` statistics, and a null/NaN count for every column.
5. **Column selection.** Keep 53 columns relevant to credit risk: loan terms and pricing, grade, borrower income and employment, DTI, FICO range, credit history, delinquency and utilisation metrics.
6. **Employment length.** Convert `emp_length` to an integer:
   - `10+ years` → `10`, `4 years` → `4`
   - `< 1 year` → `0`
   - Corrupt values containing `reactors` (text spilled from another field) → `0`
   - Null → `0`
7. **Loan status.** Inspect distinct `loan_status` values. Flag corrupt values (such as `Oct-2015`) and the legacy `Does not meet the credit policy. Status:Charged Off` status. Drop rows where `loan_status` is null, because every other column in those rows is also null.
8. **DTI and FICO.** Cast `dti` and `fico_range_low` with `TRY_CAST` in SQL. Exclude the `999` DTI placeholder and implausible values by keeping only `dti` between 0 and 100. A cleaned DataFrame (`df_cleaned`) also coalesces non-numeric `dti` to `0.0`.
9. **SQL view.** Register the cleaned DataFrame as the temp view `loans` for the SQL questions.

## Business questions answered

### 1. Top 5 states by total loan amount

For each `addr_state`: total loan amount, average interest rate and number of loans, ordered by total loan amount descending. Uses `GROUP BY` with `SUM`, `AVG` and `COUNT`.

### 2. Average loan by grade for high-income borrowers

For borrowers with `annual_inc > 100000`: average loan amount and average interest rate per `grade`. Only grades with at least 50 loans are shown (`HAVING COUNT(*) >= 50`), ordered by grade.

### 3. Top 3 largest loans per grade

`ROW_NUMBER() OVER (PARTITION BY grade ORDER BY loan_amnt DESC)` ranks loans inside each grade. The query returns the top 3 per grade with `addr_state`, `annual_inc` and `loan_rank`.

### 4. Borrowers above their state's average DTI

A window `AVG(dti) OVER (PARTITION BY addr_state)` gives each state's average DTI. The query returns borrowers whose DTI is above their state average, top 20 by DTI. Placeholder and outlier DTI values are filtered out first.

### 5. FICO risk buckets

A `CASE` expression on `fico_range_low` assigns each borrower a bucket:

| Bucket      | FICO range |
|-------------|------------|
| Low Risk    | >= 700     |
| Medium Risk | 630–699    |
| High Risk   | < 630      |

For each bucket: number of loans, total loan amount and average interest rate, ordered by average interest rate descending. This shows how pricing follows credit score.

## Extra

The last cell runs `explain("extended")` on a simple projection and filter (`loan_amnt > 6000`) to inspect Spark's parsed, analyzed, optimized and physical plans.

## How to run

1. Import `lending-club.ipynb` into a Databricks workspace.
2. Upload the Lending Club CSV to the volume `/Volumes/lending-club-loan/default/data-lending-club-loan`, or change the path in the read cells.
3. Attach a cluster or serverless compute and run all cells in order. The SQL cells depend on the `loans` temp view created earlier in the notebook.

# Data Engineering Production Scenario Bank & Technical Prep

> **Technical Screen & Scenario Prep Bank for Dedeepya Majety**  
> **Target Scope:** Complex PySpark Performance Tuning, Distributed Failure Recovery, and Lakehouse Edge Cases  
> **Standard:** Modeled after senior interview question banks and real-world production incident response.

---

## 1. PySpark Performance Optimization Scenarios

### Scenario 1: Debugging Out-Of-Memory (OOM) Errors on Spark Executors
* **The Symptom:** A PySpark batch job fails with `java.lang.OutOfMemoryError: Java heap space` or `ExecutorLostFailure (exit code 137 - OOMKilled)`.
* **Root Causes & Diagnostic Process:**
  1. *Data Skew:* One partition contains 10x more records than other partitions due to a high-frequency key (e.g. `customer_id = NULL` or a massive enterprise account).
  2. *Excessive BroadCast Size:* Broadcasting a table larger than `spark.sql.autoBroadcastJoinThreshold` (default 10MB) or exceeding driver memory.
  3. *Uncontrolled `collect()`:* Calling `.collect()` or `.toPandas()` on multi-gigabyte DataFrames, overwhelming the Spark driver.
* **Dedeepya's Resolution Strategy:**
  - Check the Spark UI **Stage details**: inspect the min, median, and max duration and input size of tasks. If max is 2GB while median is 20MB, it is textbook skew.
  - Fix skew via **key salting**: concatenate a random integer between 1 and `N` to the join key, repartition, join against a replicated dimension table, and drop the salt.
  - Enable **Adaptive Query Execution (AQE)** (`spark.sql.adaptive.enabled = true`, `spark.sql.adaptive.skewJoin.enabled = true`), allowing Spark to automatically split skewed partitions at runtime.

---

### Scenario 2: Handling Late-Arriving Data in Streaming Pipelines (Watermarking)
* **The Symptom:** Streaming clickstream or IoT transaction events arrive out of order due to network buffering, risking inaccurate aggregations.
* **Resolution via PySpark Structured Streaming:**
  - Define an explicit **watermark** specifying how late an event can arrive before being discarded by the stateful operator.
  - Example: `df.withWatermark("event_time", "30 minutes").groupBy(window("event_time", "10 minutes"), "user_id").count()`.
  - Records arriving within the 30-minute threshold update the state store; events older than 30 minutes are gracefully dropped to prevent unbounded memory growth in the state store.

---

### Scenario 3: The Small File Dilemma in Streaming Sinks
* **The Symptom:** Writing micro-batches every 10 seconds creates tens of thousands of sub-megabyte Parquet files. Querying the table takes minutes because the driver spends 90% of its time listing S3/ADLS metadata.
* **Resolution Strategy:**
  1. *Optimized Writes & Auto-Compaction:* Configure `spark.databricks.delta.autoCompact.enabled = true` and `spark.databricks.delta.optimizeWrite.enabled = true`.
  2. *Scheduled Maintenance:* Run an hourly or daily `OPTIMIZE table_name` job to compact files into 1GB chunks, followed by `VACUUM table_name RETAIN 168 HOURS` to delete stale tombstoned Parquet files older than 7 days.

---

## 2. Informatica MDM & Master Data Governance Scenarios

### Scenario 1: Configuring Match-Merge Rule Sets & Trust Scores
* **The Business Problem:** A customer registers online with `First: John, Last: Doe, Email: jdoe@gmail.com` and later creates an in-store account with `First: J., Last: Doe, Phone: 555-0199`. Inconsistent source feeds create duplicate accounts.
* **Dedeepya's Resolution:**
  - In **Informatica MDM**, configure exact match rules (Tax ID, Social Security, National ID) with a trust score of 1.0 for automated consolidation.
  - Configure fuzzy match rules (Levenshtein distance on Name + Soundex on Address + Email domain match).
  - Define **Survivorship Logic**: Assign higher decay and trust curves to modern CRM inputs over legacy ERP records so that recently verified phone numbers overwrite older unverified values.

---

## 3. Power BI & Dimensional Modeling Scenarios

### Scenario 1: Star Schema vs. Snowflake Schema for Performance
* **Question:** *"Why do we denormalize tables into a clean star schema instead of leaving them in 3NF snowflake schemas?"*
* **Dedeepya's Answer:**  
  "In analytical engines like Power BI's VertiPaq and Databricks SQL, joins are expensive and storage is cheap. 
  
  A star schema with single-hop relationships between Fact and Dimension tables minimizes join complexity, enables bidirectional cross-filtering without circular ambiguity, and maximizes columnar compression. Snowflaking adds multiple join hops that increase query latency and degrade dashboard responsiveness."

### Scenario 2: Semi-Additive Measures in DAX
* **Question:** *"How do you calculate opening or closing account balances in DAX where values cannot be summed across time?"*
* **Dedeepya's Answer:**  
  "Account balances and inventory levels are semi-additive: they can be summed across categories or stores, but not across dates. 
  
  I use `CALCULATE()` combined with `LASTDATE()` or `CLOSINGBALANCEMONTH()`:
  ```dax
  Closing_Account_Balance = 
  CALCULATE(
      SUM(FactAccountBalances[BalanceAmount]),
      LASTDATE(DimDate[Date])
  )
  ```
  This ensures that when a user selects a full quarter, Power BI evaluates the balance on the last recorded date of that quarter rather than summing each day's balance."

---

## 4. Cheat Sheet: Critical Spark SQL & PySpark Functions

| Operation | PySpark Syntax | Key Benefit |
| :--- | :--- | :--- |
| **Window Deduplication** | `row_number().over(Window.partitionBy("id").orderBy(col("updated_at").desc()))` | Takes latest record per primary key without self-joins. |
| **Broadcast Join** | `df_large.join(broadcast(df_small), "key")` | Eliminates expensive network shuffle for tables < 50MB. |
| **Delta Lake Merge** | `delta_table.alias("t").merge(source.alias("s"), "t.id = s.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()` | Atomic upsert with zero risk of partial commits. |
| **Partition Pruning** | `df.filter("year = 2026 AND month = 10")` | Reads only relevant partition folders, skipping 95%+ of data scan. |

# 30-Minute Pre-Interview Quick-Recall Cheat Sheet
### High-Density Flashcard & Strategy Guide for Dedeepya Majety

**Candidate:** Dedeepya Majety (Azure Data Engineer, 3.9 YOE @ Cognizant)  
**Read this doc 30 minutes before any Technical Screen or Hiring Manager Call.**

---

## 1. The 60-Second Opening Hook (Memorize Word-for-Word)

> *"I am an Azure Data Engineer with nearly 4 years of experience at Cognizant, specializing in building and modernizing enterprise data platforms on Azure Databricks, PySpark, and Delta Lake.*
>
> *Most recently, I contributed to migrating 40+ legacy ETL workflows from Informatica PowerCenter to a Databricks Medallion Lakehouse, helping reduce pipeline runtimes by 40% and cloud spend by ~30%, while orchestrating automated pipelines using Apache Airflow and managing data quality with Unity Catalog.*
>
> *I hold verified certifications including Databricks Certified Data Engineer Associate and Microsoft Power BI Data Analyst Associate. Currently, I am serving my official notice period with an offer in hand at ~11 LPA and am available to join within 30 days, which is why I am very excited to bring these lakehouse capabilities to [Company]."*

---

## 2. Key Production Metrics (Commit to Memory)

| Metric | Context / Project Lever |
| :--- | :--- |
| **40% Runtime Reduction** | Legacy Informatica ETL migrated to distributed PySpark on Databricks. |
| **~30% Cloud Cost Savings** | Cluster auto-scaling, spot instances, and replacing 24/7 on-prem nodes. |
| **15% Duplicate Reduction** | Informatica MDM match-and-merge trust score consolidation. |
| **99.8% Pipeline SLA Uptime** | Automated Apache Airflow DAG retries and alert webhooks. |
| **10.9 LPA Infosys Offer** | Current confirmed offer in hand used as the hard negotiation floor. |
| **30-Day Joining** | Actively serving official notice period at Cognizant. |

---

## 3. Top 10 Technical Viva Questions (1-Sentence Golden Answers)

1. **What is Catalyst Optimizer?**  
   *Spark's query optimizer that translates DataFrames through Analysis, Logical Optimization (predicate pushdown, column pruning), Physical Planning, and Whole-Stage Code Generation via Project Tungsten.*

2. **Narrow vs. Wide Transformations?**  
   *Narrow transformations (filter, select) do not require data shuffling across the network; Wide transformations (groupBy, join, distinct) require an expensive shuffle across nodes and create new Stage boundaries.*

3. **How does Delta Lake achieve ACID transactions?**  
   *Through an ordered JSON transaction log (`_delta_log/`) and Parquet checkpoints, using Optimistic Concurrency Control (OCC) to guarantee atomicity and snapshot isolation on object storage.*

4. **What is the difference between Z-Ordering and Liquid Clustering?**  
   *Z-Ordering maps multi-column attributes along a space-filling curve during manual `OPTIMIZE` runs, while Liquid Clustering (`CLUSTER BY`) dynamically clusters data during writes without full-table rewrites.*

5. **How do you identify and fix Data Skew in Spark UI?**  
   *In the Stage Details task summary, if Max task time is 10x higher than Median task time, data is skewed; fix it via Key Salting (`concat(key, rand())`) or enabling Adaptive Query Execution (`spark.sql.adaptive.skewJoin.enabled = true`).*

6. **When would you use `coalesce` instead of `repartition`?**  
   *Use `coalesce(N)` to reduce partitions without a full network shuffle when saving files to disk; use `repartition(N)` when you need to increase partitions or evenly balance skewed data across executors.*

7. **How does Databricks Auto Loader work?**  
   *Auto Loader (`cloudFiles`) incrementally ingests newly arrived files from cloud storage using queue notifications or directory listings, offering automatic schema inference, evolution, and a `_rescued_data` column.*

8. **Driver OOM vs Executor OOM?**  
   *Driver OOM occurs from calling `.collect()` or broadcasting oversized tables; Executor OOM occurs from severe partition skew or memory pressure exceeding the JVM heap + overhead.*

9. **How do you implement an idempotent pipeline?**  
   *By using partition-scoped overwrites (`replaceWhere = "date = '...' "`) or Delta `MERGE INTO` matching on unique business keys, ensuring reruns produce zero duplicate records.*

10. **What is Unity Catalog's 3-level namespace?**  
    *`catalog.schema.table`, providing unified governance, lineage tracking, and fine-grained column masking across Databricks workspaces.*

---

## 4. PySpark Code Snippets (Instant Recall)

### A. Deduplication via Window Function
```python
from pyspark.sql.window import Window
import pyspark.sql.functions as F

w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
clean_df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
```

### B. Broadcast Join for Small Dimension Tables (<10MB)
```python
joined_df = large_fact_df.join(F.broadcast(small_dim_df), "customer_id", "left")
```

### C. Exploding Nested JSON Arrays
```python
flat_df = raw_df.withColumn("item", F.explode("items")).select("order_id", "item.item_id", "item.price")
```

---

## 5. Spark UI Debugging Checklist (4 Steps)
1. **Locate the Longest Stage** under the Stages tab.
2. **Check Task Distribution:** Look at Min, Median, 75th percentile, and Max duration.
3. **Check Metrics:** Look for high **Shuffle Read**, non-zero **Spill (Memory)**, or non-zero **Spill (Disk)**.
4. **Identify the Bottleneck:** If Max duration >> Median duration $\rightarrow$ **Data Skew**. If all tasks are slow $\rightarrow$ **Resource starvation or heavy serialization**.

---

## 6. 5 High-Agency Inversion Questions to Ask the Interviewer
1. *"How is the team currently handling data governance and table-level access control across multi-tenant environments?"*
2. *"Are your pipelines predominantly batch-oriented on Delta Lake, or are you transitioning workloads toward streaming with Delta Live Tables?"*
3. *"How do you balance compute auto-termination and cluster warm-up latency for user-facing Databricks SQL dashboards?"*
4. *"What is the standard CI/CD deployment workflow for promoting Databricks notebooks and PySpark wheel packages to production?"*
5. *"What would be my primary execution priority during the first 60 days on this team?"*

---

## 7. Compensation & Notice Period Strategy
* **Notice Period:** *"I am serving my official notice period at Cognizant and am available to join within 30 days."*
* **Salary Floor:** *"I hold an active offer in hand at 10.9 LPA fixed. For this role, my target expectation is 16 to 18 LPA fixed based on my hands-on lakehouse experience and certifications."*

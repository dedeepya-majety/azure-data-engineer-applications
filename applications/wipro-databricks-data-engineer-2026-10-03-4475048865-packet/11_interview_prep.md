# Interview Preparation Master Dossier: Wipro — DataBricks Data Engineering

> **Target Role:** DataBricks - Data Engineering (`4475048865`)  
> **Company:** Wipro Limited  
> **Candidate:** Dedeepya Majety (Associate - Azure Data Engineer, 3.9 YOE)  
> **Interview Format:** 100% Virtual / Remote (Microsoft Teams)  

---

## 1. 30-Second Elevator Pitch
> *"I am an Azure Data Engineer with 3.9 years of experience at Cognizant, specializing in building and modernizing enterprise cloud data pipelines on Azure Databricks, PySpark, and Delta Lake. I hold the Databricks Certified Data Engineer Associate certification and Microsoft PL-300. In my current engagement, I led the migration of 40+ legacy ETL workflows to a Databricks Medallion Lakehouse, cutting runtime by 40% and cloud compute spend by ~30%, while automating ingestion with Auto Loader and governing data with Unity Catalog. I am currently serving my official notice period and am available to join within 30 days."*

---

## 2. Core Databricks & PySpark Technical Questions (High-Probability)

### Q1: What happens under the hood during a PySpark Shuffle, and how do you minimize it?
- **Answer Structure:**
  1. **Definition:** Shuffle is the physical redistribution of data across cluster worker nodes caused by wide transformations (`groupBy`, `join`, `distinct`, `repartition`).
  2. **Performance Bottleneck:** Involves disk I/O, network serialization, and network transfer, often causing Out-Of-Memory (OOM) or straggler tasks.
  3. **Mitigation Techniques Used in Cognizant Project:**
     - **Broadcast Join (`broadcast(small_df)`):** When one table is small (<10MB or configured threshold), broadcasting eliminates shuffling of the larger table.
     - **Partition Pruning & Salting:** Avoid data skew on skewed join keys by adding a salt key (`concat(col, lit(rand()))`) before joining, then aggregating back.
     - **`reduceByKey` over `groupByKey`:** In RDD operations, `reduceByKey` combines values locally on each executor prior to shuffling.

### Q2: How does Databricks Auto Loader work compared to standard structured streaming?
- **Answer Structure:**
  - Auto Loader uses `cloudFiles` source to incrementally process new files arriving in cloud storage (ADLS Gen2) without state management overhead.
  - Two discovery modes:
    - *Directory Listing:* Efficient for small/moderate file volume.
    - *File Notification Mode:* Leverages Azure Event Grid and Queue Storage to notify the cluster when new files land, scaling efficiently to millions of files.
  - **Schema Evolution & Rescue:** Automatically captures unmapped or unexpected columns into a `_rescued_data` column without failing the ingestion stream.

### Q3: How do Delta Lake ACID transactions work, and how do you optimize a Delta table?
- **Answer Structure:**
  - **Transaction Log (`_delta_log/`):** Delta uses write-ahead JSON transaction logs with Parquet checkpoints every 10 commits. Atomicity and consistency are ensured via optimistic concurrency control.
  - **Optimization Commands:**
    - `OPTIMIZE table_name`: Compacts small files into large ~1GB Parquet files.
    - `Z-ORDER BY (col1, col2)`: Colocates multidimensional data to maximize data skipping during filter queries.
    - `VACUUM table_name RETAIN 168 HOURS`: Removes uncommitted or historical files beyond the retention period (default 7 days) to reclaim storage.

---

## 3. STAR+R Behavioral Interview Stories

### Story 1: Migrating Legacy Workloads to Databricks (Technical Rigor & Delivery)
- **Situation:** Cognizant client had 40+ legacy Informatica batch jobs that ran overnight, frequently exceeding the morning 6 AM SLA and causing pipeline contention.
- **Task:** Migrate and re-engineer the pipeline using Azure Databricks, PySpark, and Delta Lake while verifying 100% data parity.
- **Action:** Structured the target pipeline into Medallion layers (Bronze raw, Silver cleaned/deduplicated, Gold dimensional). Replaced row-by-row processing with vectorized PySpark DataFrames, configured optimal partition columns, and authored automated checksum and row-count reconciliation scripts.
- **Result:** Pipeline execution time dropped by **40%**, infrastructure compute cost decreased by **~30%**, and all morning SLAs were met consistently.
- **Reflection:** Learned the importance of data skipping and file compaction (`OPTIMIZE` / `Z-ORDER`) in Delta Lake to maintain low query latency over time.

---

## 4. Compensation & Offer Negotiation Script
- **When Recruiter asks:** *"What is your expected CTC?"*
- **Response:**
  > *"Given my 3.9 years of hands-on experience, my Databricks Associate certification, and my active project track record in Lakehouse migrations, I am looking for a package in the range of 15 to 16 LPA. More importantly, since I am already serving my notice period with an official 30-day availability, I am ready to complete onboarding formalities and allocate to client billing immediately."*

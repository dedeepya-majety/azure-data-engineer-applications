# Interview Preparation Master Dossier: PwC Acceleration Center India
**Target Role:** FS - Data Engineer - Associate (Req ID: 4467378631)  
**Candidate:** Dedeepya Majety (Associate - Data Engineer @ Cognizant, 3.9 YOE)  
**Location:** Bengaluru East, Karnataka, India  
**Date Generated:** 2026-10-12  
**Notice Period Status:** 🟢 Serving Notice Period · Available to Join in 30 Days  
**Compensation Floor Anchor:** Infosys Offer @ 10.9 LPA · **Target Range:** 16.0 – 22.0+ LPA  

---

## 1. 24-Hour Pre-Call Positioning Briefing

### 1.1 The 60-Second Opening Hook
> "I am an Azure Data Engineer with nearly 4 years of experience at Cognizant, specializing in building and modernizing enterprise data platforms on Azure Databricks, PySpark, and Delta Lake.
>
> Most recently, I contributed to migrating 40+ legacy ETL workflows from Informatica PowerCenter to a Databricks Medallion Lakehouse, helping reduce pipeline runtimes by 40% and infrastructure spend by ~30%, while orchestrating automated pipelines using Apache Airflow and managing data quality with Unity Catalog.
>
> I hold verified certifications including Databricks Certified Data Engineer Associate and Microsoft Power BI Data Analyst Associate. Currently, I am serving my official notice period with an offer in hand at ~11 LPA and am available to join within 30 days, which is why I am very excited to bring these lakehouse capabilities to PwC Acceleration Center India."

### 1.2 Top 3 Proof Points Aligned to PwC Acceleration Center India
1. **Delta Lake Medallion Modernization:** Ingested multi-source data via Auto Loader (`cloudFiles`) into Bronze, conformed and deduplicated via PySpark in Silver, and published curated Gold models for business analytics.
2. **High-Reliability Workflow Orchestration:** Built multi-hop Apache Airflow DAGs with automated retries and SLA monitoring, upholding 99.8% pipeline uptime.
3. **Master Data & Governance Rigor:** Managed entity deduplication and match-merge rules using Informatica MDM and implemented table and column-level RBAC via Databricks Unity Catalog.

### 1.3 Strategic Gap Deflection
- **If asked about AWS / GCP:** *"While my primary production environment has been Azure (ADLS Gen2, ADF, Azure Databricks), the core distributed computing primitives are identical. Databricks runs natively across AWS and Azure, and my expertise in PySpark, Delta Lake, Spark SQL, and Airflow transfers directly to any cloud infrastructure."*
- **If asked about Snowflake:** *"Both Delta Lake and Snowflake solve modern analytical warehousing challenges. Delta Lake provides open-source parquet-based ACID storage with zero vendor lock-in and native Spark execution. Having mastered Delta Lake transactions and Medallion modeling, adapting to Snowflake's architecture is straightforward."*

---

## 2. 5 Canonical Technical Architectures to Defend in System Design

### Architecture 1: Delta Lake Medallion Architecture (Bronze -> Silver -> Gold)
- **Bronze (Raw Ingestion):** Incremental ingestion using Databricks Auto Loader (`spark.readStream.format("cloudFiles")`), storing raw JSON/CSV data with schema evolution enabled.
- **Silver (Cleaned & Conformed):** PySpark transformations, deduplication using window functions (`row_number() OVER (PARTITION BY business_key ORDER BY ingestion_timestamp DESC)`), data type standardization, and Delta ACID `MERGE INTO`.
- **Gold (Business Aggregates):** Star-schema dimensional models optimized with Z-Ordering on high-cardinality query columns (`OPTIMIZE gold_table ZORDER BY (event_date, customer_id)`), serving Databricks SQL and Power BI.

### Architecture 2: Slowly Changing Dimensions (SCD Type 2) in Delta Lake
- Efficient implementation using single-pass `MERGE INTO` or two-step staging:
  1. Identify changed records comparing hash keys of attributes.
  2. Expire old records by setting `is_current = false` and `end_date = current_date()`.
  3. Insert new versions with `is_current = true`, `start_date = current_date()`, and `end_date = null`.

### Architecture 3: PySpark Performance Tuning & Skew Mitigation
- **Data Skew Identification:** Checking Spark UI for tasks taking 10x longer than median tasks, resulting in spill to disk.
- **Remediation Techniques:**
  - **Broadcast Joins:** For dimension tables < 10MB (`broadcast(dim_df)`), eliminating expensive network shuffle stages.
  - **Salting Keys:** Appending random salt prefixes `concat(key, floor(rand() * 10))` to distribute hotspots uniformly across executor partitions.
  - **Adaptive Query Execution (AQE):** Enabling `spark.sql.adaptive.enabled = true` and `spark.sql.adaptive.skewJoin.enabled = true` for dynamic shuffle partition coalescing.

### Architecture 4: Robust Orchestration with Apache Airflow
- Modular Python DAGs using `DatabricksRunNowOperator` or `DatabricksSubmitRunOperator`.
- Robust error handling: `retries=3`, `retry_delay=timedelta(minutes=5)`, and automated Slack/Email alerts on SLA miss.
- Idempotency guarantees: Ensuring each DAG run is partition-scoped and safely rerunnable without generating duplicate records.

### Architecture 5: Centralized Governance via Databricks Unity Catalog
- 3-level namespace: `catalog.schema.table`.
- Granular permissions: `GRANT SELECT ON TABLE catalog.sales.orders TO ROLE analyst_group;`.
- Dynamic column masking (`CASE WHEN is_member('hr') THEN ssn ELSE 'XXX-XX-XXXX' END`) and row-level security filters.

---

## 3. Ground-Truth STAR+R Behavioral Story Bank

### Story 1: Enterprise Modernization (Informatica to Databricks)
- **Situation:** Legacy Informatica PowerCenter workflows were failing to meet daily SLAs due to batch data growth and compute bottlenecks.
- **Task:** Migrate 40+ legacy workflows to Azure Databricks Delta Lake and PySpark while guaranteeing 100% data parity.
- **Action:** Analyzed SQL stored procedures, converted transformations to vectorized PySpark code, configured Auto Loader for incremental ingestion, and created automated row-count and checksum parity verification scripts.
- **Result:** Slashed average pipeline runtime by 40%, reduced cloud infrastructure spend by ~30%, and achieved zero data loss during cutover.
- **Reflection:** Learned the power of decoupled compute and storage over traditional monolithic on-prem ETL engines.

### Story 2: Resolving a Critical Production Schema Failure
- **Situation:** A daily vendor data drop changed an integer field to an alphanumeric format at midnight, crashing the downstream staging job.
- **Task:** Restore pipeline execution before the 8:00 AM business reporting deadline.
- **Action:** Investigated the Airflow execution log, isolated the failed file, utilized Databricks Auto Loader's `_rescued_data` column to catch corrupt rows without aborting the batch, and updated schema evolution rules.
- **Result:** Restored the pipeline by 5:30 AM, preventing business disruption, and authored a schema contract rule set to prevent recurrence.
- **Reflection:** Reinforced the necessity of automated schema drift handling in production pipelines.

### Story 3: Master Data Deduplication & Data Quality
- **Situation:** Source system merges created duplicate customer and organization records, degrading reporting accuracy.
- **Task:** Eliminate duplicates and establish unified golden records.
- **Action:** Refined Informatica MDM match-and-merge rule sets, adjusted trust scores across source attributes, and implemented survivorship logic.
- **Result:** Successfully consolidated master records, eliminating ~15% duplicate entities and enhancing analytics reliability.
- **Reflection:** Good data engineering begins at the ingestion and entity resolution layer, not just the reporting layer.

---

## 4. Architect-Level Inversion Questions to Ask Interviewers

1. *"How is the team currently managing data governance and fine-grained access control across your lakehouse environments (e.g., Unity Catalog, Purview, or custom RBAC)?"*
2. *"Are the majority of your pipelines running scheduled batch jobs, or is there an active roadmap toward streaming and event-driven architectures with Structured Streaming?"*
3. *"What does your deployment and CI/CD lifecycle look like for Databricks notebooks and Airflow DAGs from dev to production?"*
4. *"What is the single biggest engineering bottleneck your data platform team is looking to solve over the next two quarters?"*

---

## 5. Compensation & Notice Period Strategy

- **Notice Period Positioning:**  
  *"I am actively serving my notice period at Cognizant and can join within 30 days. I have already cleared initial rounds and hold a confirmed offer in hand at 10.9 LPA."*
- **Anchoring the Offer:**  
  *"Given my 3.9 years of enterprise lakehouse experience, verified Databricks and Power BI certifications, and immediate availability, I am targeting 17–20 LPA fixed for this role."*

# Interview Preparation Master Dossier: Zoetis — Data Engineer (ZICC Hyderabad)

> **Target Role:** Data Engineer (`4472331481`)  
> **Company:** Zoetis, Inc. (Zoetis India Capability Center, Hyderabad)  
> **Candidate:** Dedeepya Majety (Associate - Azure Data Engineer, 3.9 YOE)  
> **Interview Format:** 100% Virtual / Remote (Microsoft Teams)  

---

## 1. 30-Second Elevator Pitch
> *"I am an Azure Data Engineer with 3.9 years of experience at Cognizant, specializing in building cloud data pipelines with Azure Data Factory, Azure Databricks, and PySpark. I am a Databricks Certified Data Engineer Associate and Microsoft Power BI Data Analyst. In my current client engagement with Amgen, I led the migration of 40+ legacy workflows to a Databricks Medallion Lakehouse, cutting runtime by 40% and cloud compute spend by ~30%, while managing master data deduplication across healthcare entities. I am currently serving my notice period with official 30-day availability, and am looking forward to joining Zoetis's Hyderabad capability center."*

---

## 2. Technical Focus Areas for Zoetis ZICC

### ADF + Databricks Orchestration Pattern
- **Scenario:** How do you trigger and pass dynamic parameters from Azure Data Factory to an Azure Databricks notebook?
- **Answer:**
  1. Use the **Databricks Notebook Activity** in ADF.
  2. Configure a linked service to Azure Databricks using a managed identity or secret stored in Azure Key Vault.
  3. Define `baseParameters` in ADF pointing to dynamic pipeline expressions (e.g., `@pipeline().parameters.triggerDate`, `@activity('LookupMetadata').output.value`).
  4. In the Databricks notebook, capture parameters using `dbutils.widgets.get("triggerDate")`.
  5. Return exit metrics back to ADF using `dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "rows_processed": count}))` for conditional downstream branching.

### Master Data Management (MDM) & Golden Record Consolidation
- **Scenario:** In commercial systems, how do you handle entity resolution and deduplication across multiple source feeds?
- **Answer:**
  1. Ingest raw landing feeds into ADLS Gen2 Bronze layer.
  2. Apply cleansing, normalization, and standardization in Silver layer.
  3. In MDM (Informatica MDM or Databricks PySpark entity resolution), configure match rules combining deterministic rules (NPI, tax IDs, exact address) and probabilistic fuzzy matching (Jaro-Winkler on name + phonetics).
  4. Configure survivorship and trust rules to blend attributes from higher-trust systems into the final consolidated Golden Record.
  5. Propagate Golden IDs downstream into Gold analytical tables for reporting.

---

## 3. Compensation & Negotiation Framing
- **Floor in Hand:** Infosys @ 10.9 LPA.
- **Zoetis ZICC Band:** 16.0 – 21.0 LPA.
- **Initial Anchor:**
  > *"Given my direct life sciences data pipeline experience, my Databricks Associate certification, and my immediate 30-day joining availability, I am targeting an annual fixed compensation of 18 to 20 LPA."*

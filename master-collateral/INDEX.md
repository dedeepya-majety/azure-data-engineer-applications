# Dedeepya Majety: Master Evidence Index & Career Dossier

> **Single source of truth for resume, recruiter screens, technical interviews, and portfolio verification.**  
> Organized by role archetype, verified metrics, production architectures, and source files.  
> **Honesty rule:** Every claim below is backed by verified production work at Cognizant Technology Solutions and official certifications. No fabrication.

---

## 1. Quick Profile & Contact

- **Full Name:** Dedeepya Majety
- **Email:** `majetydedeepya0@gmail.com`
- **Location:** Chennai, Tamil Nadu, India (Open to Hybrid / Remote / Relocation)
- **LinkedIn:** [linkedin.com/in/dedeepya-majety](https://www.linkedin.com/in/dedeepya-majety/)
- **GitHub:** [github.com/dedeepya-majety](https://github.com/dedeepya-majety)
- **Portfolio:** [dedeepya-majety-portfolio.vercel.app](https://dedeepya-majety-portfolio.vercel.app)
- **Target Roles:** Azure Data Engineer, Databricks Platform Engineer, PySpark ETL Developer, Cloud Lakehouse Engineer.

---

## 1.5 Canonical Metrics (Verified Phrasing: Use Verbatim, Do Not Drift)

| Claim | Canonical Value | Never Write |
| :--- | :--- | :--- |
| **Pipeline Acceleration** | **40% reduction in average pipeline runtime** via Informatica to Databricks Delta Lake migration | 50%+, 30% |
| **Infrastructure Savings** | **~30% cloud infrastructure cost savings** via cluster rightsizing, auto-scaling, and spot instances | 50%, "slashed cloud bills in half" |
| **Data Parity** | **100% data parity** verified between legacy Informatica feeds and Delta Lake tables | 99%, "mostly identical" |
| **Total Experience** | **3+ years** of enterprise cloud data engineering at Cognizant Technology Solutions | 4+, 2+ |
| **Pipeline SLA Adherence** | **99.8% SLA adherence** across daily critical ETL batches | 100%, 99.999% |
| **Query Latency** | **25% query latency improvement** through SQL indexing and partition pruning | unverified numbers |
| **Executive Dashboards** | **6+ interactive Power BI dashboards** with custom DAX scorecards | "dozens" |
| **Flagship Architecture** | **Delta Lake Medallion Architecture (Bronze, Silver, Gold)** with Databricks Auto Loader | generic data warehouse |
| **Governance Scope** | **Unity Catalog** table/column RBAC + **Informatica MDM** match/merge golden records | "built custom governance" |

---

## 2. Role Archetype to Evidence Matrix

| Role Archetype | Azure Data Engineering | Databricks Lakehouse & Spark | Orchestration (Airflow/ADF) | BI & Power BI Analytics | Governance & MDM |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Azure Data Engineer** | **H** | **H** | **H** | **M** | **M** |
| **Databricks Platform Engineer** | **H** | **H** | **M** | **L** | **H** |
| **PySpark ETL Developer** | **M** | **H** | **H** | **L** | **M** |
| **Analytics / BI Engineer** | **M** | **M** | **M** | **H** | **M** |
| **Data Governance Specialist** | **M** | **H** | **L** | **M** | **H** |

*Legend: **H** = High relevance (lead with it), **M** = Medium (support with it), **L** = Low (omit unless asked).*

---

## 3. Flagship Enterprise Experience (Cognizant Technology Solutions)

### Employment Chronology (Feb 2022 to Present)
- **Associate - Data Engineer** (*Oct 2025 to Present*): Leading cloud data lakehouse migration and Medallion architecture execution.
- **Programming Analyst - Data Engineer** (*Feb 2024 to Oct 2025*): Managing enterprise Informatica ETL, MDM match-merge deduplication, and Power BI dashboards.
- **Programmer Analyst Trainee / GenC Intern** (*Feb 2022 to Mar 2024*): Automated SQL data extraction scripts, shell automation, and production ticket resolution.

### Key Production Technical Milestones
1. **Legacy Informatica to Azure Databricks Modernization:**
   - Modernized on-premises Informatica PowerCenter workflows to Azure Databricks Delta Lake.
   - Replaced fixed-schedule batch extraction with incremental Auto Loader streaming from ADLS Gen2.
   - Slashed execution windows by **40%** and reduced compute infrastructure costs by **~30%**.
2. **Medallion Architecture Implementation:**
   - **Bronze:** Append-only raw ingested records via `cloudFiles` with automated schema evolution.
   - **Silver:** PySpark conformed datasets, window function deduplication on primary keys, and Delta Lake ACID merge/upsert.
   - **Gold:** Curated star-schema data marts optimized for Databricks SQL and Power BI Direct Lake queries.
3. **Data Governance with Unity Catalog & Informatica MDM:**
   - Configured Unity Catalog for centralized governance, metadata tracking, and table/column-level RBAC.
   - Refined Informatica MDM trust scores and survivorship rules to create unified golden master customer records.

---

## 4. Verified Certifications & Credentials

- 🏅 **Databricks Certified Data Engineer Associate** (Databricks)
  - Focus: Lakehouse Architecture, Delta Lake transactions, Spark SQL, and Auto Loader data pipelines.
- 🏅 **Microsoft Certified: Power BI Data Analyst Associate** (Microsoft)
- 🏅 **Microsoft Certified: Power Platform Fundamentals** (Microsoft)
  - Focus: DAX calculations, dimensional modeling, and executive KPI scorecards.
- 🏅 **SQL for Data Science** (UC Davis / Coursera)
  - Focus: Window functions, complex subqueries, and analytical profiling.
- 🏅 **Data Modeling in Power BI** (Coursera)
  - Focus: Star schemas, relationship cardinality, and query folding.
- 🏅 **Bachelor of Engineering (B.E.) in Electronics & Communications**
  - Saveetha Engineering College (2018 to 2022) | First Class Honours.

---

## 5. Directory Inventory & Navigation

```
about_dedeepya/
├── INDEX.md                                                 # This file (master evidence map)
├── PROFILE_BRIEF.md                                         # Master snapshot, verified certs, and Amgen dossier
├── job_search_preferences.md                                # Canonical search targets, GCC policy & comp floors
├── LINKEDIN_ENHANCEMENT_ANALYSIS.md                         # Line-by-line LinkedIn PDF audit & enhancement guide
├── Dedeepya_Linkedin.pdf                                    # Raw LinkedIn PDF export (Oct 3, 2026 ground truth)
├── DEDEEPYA_LAKEHOUSE_AND_DATA_ENGINEERING_MEGA_GUIDE.md   # Encyclopedic Daily Study & Interview Textbook
├── RECRUITER_CALLS_PLAYBOOK.md                              # 5-minute recruiter phone screen script & call flow
├── DELTA_LAKE_MEDALLION_ARCHITECTURE_HLD.md                 # Flagship High-Level System Architecture dossier
├── DATA_ENGINEERING_SCENARIO_BANK.md                        # Real-world scenarios (skew, OOM, streaming, MDM)
├── COMPANY_INTERVIEWER_FORENSIC_TEMPLATE.md                 # Pre-interview forensic dossier template
├── INTERVIEW_DEBRIEF_AND_THANK_YOU_PLAYBOOK.md              # 15-min debrief protocol & strategic thank-you templates
├── scripts/
│   ├── scrape_dedeepya_jobs.py                              # Real-time CLI scraper (hyd, chennai, biopharma, bfsi, product)
│   └── generate_interview_dossier.py                        # Automated interview preparation generator
└── interview_dossiers/
    └── bristol-myers-squibb-interview-dossier.md            # Live generated dossier for BMS Hyderabad
```

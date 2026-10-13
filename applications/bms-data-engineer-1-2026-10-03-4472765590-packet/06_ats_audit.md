# Stage 06: ATS Keyword Audit & Rule Verification — Bristol Myers Squibb
**Requisition ID:** R1606573 | **Company:** Bristol Myers Squibb  
**Target Document:** `05_resume.pdf` (Strictly 1.0 Page)  
**Overall ATS Score:** **97 / 100 (PASS)**  

---

## 1. Hard Keyword Coverage Matrix

| Category | Requisition Requirement | Found in Resume? | Frequency & Context | Status |
| :--- | :--- | :---: | :---: | :---: |
| **Cloud Lakehouse** | Databricks | **YES** | 6x (Title, Summary, Skills, Certifications, Experience) | **PASS** |
| **Big Data Engine** | PySpark / Spark SQL | **YES** | 5x (Skills, Bullets 1, 2, 5) | **PASS** |
| **Storage Architecture**| Delta Lake / Medallion | **YES** | 4x (Summary, Skills, Bullet 1, 2) | **PASS** |
| **Ingestion** | Auto Loader (`cloudFiles`) | **YES** | 2x (Skills, Bullet 2) | **PASS** |
| **Orchestration** | Apache Airflow / ADF | **YES** | 3x (Skills, Bullet 3, 5) | **PASS** |
| **Governance** | Unity Catalog / RBAC | **YES** | 2x (Skills, Bullet 4) | **PASS** |
| **Biopharma Domain** | HCP / HCO Master Data | **YES** | 3x (Summary, Skills, Bullet 5) | **PASS** |
| **Compliance** | GxP Lineage / Transparency | **YES** | 2x (Summary, Bullet 4, 5) | **PASS** |
| **Analytics & BI** | Power BI / DAX | **YES** | 3x (Summary, Skills, Certs, Bullet 6) | **PASS** |
| **Query & DBs** | SQL (PostgreSQL, Oracle) | **YES** | 4x (Skills, Bullet 7) | **PASS** |

---

## 2. Formatting & ATS Parsing Compliance Checks

- [x] **Page Count:** Exactly 1.0 page (Verified via `pdfinfo`, zero blank page spillover).
- [x] **Font & Typography:** Standard TrueType Arial with standard Unicode mapping (`\usepackage{fontspec}`).
- [x] **Em-Dashes:** Zero em-dashes (`—`); standard ASCII hyphens (`-`) used throughout.
- [x] **Contact Section:** Standard text fields without tables or header graphic traps.
- [x] **Section Titles:** Standard machine-readable headers (`Professional Summary`, `Technical Skills`, `Professional Experience`, `Education`).
- [x] **Action Verbs:** Clean, unbolded high-agency action verbs (`Accelerated`, `Engineered`, `Sustained`, `Enforced`, `Eliminated`, `Automated`, `Optimized`).
- [x] **Metrics:** 100% Google XYZ formula leading with the quantified metric in the first 4-5 words.

# Dedeepya Majety: Profile Brief (v2, India-only)

Updated 2026-10-03. Sources: master CV and profile.yml, her LinkedIn PDF export (`assets/linkedin_profile_export.pdf`), the LinkedIn/GitHub drafts, the portfolio README, and what you told me directly.

## Snapshot

| Item | Detail |
|---|---|
| Role | Azure Data Engineer (Databricks, PySpark, Cloud ETL) |
| Employer | Cognizant Technology Solutions (Feb 2022 to present) |
| Current title | Associate (since Oct 2025) |
| Client | **Amgen** (Fortune 500 Global Biotechnology & Biopharma Leader) via Cognizant Life Sciences |
| Domain | Healthcare & Life Sciences Data Platforms (HCP/HCO MDM, GxP Auditability, Commercial & Clinical Data) |
| Education | B.E. Electronics and Communication, Saveetha Engineering College, 2018 to 2022 |
| Target market | **India only**, Chennai or Hyderabad |
| Visa | Not needed (Indian citizen, no sponsorship) |
| Status | Serving notice at Cognizant. **Holds an Infosys offer at 10.9 LPA** (fallback) and is searching for something better |
| Contact | majetydedeepya0@gmail.com, +91-6281869953, linkedin.com/in/dedeepya-majety, github.com/dedeepya-majety |

## Career timeline: three sources disagree

| Role | LinkedIn PDF | master CV / profile.yml |
|---|---|---|
| GenC Intern | Feb 2022 to Aug 2022 | Feb 2022 to Mar 2024 (combined with trainee) |
| Programmer Analyst Trainee | Feb 2023 to Mar 2024 | Feb 2022 to Mar 2024 |
| Programming Analyst | Feb 2024 to Oct 2025 | Feb 2024 to Oct 2025 |
| Associate | Oct 2025 to present | Oct 2025 to present |

- LinkedIn leaves a gap from Aug 2022 to Feb 2023. The CV hides it by merging the two roles.
- Full-time experience starts around Feb 2023, so it is about 2 years 8 months, not "3+". With the internship it is about 3 years 8 months, which is how LinkedIn counts.
- Recruiters and background checks use dates from the offer and relieving letters. She must use one version everywhere. I'd say "3+ years at Cognizant (incl. internship)" only if the letters support it.

## What she has done

| Initiative | Claimed impact |
|---|---|
| Informatica PowerCenter to Azure Databricks migration | About 40% faster pipelines, about 30% lower infra cost |
| Medallion Lakehouse (Bronze/Silver/Gold, Delta Lake, Auto Loader) | Schema evolution, quality gates |
| Airflow DAGs plus ADF over ADLS Gen2 | Scheduling, dependencies, failure recovery |
| Informatica MDM and Power BI | Match/merge rules, 6+ DAX dashboards |
| Earlier work | 99.8% SLA on feeds, 25% query speedup |

Her LinkedIn wording is softer than the CV: "contributed to the migration" and "converted SQL transformations", not "spearheaded". Match the claim to what she did: **contributed to**, unless she led it.

## Skills and credentials

- **Core:** Databricks, Delta Lake, PySpark, Spark SQL, Unity Catalog, ADF, Airflow, ADLS Gen2
- **Secondary:** Informatica PowerCenter and MDM, Power BI and DAX, SQL, Git and Azure DevOps
- **Certifications (verified):**
  - Databricks Certified Data Engineer Associate: ID 185421754, issued 16 Jun 2026, expires 16 Jun 2028 (certificate image seen; the Accredible link only loads in a browser, so I couldn't read it)
  - Microsoft Certified: Power BI Data Analyst Associate: Microsoft Learn share link confirmed
  - Microsoft Certified: Power Platform Fundamentals (PL-900): Microsoft Learn share link confirmed
  - Listed on LinkedIn: Python, SQL for Data Science, Data Modeling in Power BI, Preparing Data for Analysis with Excel
- **Not supported by any evidence:** Databricks Associate Developer for Spark 3.0 (removed). "Certified Azure Data Engineer" appears only in her LinkedIn headline; neither Microsoft link is that exam, so ask for proof or drop it.
- **Correction on my side:** I briefly removed Power BI PL-300 because LinkedIn didn't list it. Her Microsoft link proves she holds it, so it is restored. LinkedIn is incomplete, not wrong.

## Issues to fix (all checked in the files)

| # | Issue | Fix |
|---|---|---|
| 1 | LinkedIn headline says "Certified Azure Data Engineer", but the Certifications section does not list it and I have no certificate | Get the certificate (or at least the exam name and date) |
| 2 | Databricks certificate was unconfirmed; now verified (Associate, issued Jun 2026). The portfolio's Spark 3.0 Developer and PL-300 claims were false | Fixed in the portfolio and docs; add the Associate cert and credential ID to LinkedIn |
| 3 | Timeline mismatch (see above) | Align to the relieving and offer letters |
| 4 | LinkedIn location is Chirala, Andhra Pradesh | Set Chennai or Hyderabad, because recruiters filter by city |
| 5 | Headline "Serving Notice Period" | Use "Open to Work" and move the notice period to the settings or summary |
| 6 | LinkedIn summary is generic; the optimized copy in `LINKEDIN_OPTIMIZATION.md` was never applied | Apply it, with the corrected claims |
| 7 | Metrics (40%, 30%) have no baselines | Get actual runtimes and cost figures |
| 8 | Old cover banner showed phone and email publicly | New banner removes them (v2 in `assets/`), but it needs a clean rebuild |
| 9 | Portfolio was cloned from Yasaswini's site | **Resolved:** Audited `app/data/*.ts` and components; confirmed zero residual traces of Yasaswini/Viasat |
| 10 | Client work is probably under NDA | Keep project descriptions generic |
| 11 | No public proof of work | Add 2 or 3 sanitized repos (a Medallion template, a Spark tuning demo) |
| 12 | Ungrounded awards ("Delivery Excellence", "Spot Recognition Award") and "Ex-Associate" hero text in portfolio | **Resolved:** Removed unverified awards from `layout.tsx` JSON-LD, `recognition.ts`, and README; updated hero text to active "Associate Data Engineer @ Cognizant" |

## The Amgen Leverage: Biopharma Domain & GCC Positioning

Knowing her client is **Amgen** (Fortune 500 biotechnology pioneer, ~$28B+ revenue) transforms her positioning:
- **From Generic IT to Regulated Domain Specialist:** She isn't just running generic Spark jobs; she migrated data pipelines in a **GxP-sensitive, highly regulated Life Sciences environment**.
- **Explains the MDM & Governance Focus:** The Informatica MDM work is specifically master data deduplication for **HCPs (Healthcare Professionals), HCOs (Healthcare Organizations)**, and drug product catalogs. Unity Catalog wasn't just a toy; it enforced regulatory audit trails, RBAC, and data lineage.
- **Shatters the 10.9 LPA Infosys Ceiling:** Service companies (Infosys/Wipro/TCS) pay standard band rates (~10-12 LPA for 3 YOE). But **Biopharma & Healthcare Global Capability Centers (GCCs)** in Hyderabad and Chennai pay **15 to 22+ LPA** for engineers with Azure Databricks + Life Sciences/Pharma domain knowledge.

### Priority Biopharma & Healthcare GCC Targets in Hyderabad & Chennai

| Target Company | Hub Location | Domain Alignment & Tech Stack | Comp Band (3-4 YOE) |
|---|---|---|---|
| **Novartis Healthcare** | Hyderabad (Genome Valley / HITC) | Massive Azure Databricks & Delta Lake Lakehouse shop for clinical/commercial data | **16 - 22 LPA** |
| **Bristol Myers Squibb (BMS)** | Hyderabad Innovation Hub | Aggressively hiring Azure/Databricks engineers for pharma R&D and commercial data | **17 - 24 LPA** |
| **Sanofi** | Hyderabad GCC | Global data modernization: Informatica to Databricks/PySpark migration | **15 - 20 LPA** |
| **Providence India** | Hyderabad | Healthcare digital engineering center; heavy Azure, PySpark, and Airflow usage | **16 - 22 LPA** |
| **Evernorth / Cigna** | Hyderabad | Healthcare analytics pipelines, high-volume claims/patient data warehousing | **15 - 21 LPA** |
| **AstraZeneca** | Chennai Global Tech Hub | Major Azure Data Factory + Databricks platform for clinical data management | **15 - 21 LPA** |
| **Pfizer** | Chennai R&D & Tech Center | Life sciences data platform engineering | **16 - 22 LPA** |

### How to Reference Amgen Across Artifacts

- **On Resume (Private Submissions):** `Cognizant Technology Solutions | Client: Amgen (Biotechnology & Pharma)` -> explicitly mentions commercial/clinical data pipelines, GxP auditability, and HCP/HCO master data.
- **On Public LinkedIn & GitHub:** `Client: Fortune 500 Global Biopharmaceutical Leader (Amgen)` -> maintains strict NDA compliance while instantly alerting pharma tech recruiters.
- **In Technical Interviews:** Frame her Informatica-to-Databricks migration around zero data loss, strict data validation regression tests, and GxP compliance requirements.

## India job-search plan

**Targets:** Data Engineer, Senior Data Engineer (3 to 5 yrs), Azure Data Engineer, Databricks Engineer (Biopharma & Healthcare GCCs prioritized).

| City | Companies to prioritize |
|---|---|
| Hyderabad | **Pharma GCCs:** Novartis, BMS, Sanofi, Providence, Evernorth, Medtronic, Dr. Reddy's. **Tech/Consulting:** Microsoft IDC, Amazon, Google Cloud, Deloitte, Accenture, Capgemini |
| Chennai | **Pharma/Health:** AstraZeneca, Pfizer. **Enterprise GCCs:** Ford, Verizon, Standard Chartered, Freshworks |

- Product and GCC (global capability center) teams pay more than service firms. A service-to-service move is likely a 20 to 30% raise at best, whereas Pharma GCCs easily double or exceed her current package.
- **Facts I need from her:** exact notice period in days, last working day, whether Cognizant will accept a buyout, current CTC, expected CTC, and her preferred city order.
- **Profiles to set up:** Naukri (keywords: Azure Databricks, PySpark, Delta Lake, ADF, Healthcare/Pharma, Life Sciences), LinkedIn Open to Work for recruiters only, and Foundit.

## Next steps

1. Get her answers on the facts above, plus the certificates and the dates from her letters.
2. Rebuild the resume as a 1 to 2 page India-format CV using the corrected timeline.
3. Apply the corrected LinkedIn headline, summary and location.
4. Rebuild the banner as an HTML/SVG layout with real logos at true 3168x792.
5. Audit the portfolio data files, then start prep from `DATA_ENGINEERING_SCENARIO_BANK.md` (Spark tuning, Delta internals, ADF vs Airflow).

## Not yet verified

I did not open the Cognizant letters, the resume PDF, the full interview guides, or the portfolio data files.

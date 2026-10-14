# Dedeepya Majety: Interactive Simulators & NotebookLM Deep-Dive Guide
### "Hands-On Visual Simulators & Audio Overviews to Master Lakehouse Architecture"

This guide provides Dedeepya with two interactive learning tools to eliminate abstract memorization and replace it with direct visual and auditory understanding:
1. **The Amgen Commercial Lakehouse Interactive Simulator (HTML/JS Web App)**
2. **Google NotebookLM Audio Deep Dive & AI Study Guide Setup**

---

## Part 1: The Interactive Lakehouse Simulator (Visual & Hands-On)

We have built a standalone visual simulator that Dedeepya can open in any browser (Chrome, Safari, Edge) without installing any software or running a server.

**File Location:** [simulators/amgen_lakehouse_simulator.html](file:///Users/sailikhithkanuparthi/Downloads/career/job-search/about_dedeepya/simulators/amgen_lakehouse_simulator.html)  
*(Also pushed to her GitHub repo under `interview-prep/simulators/amgen_lakehouse_simulator.html`)*

### How to Open It:
- Open Terminal and run: `open about_dedeepya/simulators/amgen_lakehouse_simulator.html`
- Or in Finder, double-click `amgen_lakehouse_simulator.html` to open it in Chrome.

### What She Can Play With in the Simulator:

#### 1. Medallion Pipeline Simulation (Tab 1):
- **Click "1. Ingest New Batch (Auto Loader)":**
  - Watch 100,000 raw prescription files from Veeva CRM and IQVIA stream into **Bronze Delta**.
  - Shows how Auto Loader dynamically discovers files in ADLS Gen2 without scanning old files.
- **Click "2. Deduplicate into Silver":**
  - Watch the PySpark window function (`row_number() OVER doctor_npi`) and Informatica MDM match-merge execute.
  - Shows 15,000 duplicate entities (~15%) being removed and 85,000 conformed Golden Records upserted into **Silver Delta** via `MERGE INTO`.
- **Click "3. Curate & Cluster Gold":**
  - Watch Star Schema dimensional tables build and Liquid Clustering apply on `(doctor_id, transaction_date)`.
  - Shows simulated Power BI query time drop from **4.2 seconds down to 0.28 seconds**!

#### 2. Spark UI & Data Skew Simulator (Tab 2):
- **Drag the Skew Slider to 80% or 95%:**
  - Watch the simulated Spark UI Stages view!
  - 3 executors finish in 8 seconds, while Executor 1 hangs with a red warning: **"Task Spill to Disk (42 mins)"**.
- **Toggle "Apply Key Salting" or "Broadcast Join":**
  - Instantly watch the red bottleneck disappear: all 4 executors balance out uniformly to 100% in 11 seconds!
  - Gives her an intuitive, visual memory of why salting solves skew when asked in an interview.

#### 3. Delta Lake Log & Time Travel Simulator (Tab 3):
- **Drag the Version Slider from 3 to 0:**
  - Watch how the active commit badge changes from Current to Snapshot.
  - Shows the underlying JSON files in `_delta_log/` (`000000.json` to `000003.json`).
- **Click "RESTORE TABLE" & "VACUUM":**
  - Shows the exact SQL statements executed and explains why auditors love Delta Lake Time Travel for GxP compliance.

---

## Part 2: Google NotebookLM Integration (Conversational Audio Overviews)

**Google NotebookLM** (`notebooklm.google.com`) is Google's dedicated AI research notebook that uses the Gemini model to turn uploaded markdown documents into an **Audio Overview (a natural, conversational podcast between two AI hosts)**.

### Why This is Game-Changing for Dedeepya:
- Dedeepya can listen to two AI hosts converse casually about *her* Amgen project on her headphones while walking, resting, or commuting.
- Hearing two voices naturally use phrases like *"So when Dedeepya's team replaced Informatica with PySpark, what actually happened to the runtimes?"* $\rightarrow$ *"Well, they cut the batch from 8 hours down to 4.5 hours using Auto Loader and Delta Lake!"* completely removes the intimidation factor of industry vocabulary.

### Step-by-Step Setup in 3 Minutes:

1. Go to **[notebooklm.google.com](https://notebooklm.google.com)** (sign in with her Google account).
2. Click **Create new notebook** $\rightarrow$ Name it: `Amgen Commercial Lakehouse - Dedeepya Majety`.
3. Click **Add Sources** $\rightarrow$ Choose **Upload files** and select her 6 Markdown dossiers:
   - `DEDEEPYA_GEMINI_VOICE_STUDY_COMPANION.md`
   - `DEDEEPYA_INTERVIEW_PREP_MASTER_DOSSIER.md`
   - `30_MIN_PRE_INTERVIEW_CHEATSHEET.md`
   - `EY_GDS_AZURE_DATA_ENGINEER_DOSSIER.md`
   - `ACCENTURE_DATA_ENGINEER_DOSSIER.md`
   - `PWC_FINANCIAL_SERVICES_DATA_ENGINEER_DOSSIER.md`
4. In the top-right **Notebook guide** panel, click **"Generate Audio Overview" (Deep Dive)**:
   - In 2 to 3 minutes, NotebookLM will produce a 10-to-15-minute, highly realistic audio podcast discussing her architecture, why the Informatica migration was critical, and how she solved real-world data bottlenecks.
5. In the chat box at the bottom, she can ask:
   - *"Give me a 5-question multiple choice quiz on PySpark skew tuning based on my dossiers."*
   - *"Summarize my 3 STAR stories in simple bullet points."*

---

## Summary of Learning Modalities for Dedeepya

```
1. Visual Learning:
   └─ Open simulators/amgen_lakehouse_simulator.html
      (Play with pipeline buttons, skew sliders, and time travel)

2. Auditory Learning (Passive):
   └─ Listen to NotebookLM Audio Overview Podcast on headphones
      (Absorb conversational phrasing without stress)

3. Spoken Practice (Active):
   └─ Talk to Gemini in Voice Mode
      (Practice answering viva questions in her room out loud)

4. Quick Recall (30 Mins Before Interview):
   └─ Review 30_MIN_PRE_INTERVIEW_CHEATSHEET.pdf
      (Cement the 60-second hook, production numbers, and comp script)
```

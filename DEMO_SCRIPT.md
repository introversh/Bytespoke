# 5–8 Minute Video Demo Presentation Script
**Candidate:** Shivankar Tripathi  
**Assignment ID:** DAI-045 | **Primary Track:** DataOps  
**Assignment Title:** Dependency-Aware Orchestration with Telemetry & Auto-Recovery  

---

### Timing & Scene Breakdown

| Timestamp | Section | Visual on Screen | Key Talking Points |
| :---: | :--- | :--- | :--- |
| **0:00 – 1:00** | **Introduction & Architecture Overview** | Architecture diagram in `README.md` / `DESIGN_DECISION_NOTE.md` | • Introduce self: "Hi, I'm Shivankar Tripathi presenting Assignment DAI-045 for the DataOps track."<br/>• Explain the core challenge: handling upstream transient failures without data loss or pipeline crashes.<br/>• Present the 4-step DAG: Ingest, Validate & Enrich (with retries), Conditional Branch (Standard vs Quarantine), and Finalize. |
| **1:00 – 2:30** | **Code & Architecture Walkthrough** | VS Code / IDE showing `pipeline/engine.py`, `db/schema.sql`, `ai/interface.py` | • **DAG Engine (`pipeline/engine.py`)**: Highlight topological sorting, cycle detection, and `dependency_mode='ONE_OF'` for diamond joins.<br/>• **SQL Relational Store (`db/schema.sql`)**: Show `batches`, `transactions`, `pipeline_runs`, `step_retries`, and views.<br/>• **AI Isolation (`ai/interface.py`)**: Explain how AI calls are isolated behind `AbstractRCAEnhancer` with a 100% offline fallback. |
| **2:30 – 4:15** | **Live Execution & Failure/Recovery Demo** | Terminal running `python run_pipeline.py` | • Run command: `python run_pipeline.py`.<br/>• Point out Step 1: Ingests 50 synthetic records from CSV into SQLite.<br/>• Point out Step 2: Hits simulated upstream 503 timeout on attempt 1, backs off, retries on attempt 2, and recovers on attempt 3.<br/>• Point out Step 3: Notice the conditional branch! Standard path was skipped; Quarantine & Audit path executed due to degraded recovery.<br/>• Point out Step 4: Finalize seals the batch with cryptographic hash `SEAL-...`. |
| **4:15 – 5:30** | **SQL Operational Database Inspection** | SQLite query in terminal or DB Browser | • Run quick query: `SELECT * FROM v_pipeline_run_summary;`<br/>• Show how the run, attempt count, latencies, and step statuses are logged directly in SQL tables.<br/>• Query `transactions`: Show how high-risk and degraded transactions are marked `QUARANTINED` while safe records are `SETTLED`. |
| **5:30 – 6:30** | **RCA Note & Operational Metadata** | Showing `artifacts/RCA_INCIDENT_REPORT.md` and `artifacts/operational_metadata.json` | • Walk through generated SRE Root Cause Analysis document: Incident ID, Severity (SEV-2), Executive Summary.<br/>• Show the **Chronological Event Timeline** and **5-Whys Root Cause Deductions**.<br/>• Review the **Corrective and Preventive Actions (CAPA)** table with P0/P1/P2 priorities and owners. |
| **6:30 – 7:15** | **Failure Modes & Test Suite Execution** | Terminal running `python demo_failure_modes.py` and `python -m unittest discover -s tests -v` | • Run `python demo_failure_modes.py` showing all 4 scenarios: Clean run, Transient recovery, Retries exhaustion with fallback handler, and Corrupted payload.<br/>• Run `python -m unittest discover -s tests -v` showing all 10 unit and integration tests passing. |
| **7:15 – 7:45** | **Conclusion & Wrap-Up** | GitHub repo and README | • Summarize key takeaways: Clean setup, zero secrets, Python + SQL, resilient DAG orchestration, automated self-healing, and SRE RCA generation.<br/>• Thank the reviewer for their time. |

---

### Recording Checklist for Shivankar Tripathi

- [ ] Ensure terminal font size is comfortable (16-18pt).
- [ ] Set audio input clearly with no background noise.
- [ ] Test command execution beforehand:
  ```bash
  python run_pipeline.py
  python demo_failure_modes.py
  python -m unittest discover -s tests -v
  ```
- [ ] Keep presentation concise, confident, and focused on architectural decisions and resilience verification.

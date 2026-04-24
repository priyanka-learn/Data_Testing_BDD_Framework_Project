# BDD Practice Project — FHIR Patient Data Validation

A behaviour-driven test framework that validates FHIR R4 patient data loaded into **Databricks** against local JSON source files.  
Built with **Python + Behave** and designed to run against a Databricks SQL Warehouse, classic Cluster, or Serverless compute.

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Configuration](#configuration)
- [Running Tests](#running-tests)
- [Test Scenarios](#test-scenarios)
- [Reporting](#reporting)
- [Tech Stack](#tech-stack)

---

## Overview

The project reads sample FHIR patient JSON files from a local `testdata/` folder, loads the corresponding tables from a Databricks workspace, and asserts that:

- Row counts match between the source JSON and the Databricks table.
- Individual patient fields (family name, given name, gender, birth date, active status) are correctly populated.
- All gender values conform to the FHIR R4 allowed set (`male`, `female`, `other`, `unknown`).

---

## Project Structure

```
BDD_Practice_Project/
├── features/
│   ├── patient.feature              # BDD scenarios (Gherkin)
│   ├── environment.py               # Behave hooks (before_all, before_feature)
│   ├── constants/
│   │   └── patient_columns.py       # Column name constants
│   ├── implementation/
│   │   └── common.py                # Shared assertion helpers
│   └── steps/
│       └── step_patient.py          # Step definitions
├── configs/
│   ├── config.py                    # Singleton config reader (application.properties)
│   └── setup_logging.py             # Logging setup
├── services/
│   ├── databricks_cluster_service.py
│   ├── databricks_job_service.py
│   └── databricks_workspace_service.py
├── utils/
│   ├── databricks_utils.py          # Table reads, row counts, notebook management
│   └── spark_session_manager.py     # Local PySpark session
├── testdata/
│   ├── fhir_patients.json           # Sample FHIR Patient resources
│   └── fhir_encounters.json         # Sample FHIR Encounter resources
├── reports/
│   └── allure-report/               # Pre-generated Allure HTML report
├── application.properties.example   # Config template (copy → application.properties)
├── .behaverc                        # Behave runtime options
└── Pipfile                          # Python dependencies
```

---

## Prerequisites

| Tool | Version |
|------|---------|
| Python | 3.12 |
| pipenv | latest |
| Databricks workspace | any tier (Serverless, Warehouse, or Cluster) |
| Java (for PySpark) | 8 or 11 |

---

## Setup

1. **Clone the repository**

   ```bash
   git clone <repo-url>
   cd BDD_Practice_Project
   ```

2. **Install dependencies**

   ```bash
   pip install pipenv
   pipenv install
   ```

3. **Configure credentials** (see [Configuration](#configuration) below)

---

## Configuration

Copy the example config and fill in your Databricks details:

```bash
cp application.properties.example application.properties
```

`application.properties` is git-ignored and must never be committed.

### Key settings

```ini
[FOLDERS]
TESTDATA_PATH=testdata/

[DATABRICKS]
HOST=https://<your-workspace>.azuredatabricks.net
TOKEN=<your-databricks-personal-access-token>
CATALOG_NAME=<your-unity-catalog>      # leave blank for Hive metastore
DATABASE_NAME=<your-schema>

# COMPUTE_MODE: SERVERLESS | WAREHOUSE | CLUSTER
COMPUTE_MODE=SERVERLESS
CLUSTER_TYPE=DEFAULT

[WAREHOUSES]
DEFAULT=<warehouse-id>   # required when COMPUTE_MODE=WAREHOUSE

[CLUSTERS]
DEFAULT=<cluster-id>     # required when COMPUTE_MODE=CLUSTER
```

**Finding your Warehouse ID:**  
SQL Warehouses → select warehouse → Connection Details → HTTP Path  
`/sql/1.0/warehouses/abc123def456` → ID is `abc123def456`

---

## Running Tests

```bash
# Run all scenarios
pipenv run behave

# Run with verbose output
pipenv run behave --no-capture

# Run a specific feature file
pipenv run behave features/patient.feature

# Run a specific scenario by tag (if tagged)
pipenv run behave --tags=@smoke
```

Test output format is configured in [.behaverc](.behaverc) (`format = plain`, with all capture disabled for real-time logging).

---

## Test Scenarios

All scenarios are defined in [features/patient.feature](features/patient.feature).

### 1. Record Count Validation

> Verifies the number of patients in the source JSON matches the Databricks table row count.

```gherkin
Scenario Outline: Validate patient record count matches between source JSON and Databricks table
  Given the <resource> patient <source_json_file> is loaded
  When the patient data is read from the Databricks <target_table> table
  Then the <source_json_file> data count should match the Databricks <target_table> table count
```

### 2. Full Field Validation (by Patient ID)

> Checks that family name, given name, gender, birth date, and active status are correctly stored for specific patients.

```gherkin
Scenario Outline: Validate full patient record in Databricks matches JSON by patient ID
  Then the patient data in <target_table> table should be correctly populated with
       family name, given name, gender, birth date, and active status
```

### 3. FHIR Gender Validation

> Asserts every patient row in Databricks has a valid FHIR R4 gender value.

```gherkin
Scenario: Validate all patients in JSON have a valid FHIR gender value
  Then all patients in the Databricks fhir_patients table should have gender in male, female, other, unknown
```

---

## Reporting

A pre-generated Allure report is available in the [`docs/`](docs/) folder and is published via GitHub Pages.

To generate a fresh report after a test run:

```bash
# Install Allure CLI (https://docs.qameta.io/allure/#_installing_a_commandline)
pipenv run behave -f allure_behave.formatter:AllureFormatter -o reports/allure-results

allure generate reports/allure-results -o reports/allure-report --clean
allure open reports/allure-report
```

---

## Tech Stack

| Library | Purpose |
|---------|---------|
| [Behave](https://behave.readthedocs.io/) | BDD test runner (Gherkin syntax) |
| [databricks-sql-connector](https://docs.databricks.com/dev-tools/python-sql-connector.html) | SQL Warehouse / JDBC queries |
| [PySpark](https://spark.apache.org/docs/latest/api/python/) | Local JSON → DataFrame parsing |
| [pandas](https://pandas.pydata.org/) | Data manipulation helpers |
| [requests](https://requests.readthedocs.io/) | Databricks REST API calls |

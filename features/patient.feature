Feature: Patient FHIR data validation

  # ─── Source vs Databricks count check ───────────────────────────────────────

  Scenario Outline: Validate patient record count matches between source JSON and Databricks table
    Given the <resource> patient <source_json_file> is loaded
    When the patient data is read from the Databricks <target_table> table
    Then the <source_json_file> data count should match the Databricks <target_table> table count

    Examples:
      | resource | source_json_file   | target_table  |
      | patient  | fhir_patients.json | fhir_patients |

  # # ─── Individual patient record validation (by patientId) ────────────────────

  Scenario Outline: Validate full patient record in Databricks matches JSON by patient ID
    Given the <resource> patient <source_json_file> is loaded
    When the patient data is read from the Databricks <target_table> table
    Then the patient data in <target_table> table should be correctly populated with family name, given name, gender, birth date, and active status
    | patient_id                           | family | given    | gender  | birth_date | active |
    | 752c3eef-206b-4351-8d4d-389786bc3696 | Avils  | Rafa     | unknown | 2004-11-24 | false  |
    | c12565df-460b-4638-9a09-8da3ce397625 | Valero | Teófila  | female  | 2015-09-29 | true   |
    | d9a7b34e-7887-4bd6-9ee5-6dce6a7cd311 | Puente | Graciana | male    | 1974-02-11 | true   |
    | 6cb73d9e-ba7c-490e-848d-99b2126204c6 | Lara   | Berta    | other   | 1992-09-04 | false  |
    | 42802539-367d-4d03-8b5b-9880dbabd106 | Ropero | Isidora  | female  | 1939-10-24 | false  |

    Examples:
      | resource | source_json_file   | target_table  |
      | patient  | fhir_patients.json | fhir_patients |

# # ─── Gender validation ───────────────────────────────────────────────────────
Scenario: Validate all patients in JSON have a valid FHIR gender value
  Given the patient patient fhir_patients.json is loaded
  When the patient data is read from the Databricks fhir_patients table
  Then all patients in the Databricks fhir_patients table should have gender in male, female, other, unknown


# Scenario: Validate all patients in Databricks have a valid FHIR gender value
#   When the patient data is read from the Databricks fhir_patients table
#   Then all patients in the Databricks table should have gender in "male, female, other, unknown"

# # ─── birthDate validation ────────────────────────────────────────────────────

# Scenario: Validate all patients in JSON have a valid birthDate
#   Then all patients in the JSON should have a valid birthDate in YYYY-MM-DD format

# Scenario: Validate birthDate is not in the future for any patient
#   Then no patient in the JSON should have a birthDate in the future

# Scenario: Validate all patients in Databricks have a valid birthDate
#   When the patient data is read from the Databricks fhir_patients table
#   Then all patients in the Databricks table should have a valid birthDate in YYYY-MM-DD format

# # ─── Required field presence ─────────────────────────────────────────────────

# Scenario: Validate required FHIR fields are present for all patients in JSON
#   Then every patient in the JSON should have the required fields "resourceType, identifier, name, gender, birthDate"

# Scenario Outline: Validate a specific required field is not null or empty in JSON
#   Then the field "<field>" should not be null or empty for any patient in the JSON

#   Examples:
#     | field        |
#     | resourceType |
#     | identifier   |
#     | name         |
#     | gender       |
#     | birthDate    |
#     | active       |

# # ─── Identifier uniqueness ───────────────────────────────────────────────────

# Scenario: Validate patient identifiers are unique and not null in Databricks
#   When the patient data is read from the Databricks fhir_patients table
#   Then all patient identifier values in the Databricks table should be unique and not null

# # ─── resourceType validation ─────────────────────────────────────────────────

# Scenario: Validate resourceType is "Patient" for all records in JSON
#   Then all records in the JSON should have resourceType equal to "Patient"



# # ─── Address validation ──────────────────────────────────────────────────────

# Scenario: Validate patient postalCode is numeric
#   Then every patient postalCode in the JSON should contain only numeric characters


# # Change sample fhir patient file to fhir r4 sample file

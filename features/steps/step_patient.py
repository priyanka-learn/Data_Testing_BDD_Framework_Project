from behave import *
import logging
from pathlib import Path

from configs.config import Config
from utils import databricks_utils

import json

from features.implementation.common import verify_expected_values, verify_column_in_allowed_values
from features.constants.patient_columns import PATIENT_COLUMNS

logger = logging.getLogger(__name__)
config = Config()

# Step definitions for patient record count matches between source JSON file and Databricks table
@given("the {resource} patient {source_json_file} is loaded")
def step_patient_file_check(context, resource, source_json_file):
    # Store all values on context for use in When / Then steps
    context.resource         = resource
    context.source_json_file = source_json_file

    # Build file path: TESTDATA_PATH (from config) + source_json_file (from Examples table)
    testdata_dir  = config.get('FOLDERS', 'TESTDATA_PATH')
    project_root  = Path(config.get_root_directory())
    path          = (project_root / testdata_dir / source_json_file).resolve()

    context.source_json_path = path

    assert path.exists(), f"Source JSON file does not exist: {path}"
    logger.info(f"Source JSON file found: {path}")


@when("the patient data is read from the Databricks {target_table} table")
def step_read_from_databricks(context, target_table):
    context.target_table = target_table
    logger.info(f"Reading data from Databricks table: {target_table}")
    # Here you would add code to connect to Databricks and read the table into a DataFrame
    # For example:
    context.df = databricks_utils.read_table_to_df(target_table)
    assert context.df is not None, f"Failed to read data from Databricks table: {target_table}"
    logger.info(f"Data read successfully from Databricks table: {target_table}")


@then("the {source_json_file} data count should match the Databricks {target_table} table count")
def step_validate_data(context, source_json_file, target_table):
    logger.info(f"Validating data count between source JSON file: {source_json_file} and Databricks table: {target_table}")
    # Compare the count of rows in the source JSON file and the Databricks table
    with open(context.source_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        source_count = len(data)

    target_count = databricks_utils.get_table_row_count(target_table)
    assert source_count == target_count, f"Data count mismatch: {source_count} (JSON) != {target_count} (Databricks)"
    logger.info(f"Data count validation successful: {source_count} rows in both source and target")


# Validate full patient record in Databricks matches source JSON file
@then("the patient data in {target_table} table should be correctly populated with family name, given name, gender, birth date, and active status")
def step_validate_patient_data(context, target_table):
    logger.info("Validating patient data fields between source JSON and Databricks table")
    for row in context.table:
        patient_id = row['patient_id']
        expected_family = row['family']
        expected_given = row['given']
        expected_gender = row['gender']
        expected_birth_date = row['birth_date']
        expected_active = row['active'].lower()  # 'true' or 'false' string

        verify_expected_values(
            target_table, PATIENT_COLUMNS,
            primary_key_filter={'patient_id': patient_id},
            expected_values={
                'family':     expected_family,
                'given':      expected_given,
                'gender':     expected_gender,
                'birth_date': expected_birth_date,
                'active':     expected_active,
            }
        )


@then("all patients in the Databricks {fhir_patients} table should have gender in {male}, {female}, {other}, {unknown}")
def step_validate_gender(context, fhir_patients, male, female, other, unknown):
    logger.info("Validating that all patients in Databricks table have valid FHIR gender values")
    allowed_gender_values = [male, female, other, unknown]
    verify_column_in_allowed_values(fhir_patients, "gender", allowed_gender_values)
import logging
from databricks import sql as dbsql
from configs.config import Config

logger = logging.getLogger(__name__)
config = Config()


def _get_warehouse_connection_params():
    """Return (host, token, http_path) for the configured SQL Warehouse."""
    host         = config.get_host()
    token        = config.get_token()
    warehouse_id = config.get_warehouse_id()
    return host, token, f"/sql/1.0/warehouses/{warehouse_id}"


def _build_full_table_name(table_name):
    """Return fully-qualified table name as catalog.database.table (or shorter if not configured)."""
    catalog  = config.get_catalog_name() or ""
    database = config.get_database_name() or ""
    if catalog and database:
        return f"{catalog}.{database}.{table_name}"
    elif database:
        return f"{database}.{table_name}"
    return table_name


def verify_expected_values(table_name, columns, primary_key_filter, expected_values):
    try:
        host, token, http_path = _get_warehouse_connection_params()
        full_table = _build_full_table_name(table_name)

        # columns is a dict of {alias: sql_expression} to support nested FHIR columns.
        # e.g. {'patient_id': 'identifier[0].value', 'family': 'name[0].family', ...}
        cast_select = ", ".join(
            f"CAST({expr} AS STRING) AS `{alias}`"
            for alias, expr in columns.items()
        )

        # Build WHERE clause: look up the SQL expression for each filter alias
        where_parts = [
            f"CAST({columns[alias]} AS STRING) = '{value}'"
            for alias, value in primary_key_filter.items()
        ]
        where_clause = " AND ".join(where_parts)
        query = f"SELECT {cast_select} FROM {full_table} WHERE {where_clause}"

        logger.info(f"Validating row in {full_table} with filter {primary_key_filter}")

        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                col_names = [desc[0] for desc in cursor.description]

        row_count = len(rows)
        if row_count != 1:
            logger.error(f"Expected exactly one record for filter {primary_key_filter}, but found {row_count}")
            raise AssertionError(f"Expected exactly one record for filter {primary_key_filter}, but found {row_count}")

        row = dict(zip(col_names, rows[0]))

        # Compare expected vs actual — normalize both to lowercase strings.
        # All columns are cast to STRING so actual values are always str or None.
        for expected_column, expected_value in expected_values.items():
            actual_value = row.get(expected_column)

            expected_str = str(expected_value).lower() if expected_value is not None else None
            actual_str   = str(actual_value).lower()   if actual_value   is not None else None

            if actual_str is None and expected_str is None:
                logger.warning(f"Both actual and expected values are None for column '{expected_column}' in {table_name} table")
                continue

            if actual_str != expected_str:
                logger.error(f"Mismatched values for column '{expected_column}' in {table_name} table: expected '{expected_str}', actual '{actual_str}'")
                raise AssertionError(f"Mismatched values for column '{expected_column}' in {table_name} table: expected '{expected_str}', actual '{actual_str}'")
            else:
                logger.info(f"Value match for column '{expected_column}' in {table_name} table: expected '{expected_str}', actual '{actual_str}'")

    except AssertionError:
        raise
    except Exception as e:
        logger.error(f"Error validating expected values in {table_name} table: {str(e)}")
        raise AssertionError(f"Error validating expected values in {table_name} table: {str(e)}")


def verify_column_in_allowed_values(table_name, column, allowed_values):
    try:
        host, token, http_path = _get_warehouse_connection_params()
        full_table = _build_full_table_name(table_name)

        query = f"SELECT CAST(`{column}` AS STRING) AS `{column}` FROM {full_table}"
        logger.info(f"Validating all '{column}' values in {full_table} are in {allowed_values}")

        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

        allowed_lower = {str(v).lower() for v in allowed_values}
        failures = []
        for row in rows:
            actual = row[0]
            actual_str = str(actual).lower() if actual is not None else None
            if actual_str not in allowed_lower:
                failures.append(actual)

        if failures:
            logger.error(f"Invalid '{column}' values found in {table_name}: {failures}")
            raise AssertionError(f"Invalid '{column}' values found in '{table_name}': {failures}")

        logger.info(f"All '{column}' values in '{table_name}' are valid: {allowed_values}")

    except AssertionError:
        raise
    except Exception as e:
        logger.error(f"Error validating '{column}' values in {table_name} table: {str(e)}")
        raise AssertionError(f"Error validating '{column}' values in {table_name} table: {str(e)}")
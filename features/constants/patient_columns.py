# Mapping of alias -> SQL expression for FHIR nested columns in fhir_patients table.
# patient_id lives inside identifier array-of-structs; name fields inside name array-of-structs.
PATIENT_COLUMNS = {
    'patient_id': 'identifier[0].value',
    'family':     'name[0].family',
    'given':      'name[0].given[0]',
    'gender':     'gender',
    'birth_date': 'DATE_FORMAT(birthDate, "yyyy-MM-dd")',
    'active':     'active',
}
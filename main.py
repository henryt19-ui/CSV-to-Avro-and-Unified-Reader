import pandas as pd
from fastavro import (
    parse_schema,
    reader,
    writer,
)

# The File paths
csv_file1 = 'transactions_v1.csv'
csv_file2 = 'transactions_v2.csv'
avro_file1 = 'transactions1.avro'
avro_file2 = 'transactions2.avro'

# ------------------------------
# Converting CSV to Avro


def csv_to_avro(csv_path, avro_path, schema):
    df = pd.read_csv(csv_path)
    records = df.to_dict(orient='records')
    parsed_schema = parse_schema(schema)
    with open(avro_path, 'wb') as out:
        writer(out, parsed_schema, records)


# ------------------------------
# First Schema
schema1 = {
    'doc': 'A transaction reading.',
    'name': 'Transactions1',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'event_time', 'type': 'string'},
        {'name': 'user_email', 'type': 'string'},
        {'name': 'amount', 'type': 'double', 'aliases': ['total_amount']},
    ],
}

# Second Schema
schema2 = {
    'doc': 'A transaction reading.',
    'name': 'Transactions2',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'event_time', 'type': 'string'},
        {'name': 'total_amount', 'type': 'double'},
        {'name': 'currency', 'type': 'string'},
    ],
}

# Converting CSV files to Avro files
csv_to_avro(csv_file1, avro_file1, schema1)
csv_to_avro(csv_file2, avro_file2, schema2)

# ------------------------------
#  The Unified Reader with no branching logic


def unified_reader(avro_path):
    with open(avro_path, 'rb') as f:
        for record in reader(f):

            unified = {
                'id': record.get('id'),
                'event_time': record.get('event_time'),
                'total_amount': record.get('total_amount') or record.get('amount'),
                'user_email': record.get('user_email'),
                'currency': record.get('currency'),
            }

            print(unified)


# ------------------------------
# Display final unified output
print()
unified_reader(avro_file1)

print()
unified_reader(avro_file2)

'''
This script uses PyIceberg to create an Iceberg table where we add new partition fields.
See TESTING.md for how to run this script.
'''

import pyarrow as pa
from datetime import datetime
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import TimestampType, FloatType, DoubleType, StringType, NestedField, StructType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform, IdentityTransform

catalog = SqlCatalog(
    "pyiceberg-3",
    **{
        "uri": f"sqlite:///dh-iceberg-test.db",
        "warehouse": f"catalogs/pyiceberg-3",
    },
)

schema = Schema(
    NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=False),
    NestedField(field_id=2, name="symbol", field_type=StringType(), required=False),
    NestedField(field_id=3, name="bid", field_type=DoubleType(), required=False),
    NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
)

partition_spec = PartitionSpec(
    PartitionField(
        source_id=2, field_id=1001, transform=IdentityTransform(), name="symbol",
    )
)

catalog.create_namespace_if_not_exists("trading")

# Generate some data to be added to the tables later
data = pa.Table.from_pylist([
    {"datetime": datetime(2024, 11, 27, 10, 0, 0), "symbol": "AAPL", "bid": 150.25, "ask": 151.0},
    {"datetime": datetime(2022, 11, 27, 10, 0, 0), "symbol": "MSFT", "bid": 150.25, "ask": 151.0},
])

more_data = pa.Table.from_pylist([
    {"datetime": datetime(2022, 11, 26, 10, 1, 0), "symbol": "GOOG", "bid": 2800.75, "ask": 2810.5},
    {"datetime": datetime(2023, 11, 26, 10, 2, 0), "symbol": "AMZN", "bid": 3400.5, "ask": 3420.0},
    {"datetime": datetime(2025, 11, 28, 10, 3, 0), "symbol": "MSFT", "bid": 238.85, "ask": 250.0},
])

def add_non_identity_partition_field():
    table_identifier = "trading.add_non_identity_partition_field"
    if catalog.table_exists(table_identifier):
        catalog.purge_table(table_identifier)

    tbl = catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        partition_spec=partition_spec,
    )

    tbl.append(data)

    # Add a non-identity partition field
    with tbl.update_spec() as update:
        update.add_field("datetime", YearTransform(), "datetime_year")

    tbl.append(more_data)

def add_identity_partition_field():
    table_identifier = "trading.add_identity_partition_field"
    if catalog.table_exists(table_identifier):
        catalog.purge_table(table_identifier)

    tbl = catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        partition_spec=partition_spec,
    )

    tbl.append(data)

    # Add an identity partition field
    with tbl.update_spec() as update:
        update.add_field("bid", IdentityTransform())

    tbl.append(more_data)

add_non_identity_partition_field()
add_identity_partition_field()
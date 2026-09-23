import pytest

from company_ingestion.config import SourceConfig


def test_table_partition_hints_are_not_an_input_contract():
    with pytest.raises(TypeError):
        SourceConfig(
            type="sqlserver", host="db", database="db", schema="dbo",
            table="x", partitionColumn="id",
        )

from uuid import uuid4
import agate
import re
import boto3
from pyathena.error import OperationalError

from dbt.adapters.base import available
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.relation import AthenaRelation
from dbt.logger import GLOBAL_LOGGER as logger

class AthenaAdapter(SQLAdapter):
    ConnectionManager = AthenaConnectionManager
    Relation = AthenaRelation

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(
        cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "integer"

    @classmethod
    def convert_datetime_type(
            cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        return "timestamp"

    @available
    def s3_uuid_table_location(self):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        return f"{client.s3_staging_dir}tables/{str(uuid4())}/"

    @available
    def clean_up_partitions(
        self, database_name: str, table_name: str, where_condition: str
    ):
        logger.info("database_name='{}', table_name='{}', where_condition='{}'", database_name, table_name, where_condition)

        # Look up Glue partitions & clean up
        glue_client = boto3.client('glue')
        s3_resource = boto3.resource('s3')
        partitions = glue_client.get_partitions(
            # CatalogId='awsdatacatalog',  # Using this caused permission error that 'glue:GetPartitions' is required
            DatabaseName=database_name,
            TableName=table_name,
            Expression=where_condition
        )
        p = re.compile('s3://([^/]*)/(.*)')
        for partition in partitions["Partitions"]:
            logger.debug("Deleting objects for partition '{}' at '{}'", partition["Values"], partition["StorageDescriptor"]["Location"])
            m = p.match(partition["StorageDescriptor"]["Location"])
            if m is not None:
                bucket_name = m.group(1)
                prefix = m.group(2)
                s3_bucket = s3_resource.Bucket(bucket_name)
                s3_bucket.objects.filter(Prefix=prefix).delete()


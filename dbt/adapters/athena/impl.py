import re
from typing import List
from uuid import uuid4

import agate
from botocore.exceptions import ClientError
from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.relation import AthenaRelation
from dbt.adapters.base import available
from dbt.adapters.base.column import Column
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.relation import RelationType
from dbt.logger import GLOBAL_LOGGER as logger


class AthenaAdapter(SQLAdapter):
    ConnectionManager = AthenaConnectionManager
    Relation = AthenaRelation
    Column = Column

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "integer"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp"

    def get_columns_in_relation(self, relation: Relation) -> List[Column]:
        """"""
        cached_relations = self.cache.get_relations(relation.database, relation.schema)
        cached_relation = [x for x in cached_relations if str(x) == str(relation)]
        cached_relation = cached_relation[0] if cached_relation else None
        columns = []
        if cached_relation and cached_relation.column_information:
            for col_dict in cached_relation.column_information:
                column = Column.create(col_dict["Name"], col_dict["Type"])
                columns.append(column)
            return columns
        else:
            return super().get_columns_in_relation(relation)

    def list_relations_without_caching(self, schema_relation: AthenaRelation) -> List[AthenaRelation]:
        # results has a 4-tuple, schema(database_name),table_name, schema_name,
        relations = []
        try:
            logger.debug("Get relations through Glue API")
            conn = self.connections.get_thread_connection()
            client = conn.handle
            glue_client = client.session.client("glue")
            paginator = glue_client.get_paginator("get_tables")
            page_iterator = paginator.paginate(DatabaseName=schema_relation.schema, MaxResults=50)
            for page in page_iterator:
                for table in page["TableList"]:
                    table_type = RelationType.View if table["TableType"] == "EXTERNAL_VIEW" else RelationType.Table
                    relation = self.Relation.create(
                        database=schema_relation.database,
                        identifier=table["Name"],
                        schema=schema_relation.schema,
                        type=table_type,
                        column_information=table["StorageDescriptor"]["Columns"],
                    )
                    relations.append(relation)
            return relations
        except ClientError as e:
            logger.debug(
                "Boto3 Error while retrieving relations: code=%s, message=%s",
                e.response["Error"]["Code"],
                e.response["Error"].get("Message"),
            )
            # Fallback to SQL query
            return super().list_relations_without_caching(schema_relation)

    @available
    def s3_uuid_table_location(self):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        return f"{client.s3_staging_dir}tables/{str(uuid4())}/"

    @available
    def clean_up_partitions(self, database_name: str, table_name: str, where_condition: str):
        # Look up Glue partitions & clean up
        conn = self.connections.get_thread_connection()
        client = conn.handle
        glue_client = client.session.client("glue")
        s3_resource = client.session.resource("s3", region_name=client.region_name)
        partitions = glue_client.get_partitions(
            # CatalogId='123456789012', # Need to make this configurable if it is different from default AWS Account ID
            DatabaseName=database_name,
            TableName=table_name,
            Expression=where_condition,
        )
        p = re.compile("s3://([^/]*)/(.*)")
        for partition in partitions["Partitions"]:
            logger.debug(
                "Deleting objects for partition '{}' at '{}'",
                partition["Values"],
                partition["StorageDescriptor"]["Location"],
            )
            m = p.match(partition["StorageDescriptor"]["Location"])
            if m is not None:
                bucket_name = m.group(1)
                prefix = m.group(2)
                s3_bucket = s3_resource.Bucket(bucket_name)
                s3_bucket.objects.filter(Prefix=prefix).delete()

    @available
    def clean_up_table(self, database_name: str, table_name: str):
        # Look up Glue partitions & clean up
        conn = self.connections.get_thread_connection()
        client = conn.handle
        glue_client = client.session.client("glue")

        try:
            table = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                logger.debug("Table '{}' does not exists - Ignoring", table_name)
                return

        if table is not None:
            logger.debug("Deleting table data from'{}'", table["Table"]["StorageDescriptor"]["Location"])
            p = re.compile("s3://([^/]*)/(.*)")
            m = p.match(table["Table"]["StorageDescriptor"]["Location"])
            if m is not None:
                bucket_name = m.group(1)
                prefix = m.group(2)

                s3_resource = client.session.resource("s3")
                s3_bucket = s3_resource.Bucket(bucket_name)
                s3_bucket.objects.filter(Prefix=prefix).delete()

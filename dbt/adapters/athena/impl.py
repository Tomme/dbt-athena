import itertools
import re
from email.quoprimime import quote
from typing import List, Set
from uuid import uuid4

import agate
import dbt.exceptions
from botocore.exceptions import ClientError
from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.relation import AthenaRelation
from dbt.adapters.base import available
from dbt.adapters.base.column import Column
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.sql import SQLAdapter
from dbt.clients.agate_helper import table_from_rows
from dbt.contracts.graph.manifest import Manifest
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

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        manifest: Manifest,
    ) -> agate.Table:
        """hook macro get_catalog, and at first retrieving info via Glue API Directory"""
        # At first, we need to retrieve all schema name, and filter out from used schema lists
        target_database = information_schema.database
        schemas = self.list_schemas(target_database)
        used_schemas = frozenset(s.lower() for _, s in manifest.get_used_schemas())
        target_schemas = [x for x in schemas if x.lower() in used_schemas]

        try:
            rows = []
            for schema in target_schemas:
                for table in self._retrieve_glue_tables(target_database, schema):
                    rel_type = self._get_rel_type_from_glue_response(table)
                    # Important prefix: "table_", "column_", "stats:"
                    # Table key: database, schema, name
                    # Column key: type, index, name, comment
                    # Stats key: label, value, description, include
                    # Stats has a secondary prefix, user defined one.
                    row = {
                        "table_database": target_database,
                        "table_schema": schema,
                        "table_name": table["Name"],
                        "table_type": rel_type,
                    }
                    descriptor = table["StorageDescriptor"]
                    for idx, col in enumerate(descriptor["Columns"]):
                        row.update(
                            {
                                "column_name": col["Name"],
                                "column_type": col["Type"],
                                "column_index": str(idx),
                                "column_comment": col.get("Comment", ""),
                            }
                        )
                    # Additional info
                    row.update(
                        self._create_stats_dict("description", table.get("Description", ""), "Table description")
                    )
                    row.update(self._create_stats_dict("owner", table.get("Owner", ""), "Table owner"))
                    row.update(
                        self._create_stats_dict("created_at", str(table.get("CreateTime", "")), "Table creation time")
                    )
                    row.update(
                        self._create_stats_dict("updated_at", str(table.get("UpdateTime", "")), "Table update time")
                    )
                    row.update(self._create_stats_dict("created_by", table["CreatedBy"], "Who create it"))
                    row.update(self._create_stats_dict("partitions", table["PartitionKeys"], "Partition keys"))
                    row.update(self._create_stats_dict("location", descriptor.get("Location", ""), "Table path"))
                    row.update(
                        self._create_stats_dict("compressed", descriptor["Compressed"], "Table has compressed or not")
                    )
                    rows.append(row)
                    logger.debug("{}", row)

            if not rows:
                return table_from_rows([])  # Return empty table
            # rows is List[Dict], so iterate over each row as List[columns], List[column_names]
            column_names = list(rows[0].keys())  # dict key order is preserved in language level
            table = table_from_rows(
                [list(x.values()) for x in rows],
                column_names,
                text_only_columns=["table_database", "table_schema", "table_name"],
            )
            logger.debug("{}", table)
            return self._catalog_filter_table(table, manifest)
        except ClientError as e:
            logger.warning(
                "Boto3 Error while retrieving catalog. Fallback into SQL execution: code={}, message={}",
                e.response["Error"]["Code"],
                e.response["Error"].get("Message"),
            )
            return super()._get_one_catalog(information_schema, schemas, manifest)

    def get_columns_in_relation(self, relation: Relation) -> List[Column]:
        cached_relation = self.get_relation(relation.database, relation.schema, relation.identifier)
        columns = []
        if cached_relation and cached_relation.column_information:
            for col_dict in cached_relation.column_information:
                column = Column.create(col_dict["Name"], col_dict["Type"])
                columns.append(column)
            return columns
        else:
            return super().get_columns_in_relation(relation)

    def list_relations_without_caching(self, schema_relation: AthenaRelation) -> List[AthenaRelation]:
        relations = []
        # Default quote policy of SQLAdapter
        quote_policy = {"database": True, "schema": True, "identifier": True}
        try:
            for table in self._retrieve_glue_tables(schema_relation.database, schema_relation.schema):
                rel_type = self._get_rel_type_from_glue_response(table)
                relation = self.Relation.create(
                    database=schema_relation.database,
                    identifier=table["Name"],
                    schema=schema_relation.schema,
                    quote_policy=quote_policy,
                    type=rel_type,
                    column_information=table["StorageDescriptor"]["Columns"],
                )
                relations.append(relation)
            return relations
        except ClientError as e:
            logger.warning(
                "Boto3 Error while retrieving relations. Fallback into SQL execution: code={}, message={}",
                e.response["Error"]["Code"],
                e.response["Error"].get("Message"),
            )
            # Fallback into SQL query
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
        s3_resource = client.session.resource("s3")
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

    def _retrieve_glue_tables(self, catalog_id: str, name: str):
        """Retrive Table informations through Glue API"""
        if not catalog_id:
            raise dbt.exceptions.RuntimeException("Glue GetTables: Need catalog id")
        if not name:
            raise dbt.exceptions.RuntimeException("Glue GetTables: Need database name")
        query_params = {"DatabaseName": name, "MaxResults": 50}
        if catalog_id != "awsdatacatalog":
            query_params["CatalogId"] = catalog_id
        # i have no idea adapter's 'database' (tipically "awsdatacatalog") could be an accountid or not.
        logger.debug("Get relations of schema through Glue API: catalog={}, name={}", catalog_id, name)
        conn = self.connections.get_thread_connection()
        client = conn.handle
        glue_client = client.session.client("glue")
        paginator = glue_client.get_paginator("get_tables")
        page_iterator = paginator.paginate(**query_params)
        for page in page_iterator:
            for table in page["TableList"]:
                yield table

    def _create_stats_dict(self, label, value, description, include=True):
        return {
            f"stats:{label}:label": label,
            f"stats:{label}:value": value,
            f"stats:{label}:description": description,
            f"stats:{label}:include": include,
        }

    def _get_rel_type_from_glue_response(self, table):
        if table["TableType"] == "VIRTUAL_VIEW":
            return RelationType.View
        elif table["TableType"] == "EXTERNAL_TABLE":
            return RelationType.Table
        else:
            raise dbt.exceptions.RuntimeException(f'Unknown table type {table["TableType"]} for {table["Name"]}')

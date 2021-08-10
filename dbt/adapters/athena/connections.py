from typing import ContextManager, Tuple, Optional, List, Dict, Any
from dataclasses import dataclass
from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal
from concurrent.futures.thread import ThreadPoolExecutor

import boto3
import re
from pyathena.connection import Connection as AthenaConnection
from pyathena.result_set import AthenaResultSet
from pyathena.model import AthenaQueryExecution
from pyathena.cursor import Cursor
from pyathena.error import ProgrammingError, OperationalError
from pyathena.formatter import Formatter
# noinspection PyProtectedMember
from pyathena.formatter import _DEFAULT_FORMATTERS, _escape_hive, _escape_presto

from dbt.adapters.base import Credentials
from dbt.contracts.connection import Connection, AdapterResponse
from dbt.adapters.sql import SQLConnectionManager
from dbt.exceptions import RuntimeException, FailedToConnectException
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class AthenaCredentials(Credentials):
    s3_staging_dir: str
    region_name: str
    schema: str
    work_group: Optional[str] = None
    aws_profile_name: Optional[str] = None
    poll_interval: float = 1.0
    _ALIASES = {
        "catalog": "database"
    }
    # TODO Add pyathena.util.RetryConfig ?

    @property
    def type(self) -> str:
        return "athena"

    def _connection_keys(self) -> Tuple[str, ...]:
        return "s3_staging_dir", "work_group", "region_name", "database", "schema", "poll_interval", "aws_profile_name"


class AthenaCursor(Cursor):
    def __init__(self, **kwargs):
        super(AthenaCursor, self).__init__(**kwargs)
        self._executor = ThreadPoolExecutor()

    def _collect_result_set(self, query_id: str) -> AthenaResultSet:
        query_execution = self._poll(query_id)
        return self._result_set_class(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
        )

    def _clean_up_partitions(
        self,
        operation: str
    ):
        # Extract metadata from query
        query_pattern = re.compile("delete\s+from\s+([^\s\.]+)\.([^\s\.]+)\s+where\s+([^;]+);")
        query_search = query_pattern.search(operation)
        if query_search is None:
            raise OperationalError('Unable to extract metadata for cleaning up partitions from ' + operation)
        database_name = query_search.group(1)
        table_name = query_search.group(2)
        where_condition = query_search.group(3)

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


    def _clean_up_table(
        self,
        operation: str
    ):
        # Extract metadata from query
        query_pattern = re.compile("drop\s+table\s+if\s+exists\s+([^\s\.]+)\.([^\s\.]+)")
        query_search = query_pattern.search(operation)
        if query_search is None:
            raise OperationalError('Unable to extract metadata for cleaning up table data from ' + operation)
        database_name = query_search.group(1)
        table_name = query_search.group(2)

        # Look up Glue partitions & clean up
        glue_client = boto3.client('glue')
        table = glue_client.get_table(
            DatabaseName=database_name,
            Name=table_name
        )
        if table is not None:
            logger.debug("Deleting table data from'{}'", table["Table"]["StorageDescriptor"]["Location"])
            p = re.compile('s3://([^/]*)/(.*)')
            m = p.match(table["Table"]["StorageDescriptor"]["Location"])
            if m is not None:
                bucket_name = m.group(1)
                prefix = m.group(2)
                s3_resource = boto3.resource('s3')
                s3_bucket = s3_resource.Bucket(bucket_name)
                s3_bucket.objects.filter(Prefix=prefix).delete()

    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
    ):
        if re.search("delete\s+from", operation):
            self._clean_up_partitions(operation)
            return self

        if re.search("drop\s+table", operation):
            self._clean_up_table(operation)

        query_id = self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
        )
        query_execution = self._executor.submit(self._collect_result_set, query_id).result()
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = self._result_set_class(
                self._connection,
                self._converter,
                query_execution,
                self.arraysize,
                self._retry_config,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self


class AthenaConnectionManager(SQLConnectionManager):
    TYPE = "athena"

    @contextmanager
    def exception_handler(self, sql: str) -> ContextManager:
        try:
            yield
        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            raise RuntimeException(str(e)) from e

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        try:
            creds: AthenaCredentials = connection.credentials

            handle = AthenaConnection(
                s3_staging_dir=creds.s3_staging_dir,
                region_name=creds.region_name,
                schema_name=creds.schema,
                work_group=creds.work_group,
                cursor_class=AthenaCursor,
                formatter=AthenaParameterFormatter(),
                poll_interval=creds.poll_interval,
                profile_name=creds.aws_profile_name
            )

            connection.state = "open"
            connection.handle = handle

        except Exception as e:
            logger.debug("Got an error when attempting to open a Athena "
                         "connection: '{}'"
                         .format(e))
            connection.handle = None
            connection.state = "fail"

            raise FailedToConnectException(str(e))

        return connection

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        if cursor.state == AthenaQueryExecution.STATE_SUCCEEDED:
            code = "OK"
        else:
            code = "ERROR"

        return AdapterResponse(
            _message="{} {}".format(code, cursor.rowcount),
            rows_affected=cursor.rowcount,
            code=code
        )

    def cancel(self, connection: Connection):
        connection.handle.cancel()

    def add_begin_query(self):
        pass

    def add_commit_query(self):
        pass

    def begin(self):
        pass

    def commit(self):
        pass


class AthenaParameterFormatter(Formatter):
    def __init__(self) -> None:
        super(AthenaParameterFormatter, self).__init__(
            mappings=deepcopy(_DEFAULT_FORMATTERS), default=None
        )

    def format(
        self, operation: str, parameters: Optional[List[str]] = None
    ) -> str:
        if not operation or not operation.strip():
            raise ProgrammingError("Query is none or empty.")
        operation = operation.strip()

        if operation.upper().startswith("SELECT") or operation.upper().startswith(
            "WITH"
        ):
            escaper = _escape_presto
        else:
            escaper = _escape_hive

        kwargs: Optional[List[str]] = None
        if parameters is not None:
            kwargs = list()
            if isinstance(parameters, list):
                for v in parameters:

                    # TODO Review this annoying Decimal hack, unsure if issue in dbt, agate or pyathena
                    if isinstance(v, Decimal) and v == int(v):
                        v = int(v)

                    func = self.get(v)
                    if not func:
                        raise TypeError("{0} is not defined formatter.".format(type(v)))
                    kwargs.append(func(self, escaper, v))
            else:
                raise ProgrammingError(
                    "Unsupported parameter "
                    + "(Support for list only): {0}".format(parameters)
                )
        return (operation % tuple(kwargs)).strip() if kwargs is not None else operation.strip()

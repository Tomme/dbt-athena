from typing import ContextManager, Tuple, Optional, List, Dict, Any
from dataclasses import dataclass
from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal
from concurrent.futures.thread import ThreadPoolExecutor

from pyathena.connection import Connection as AthenaConnection
from pyathena.result_set import AthenaResultSet
from pyathena.model import AthenaQueryExecution
from pyathena.cursor import Cursor
from pyathena.error import ProgrammingError, OperationalError
from pyathena.formatter import Formatter
from pyathena.util import RetryConfig

# noinspection PyProtectedMember
from pyathena.formatter import _DEFAULT_FORMATTERS, _escape_hive, _escape_presto

from dbt.adapters.base import Credentials
from dbt.contracts.connection import Connection, AdapterResponse
from dbt.adapters.sql import SQLConnectionManager
from dbt.exceptions import RuntimeException, FailedToConnectException
from dbt.events import AdapterLogger

import tenacity
from tenacity.retry import retry_if_exception
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

logger = AdapterLogger("Athena")


@dataclass
class AthenaCredentials(Credentials):
    s3_staging_dir: str
    region_name: str
    schema: str
    endpoint_url: Optional[str] = None
    work_group: Optional[str] = None
    aws_profile_name: Optional[str] = None
    poll_interval: float = 1.0
    _ALIASES = {"catalog": "database"}
    num_retries: Optional[int] = 5

    @property
    def type(self) -> str:
        return "athena"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self) -> Tuple[str, ...]:
        return "s3_staging_dir", "work_group", "region_name", "database", "schema", "poll_interval", "aws_profile_name", "endpoing_url"


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

    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
    ):
        def inner():
            query_id = self._execute(
                operation,
                parameters=parameters,
                work_group=work_group,
                s3_staging_dir=s3_staging_dir,
                cache_size=cache_size,
                cache_expiration_time=cache_expiration_time,
            )
            query_execution = self._executor.submit(
                self._collect_result_set, query_id
            ).result()
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

        retry = tenacity.Retrying(
            retry=retry_if_exception(lambda _: True),
            stop=stop_after_attempt(self._retry_config.attempt),
            wait=wait_exponential(
                multiplier=self._retry_config.attempt,
                max=self._retry_config.max_delay,
                exp_base=self._retry_config.exponential_base,
            ),
            reraise=True,
        )
        return retry(inner)


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
                endpoint_url=creds.endpoint_url,
                region_name=creds.region_name,
                schema_name=creds.schema,
                work_group=creds.work_group,
                cursor_class=AthenaCursor,
                formatter=AthenaParameterFormatter(),
                poll_interval=creds.poll_interval,
                profile_name=creds.aws_profile_name,
                retry_config=RetryConfig(
                    attempt=creds.num_retries,
                    exceptions=(
                        "ThrottlingException",
                        "TooManyRequestsException",
                        "InternalServerException",
                    ),
                ),
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
            # Fixes ParseException that comes with newer version of PyAthena
            operation = operation.replace("\n\n    ", "\n")

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

from couchbase_operational_insights.errors import (
    InvalidCredentialError,
    OperationalInsightsError,
    QueryError,
    QueryNotFoundError,
    TimeoutError,
)
from insights_performer.protocol.columnar import errors_pb2 as errors_pb


def to_proto_error(e: Exception) -> errors_pb.Error:
    # TODO: Handle the different types of exceptions
    if isinstance(e, OperationalInsightsError):
        sub = None
        if isinstance(e, TimeoutError):
            sub = errors_pb.SubColumnarError(timeout_exception=errors_pb.TimeoutException())
        elif isinstance(e, InvalidCredentialError):
            sub = errors_pb.SubColumnarError(invalid_credential_exception=errors_pb.InvalidCredentialException())
        elif isinstance(e, QueryError):
            sub = errors_pb.SubColumnarError(
                query_exception=errors_pb.QueryException(error_code=e.code, server_message=e.server_message)
            )
        elif isinstance(e, QueryNotFoundError):
            sub = errors_pb.SubColumnarError(query_not_found_exception=errors_pb.QueryNotFoundException())

        if sub is None:
            return errors_pb.Error(columnar=errors_pb.ColumnarError(as_string=str(e)))

        return errors_pb.Error(
            columnar=errors_pb.ColumnarError(
                sub_exception=sub,
                as_string=str(e),
            )
        )
    else:
        t = errors_pb.PLATFORM_ERROR_OTHER
        if isinstance(e, ValueError) or isinstance(e, TypeError):
            t = errors_pb.PLATFORM_ERROR_INVALID_ARGUMENT
        return errors_pb.Error(
            platform=errors_pb.PlatformError(
                as_string=str(e),
                type=t,
            ),
        )

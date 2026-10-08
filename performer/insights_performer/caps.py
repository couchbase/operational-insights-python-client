from couchbase_operational_insights.protocol import PYCBOI_VERSION
from insights_performer.protocol.columnar import caps_pb2 as caps_pb

DEFAULT_MODE = 0


def build_caps():
    caps = caps_pb.FetchPerformerCapsResponse(
        sdk=caps_pb.SDK_PYTHON,
        sdk_version=PYCBOI_VERSION,
        analytics_product=caps_pb.AnalyticsProduct.ANALYTICS,
        supports_server_async_queries=True,
        credential_support=caps_pb.CredentialSupport(
            supports_jwt_credential=True,
            supports_certificate_credential=True,
            supports_set_credential=True,
        ),
    )

    caps.cluster_new_instance[DEFAULT_MODE].CopyFrom(
        caps_pb.PerApiElementClusterNewInstance(supports_dispatch_timeout=False)
    )
    caps.cluster_close[DEFAULT_MODE].CopyFrom(caps_pb.PerApiElementClusterClose())

    caps.cluster_execute_query[DEFAULT_MODE].CopyFrom(
        caps_pb.PerApiElementExecuteQuery(
            execute_query_returns=caps_pb.PerApiElementExecuteQuery.EXECUTE_QUERY_RETURNS_QUERY_RESULT,
            row_iteration=caps_pb.PerApiElementExecuteQuery.ROW_ITERATION_STREAMING_ITERATOR_BASED,
            row_deserialization=caps_pb.PerApiElementExecuteQuery.ROW_DESERIALIZATION_DYNAMIC_ROW_TYPING,
            supports_passthrough_deserializer=True,
            supports_custom_deserializer=True,
        )
    )

    caps.scope_execute_query[DEFAULT_MODE].CopyFrom(
        caps_pb.PerApiElementExecuteQuery(
            execute_query_returns=caps_pb.PerApiElementExecuteQuery.EXECUTE_QUERY_RETURNS_QUERY_RESULT,
            row_iteration=caps_pb.PerApiElementExecuteQuery.ROW_ITERATION_STREAMING_ITERATOR_BASED,
            row_deserialization=caps_pb.PerApiElementExecuteQuery.ROW_DESERIALIZATION_DYNAMIC_ROW_TYPING,
            supports_passthrough_deserializer=True,
            supports_custom_deserializer=True,
        )
    )

    caps.sdk_connection_error[DEFAULT_MODE].CopyFrom(
        caps_pb.SdkConnectionError(
            invalid_cred_error_type=caps_pb.SdkConnectionError.InvalidCredentialErrorType.AS_INVALID_CREDENTIAL_EXCEPTION,
            bootstrap_error_type=caps_pb.SdkConnectionError.BootstrapErrorType.AS_COLUMNAR_ERROR,
        )
    )

    return caps

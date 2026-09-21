from __future__ import annotations

import logging
import os
import tempfile
from typing import Union

from acouchbase_operational_insights.cluster import AsyncCluster
from couchbase_operational_insights.cluster import Cluster
from couchbase_operational_insights.credential import Credential
from couchbase_operational_insights.options import ClusterOptions, SecurityOptions, TimeoutOptions
from insights_performer.exceptions import PerformerException
from insights_performer.options import SharedOptions
from insights_performer.protocol.columnar import cluster_management_pb2 as cluster_management_pb
from insights_performer.protocol.columnar import result_pb2 as result_pb
from insights_performer.result import to_proto_error

# class Mode(Enum):
#     BLOCKING = 0
#     ASYNC = 1

#     @staticmethod
#     def from_ctx(ctx: Union[execution_context_pb.ExecutionContext,
#                             execution_context_pb.ExecutionContextClusterLevel,
#                             execution_context_pb.ExecutionContextScopeLevel]
#                  ) -> Mode:
#         if isinstance(ctx, execution_context_pb.ExecutionContext):
#             return Mode(ctx.mode_index)
#         else:
#             return Mode(ctx.shared.mode_index)


class ClusterManager:
    def __init__(self):
        self._clusters: dict[str, Union[Cluster, AsyncCluster]] = {}
        self._logger: logging.Logger = logging.getLogger()

    @staticmethod
    def _write_temp_pem(contents: str, suffix: str) -> str:
        fd, path = tempfile.mkstemp(prefix='fit_insights_auth_', suffix=suffix)
        with os.fdopen(fd, 'w') as f:
            f.write(contents)
        return path

    @classmethod
    def _convert_credential(cls, proto_cred: cluster_management_pb.ClusterNewInstanceRequest.Credential) -> Credential:
        cred_type = proto_cred.WhichOneof('type')
        if cred_type == 'username_and_password':
            return Credential.from_username_and_password(
                proto_cred.username_and_password.username,
                proto_cred.username_and_password.password,
            )
        elif cred_type == 'jwt_auth':
            return Credential.from_jwt(proto_cred.jwt_auth.jwt)
        elif cred_type == 'certificate_auth':
            cert_path = ClusterManager._write_temp_pem(proto_cred.certificate_auth.cert, suffix='_client_cert.pem')
            key_path = ClusterManager._write_temp_pem(proto_cred.certificate_auth.key, suffix='_client_key.pem')
            return Credential.from_certificate(cert_path, key_path)
        else:
            raise PerformerException(f'Unsupported credential type: {cred_type}')

    @classmethod
    def _convert_security_options(
        cls, proto_opts: cluster_management_pb.ClusterNewInstanceRequest.Options.SecurityOptions
    ) -> SecurityOptions:
        kwargs = {}

        if proto_opts.HasField('trust_only_capella'):
            kwargs['trust_only_capella'] = proto_opts.trust_only_capella
        if proto_opts.HasField('trust_only_pem_string'):
            kwargs['trust_only_pem_str'] = proto_opts.trust_only_pem_string
        # if proto_opts.HasField('trust_only_platform'):
        #     kwargs['trust_only_platform'] = proto_opts.trust_only_platform
        if proto_opts.HasField('disable_server_certificate_verification'):
            kwargs['disable_server_certificate_verification'] = proto_opts.disable_server_certificate_verification
        if len(proto_opts.cipher_suites) > 0:
            raise PerformerException('The Python SDK does not currently support the `cipher_suites` cluster option')

        return SecurityOptions(**kwargs)

    @classmethod
    def _convert_timeout_options(
        cls, proto_opts: cluster_management_pb.ClusterNewInstanceRequest.Options.TimeoutOptions
    ) -> TimeoutOptions:
        kwargs = {}

        if proto_opts.HasField('connect_timeout'):
            kwargs['connect_timeout'] = proto_opts.connect_timeout.ToTimedelta()
        # if proto_opts.HasField('dispatch_timeout'):
        #     kwargs['dispatch_timeout'] = proto_opts.dispatch_timeout.ToTimedelta()
        if proto_opts.HasField('query_timeout'):
            kwargs['query_timeout'] = proto_opts.query_timeout.ToTimedelta()

        return TimeoutOptions(**kwargs)

    @classmethod
    def _convert_options(cls, proto_opts: cluster_management_pb.ClusterNewInstanceRequest.Options) -> ClusterOptions:
        kwargs = {}
        if proto_opts.HasField('deserializer'):
            kwargs['deserializer'] = SharedOptions.convert_deserializer(proto_opts.deserializer)
        if proto_opts.HasField('security'):
            kwargs['security_options'] = cls._convert_security_options(proto_opts.security)
        if proto_opts.HasField('timeout'):
            kwargs['timeout_options'] = cls._convert_timeout_options(proto_opts.timeout)

        return ClusterOptions(**kwargs)

    def new_instance(
        self, req: cluster_management_pb.ClusterNewInstanceRequest
    ) -> result_pb.EmptyResultOrFailureResponse:
        args = [req.connection_string, self._convert_credential(req.credential)]
        if req.HasField('options'):
            args.append(self._convert_options(req.options))

        self._logger.info('Creating new Cluster instance.')

        try:
            self._clusters[req.cluster_connection_id] = Cluster.create_instance(*args)
        except PerformerException as e:
            raise e
        except Exception as e:
            self._logger.warning(f'Could not create cluster instance: {e}')
            return result_pb.EmptyResultOrFailureResponse(error=to_proto_error(e))

        return result_pb.EmptyResultOrFailureResponse(empty_success=True)

    def new_async_instance(
        self, req: cluster_management_pb.ClusterNewInstanceRequest
    ) -> result_pb.EmptyResultOrFailureResponse:
        args = [req.connection_string, self._convert_credential(req.credential)]
        if req.HasField('options'):
            args.append(self._convert_options(req.options))

        self._logger.info('Creating new AsyncCluster instance.')

        try:
            self._clusters[req.cluster_connection_id] = AsyncCluster.create_instance(*args)
        except PerformerException as e:
            raise e
        except Exception as e:
            self._logger.warning(f'Could not create cluster instance: {e}')
            return result_pb.EmptyResultOrFailureResponse(error=to_proto_error(e))

        return result_pb.EmptyResultOrFailureResponse(empty_success=True)

    def close_instance(self, req: cluster_management_pb.ClusterCloseRequest) -> result_pb.EmptyResultOrFailureResponse:
        cluster_id = req.execution_context.cluster_id
        if cluster_id not in self._clusters:
            raise PerformerException(f'Cluster {cluster_id} does not exist')
        cluster = self._clusters.pop(cluster_id)
        cluster.shutdown()
        return result_pb.EmptyResultOrFailureResponse(empty_success=True)

    async def close_instance_async(
        self, req: cluster_management_pb.ClusterCloseRequest
    ) -> result_pb.EmptyResultOrFailureResponse:
        cluster_id = req.execution_context.cluster_id
        if cluster_id not in self._clusters:
            raise PerformerException(f'Cluster {cluster_id} does not exist')
        cluster = self._clusters.pop(cluster_id)
        if isinstance(cluster, AsyncCluster):
            await cluster.shutdown()
        else:
            raise PerformerException(f'Cluster {cluster_id} is not an AsyncCluster instance')
        return result_pb.EmptyResultOrFailureResponse(empty_success=True)

    def close_all(
        self, req: cluster_management_pb.CloseAllColumnarClustersRequest
    ) -> result_pb.EmptyResultOrFailureResponse:
        for c in self._clusters.values():
            c.shutdown()

        self._clusters.clear()
        return result_pb.EmptyResultOrFailureResponse(empty_success=True)

    async def close_all_async(
        self, req: cluster_management_pb.CloseAllColumnarClustersRequest
    ) -> result_pb.EmptyResultOrFailureResponse:
        for c in self._clusters.values():
            if isinstance(c, AsyncCluster):
                await c.shutdown()
            else:
                raise PerformerException('Cluster is not an AsyncCluster instance')

        self._clusters.clear()
        return result_pb.EmptyResultOrFailureResponse(empty_success=True)

    def set_credential(self, req: cluster_management_pb.SetCredentialRequest) -> result_pb.EmptyResultOrFailureResponse:
        cluster_id = req.execution_context.cluster_id
        if cluster_id not in self._clusters:
            raise PerformerException(f'Cluster {cluster_id} does not exist')
        self._clusters[cluster_id].set_credential(self._convert_credential(req.credential))
        return result_pb.EmptyResultOrFailureResponse(empty_success=True)

    async def set_credential_async(
        self, req: cluster_management_pb.SetCredentialRequest
    ) -> result_pb.EmptyResultOrFailureResponse:
        cluster_id = req.execution_context.cluster_id
        if cluster_id not in self._clusters:
            raise PerformerException(f'Cluster {cluster_id} does not exist')
        await self._clusters[cluster_id].set_credential(self._convert_credential(req.credential))
        return result_pb.EmptyResultOrFailureResponse(empty_success=True)

    def get(self, cluster_id: str) -> Union[Cluster, AsyncCluster]:
        return self._clusters.get(cluster_id)
